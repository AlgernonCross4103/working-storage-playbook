# E-commerce Image Ingest Recovery: Browser Uploads, Object Storage, and Thumbnail Workers

Short answer: upload the original image directly from the browser to private object storage, record an upload intent before issuing the upload, and let an asynchronous worker create thumbnails after a durable notification or queue message. Treat every notification as repeatable, and make the database record the authority for completion.

This is the useful boundary for an e-commerce catalog: the web application handles authorization and metadata, while the browser transfers the large body and a worker handles decoding and resizing. The upload request does not wait for derivatives. A worker can be retried, drained, or replayed without making the customer submit the original again.

The important qualification is that direct upload is not automatically the simplest delivery path. Access control, CORS, abandoned uploads, and the gap between an object arriving and a database knowing about it decide whether the design is sound. Delivery convenience is a poor substitute for a defined failure boundary.

## Start with an upload intent, not an object event

Create an image record with a server-generated identifier before returning an upload capability to the browser. The record should bind the authenticated seller, expected content type, maximum accepted size, original object key, and transformation revision. The browser then uploads to a private key such as `originals/<image-id>` and reports completion to the application.

That completion call is a state transition, not proof that processing succeeded. The storage event remains useful because it can recover work when the browser closes after the transfer, but the application record gives the worker enough context to reject an object that arrived for the wrong seller or key. A reconciliation job can periodically compare pending intents with the storage listing; listing is a recovery input, not a replacement for authorization.

For progress reporting, `XMLHttpRequest` exposes upload progress events. A signed upload capability keeps long-lived storage credentials out of browser code, and the browser should send only the headers required by that capability. If the storage policy cannot support the needed browser CORS behavior, proxying bytes through the application is a valid fallback, although it moves bandwidth, connection count, and upload latency back into the web tier. Validate that choice with a capacity model before launch.

The original stays private. Thumbnail reads should use an application authorization check followed by a short-lived signed read, or an image delivery layer that has an equivalent policy. Public object keys are a different product decision: catalog images, seller drafts, and moderation inputs do not have the same audience.

## How should a Node.js queue coordinate browser uploads, object storage, and thumbnail workers?

The queue should carry a compact job reference, not the image bytes. A message can identify the image record, source key, transformation revision, and requested sizes. The consumer loads the record, verifies that the source is accepted, claims the deterministic job, renders each derivative, and commits the resulting keys.

The following Go example shows the coordination contract without pretending that a particular queue or storage SDK has been selected. A Node.js consumer can apply the same sequence with its queue client and database transaction; the language is an implementation detail, while claim, lease, and completion are the reliability contract.

```go
package thumbnails

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Job struct {
	ImageID  string
	Source   string
	Revision string
	Sizes    []string
}

type JobStore interface {
	Claim(ctx context.Context, key string, lease time.Duration) (bool, error)
	Complete(ctx context.Context, key string, outputs []string) error
	Fail(ctx context.Context, key string, cause error) error
}

type Renderer interface {
	Render(ctx context.Context, source, destination, size string) error
}

func idempotencyKey(j Job) string {
	sizes := append([]string(nil), j.Sizes...)
	sort.Strings(sizes)
	data := strings.Join([]string{
		j.ImageID, j.Source, j.Revision, strings.Join(sizes, ","),
	}, "\x00")
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func Process(ctx context.Context, store JobStore, renderer Renderer, j Job) error {
	key := idempotencyKey(j)
	claimed, err := store.Claim(ctx, key, 10*time.Minute)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	outputs := make([]string, 0, len(j.Sizes))
	for _, size := range j.Sizes {
		destination := fmt.Sprintf("thumbnails/%s/%s/%s", j.ImageID, j.Revision, size)
		if err := renderer.Render(ctx, j.Source, destination, size); err != nil {
			_ = store.Fail(ctx, key, err)
			return err
		}
		outputs = append(outputs, destination)
	}
	return store.Complete(ctx, key, outputs)
}
```

The first transaction must make the uniqueness decision. Two deliveries for the same image, revision, and size set should result in one claimed job; a completed or currently leased job is an ordinary duplicate, not an incident. If a process dies after writing one thumbnail, the lease can expire and a later attempt can write the same deterministic destination before recording the complete output set. The state transition that marks completion must reject a stale lease owner.

Keep queue acknowledgement after the durable state change. Acknowledging first creates a commit gap: the consumer can disappear after the message is removed but before the thumbnail keys are recorded. Acknowledging last means a crash can redeliver a message, which is exactly why the job must be idempotent. The queue is a delivery mechanism; it is not the catalog database.

## The failure modes that deserve a test

The first test is duplicate delivery. Publish the same job twice and assert that the database has one logical job and one output set. The second is a worker crash after the first derivative is written. Let the lease expire, run the job again, and verify that the final state contains every expected key for one revision. The third is a late completion from the old worker; it must not mark a newer revision complete.

Test authorization separately from processing. An object under a valid-looking key but owned by another seller must not enter the resize path. A source whose metadata says “uploading” must not be treated as ready merely because a listing can see a key. Test an unsupported or malformed image as a terminal data error with a reviewable record, rather than allowing a hot retry loop.

The most expensive test is the commit gap. Suppose the worker renders `small` successfully, writes it, and then loses its database connection before recording that output. If the queue message was acknowledged before the write, the job has vanished from the delivery system while the database still says it is pending; an operator sees no obvious queue backlog and the catalog waits forever. If the message is acknowledged after completion, the same crash produces a redelivery. That is acceptable: the next worker claims the same idempotency key, checks the existing deterministic destination, renders or verifies the missing set, and records all expected keys in one completion transition. Now add a deployment: the old worker wakes up after its lease has expired and tries to finish. The completion update must include the lease owner or fencing token, so the stale process cannot overwrite the state created by the replacement. This is why a queue setting alone cannot solve the problem. The queue controls delivery, the object store holds bytes, and the database controls ownership of the workflow.

It fails quietly otherwise.

Measure the whole user-visible path: accepted upload to complete derivative set. Queue age alone misses time spent waiting for reconciliation, and time from notification to first thumbnail hides a missing size. Set the SLO after measuring arrival rate, source-size distribution, variant count, decode memory, and downstream write throughput; I’m not sure a useful percentile can be chosen without those values, and your mileage may vary between a small seller catalog and a promotion-day burst.

Three words: measure the backlog.

At minimum, expose upload-intent age, reconciliation lag, queue age, claim conflicts, attempts per job, terminal data errors, derivative count, and the age of the oldest incomplete image. Alert on an SLO budget burn rather than on every retry. A transient transport failure and a permanently invalid image need different operators and different retry policies.

## Verify a release, then roll it back cleanly

Before a renderer release receives real catalog traffic, run a fixed fixture set through the complete path. Check dimensions, content type, private-read authorization, output naming, and the database's expected-key set. Repeat the notification, interrupt processing between two output writes, and restart the worker with the same revision. The important assertion is convergence: repeated delivery ends at the same recorded result.

Write a new transformation revision to a new prefix. Do not replace the previously accepted thumbnail in place. Promotion can then be a database pointer update after verification, while rollback is a pointer change back to the prior complete revision. Originals are immutable from the pipeline's point of view, and cleanup should consult database state before deleting old prefixes.

The catch is that this design is not suitable when the application cannot provide a durable job store, when strict provider-native object controls are a hard requirement, or when direct browser CORS policy cannot be made consistent with the signed upload. Stick with a backend-proxied upload or a storage boundary with the required controls when those conditions apply. The extra hop is easier to operate than a security policy nobody can verify.

Use this small decision table during the design review:

| Constraint | Prefer direct browser upload | Prefer an application proxy |
| --- | --- | --- |
| Browser can use the signed request and required CORS policy | Yes | Not required |
| Application must inspect every byte before storage | No | Yes |
| Web tier has measured bandwidth and connection headroom | Helpful | Required |
| Original must stay private after upload | Enforce signed, private access | Enforce the same policy |
| Recovery must survive a missing browser completion call | Add storage reconciliation | Add storage reconciliation |

Capacity planning belongs in the runbook. Peak upload rate multiplied by derivative count gives a starting write rate, then add retry headroom and the memory cost of concurrent decodes. Limit concurrency from measured worker memory and downstream throughput, set a lease longer than the normal processing window, and provide a drain mode for deployments. An SLO without a queue-drain and rollback procedure is a dashboard, not a service contract.

## References

Further reading:

- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- MDN, Using XMLHttpRequest, including upload progress events: https://developer.mozilla.org/en-US/docs/Web/API/XMLHttpRequest_API/Using_XMLHttpRequest
