# Node.js Thumbnail Resize with Private Bucket Retention and Presigned Downloads

The deletion deadline changes the design more than the resize operation does. **Short answer: keep signed documents and their Sharp-generated thumbnails private, give every document generation an explicit `delete_at` value, publish derivatives only after verification, and issue short-lived presigned download URLs from an authorization-controlled service.** Treat the deadline as application data and as a storage-lifecycle input; relying on a filename, a cache timeout, or an engineer remembering to delete a folder is not a retention policy.

This is a runbook for a media platform that retains signed documents for a defined period, creates image previews for internal review, and must be able to explain what happened when a deadline passes. The hard part is the evidence trail.

The clock is the contract.

## How should a Node.js image thumbnail resize flow handle private bucket deletion and presigned download URLs?

Separate four transitions: intake, transformation, publication, and deletion. The upload request creates a document record with an immutable source key, a generation identifier, and `delete_at`. A worker reads the source, uses Sharp to normalize orientation and create the agreed thumbnail sizes, writes those derivatives under keys containing the same generation, and marks the generation ready only after each expected object has been checked. A download handler authorizes the caller, then creates a short-lived signed URL for the exact ready key.

The reader-facing record should point to a generation, not to a mutable path. That detail prevents a half-finished resize from becoming visible. If the worker has written a small thumbnail but not the large one, the database still points to the previous complete generation, or reports the document as pending when there is no previous generation. The object store is holding bytes; the database owns workflow state, ownership, dimensions, and deletion intent.

The deadline must apply to every retained object. For a document with one source and three derivatives, store four object keys and four cleanup results under one generation. A successful delete means the object is gone or the storage system's documented deletion state has been observed; it does not mean a queue message was emitted. The cleanup job should be idempotent, record an attempt, and leave a reviewable state when authorization, throttling, or a transient network failure interrupts it. This is where teams usually discover that “delete the document” was never defined precisely: does it include a failed thumbnail, a temporary upload, a replicated copy, a CDN cache, a database attachment, and the audit record that proves the deletion request existed? Write those answers into the retention contract before the worker is deployed. If a copy is intentionally retained for legal or operational reasons, record its owner and expiry separately rather than quietly extending the document's deadline.

Do not make the signed URL the retention mechanism. Its expiry limits one access path, while a copied object key, an old URL, or an internal worker can still reach data unless the storage and application policies agree. The URL issuer should check the document's live generation and `delete_at` before signing. After the deadline, it should refuse new URLs even if cleanup is waiting in the queue.

There is one uncomfortable capacity fact: immutable generations consume space during the rollback window. For `R` new documents per second, `B` average source bytes, `N` thumbnail variants, and `D` seconds of retention, a first estimate for live bytes is `R × D × B × (1 + N × thumbnail_ratio)`. Add space for retries and unreferenced generations separately. I would not put a numeric SLO on this equation without representative files; very large scans and unusual formats can dominate both Sharp CPU and storage. Your mileage may vary, and a replay of the production size distribution is what resolves that uncertainty.

## Model retention before writing the upload worker

Use a small state machine rather than a boolean called `processed`. A useful record has `document_id`, `generation_id`, `source_key`, `derivative_keys`, `delete_at`, `state`, and timestamps for intake, ready, and deletion. States such as `pending`, `ready`, `delete_due`, `deleted`, and `delete_review` make dashboards and recovery actions answerable. They also make it possible to distinguish “the source never arrived” from “the source arrived and is overdue for deletion.”

The worker should derive keys from an opaque document and generation identifier, not from a user filename. A layout such as `source/{generation}` and `thumb/{size}/{generation}` is easy to audit by prefix, while the database remains the authoritative index. Never reuse a key for a new logical document. A retry then converges on the same generation instead of creating an unexplained second object.

The retention clock needs an owner. Decide whether `delete_at` starts when the signed document is accepted, when it becomes ready, or at a legally defined event, and persist that decision with the record. Clock handling also belongs in the design: workers should compare times in UTC, tolerate a small scheduling delay, and expose the age of the oldest overdue item. A lifecycle rule can provide a backstop for expiration, but the application still needs an audit record and an authorization check.

| Decision | Managed object storage | Self-hosted object storage |
|---|---|---|
| Deletion enforcement | Use documented lifecycle and delete behavior, then verify the application record against observed results | Own the scheduler, replication behavior, disk failures, and evidence that deletion reached every copy |
| On-call load | Fewer storage operations to operate, but provider limits and policy details remain part of the SLO | More control over placement and policy, with more failure modes on the platform team |
| Portability | S3-compatible APIs can reduce client changes, but compatibility is not proof of identical retention semantics | Maximum control over implementation, with migration and capacity work carried by the team |
| Best fit | A team that values managed durability and can accept documented provider boundaries | A team with a clear reason to own the storage control plane and its recovery drills |

The catch is that a generic compatibility layer is not automatically a compliance control. It may hide provider-specific lifecycle, legal-hold, replication, or deletion semantics that the retention review needs to see. Choose a direct provider integration or self-hosted system when immutable retention, region-specific residency, or independently verifiable deletion is a hard requirement and the abstraction cannot expose that evidence. Stick with the managed path when the team cannot staff storage operations; the extra control is not free.

## A safe runbook for resize, upload, and publication

The request path should accept the source and write a pending record, then hand transformation to a bounded worker pool. The worker validates the input before allocating work, produces thumbnails in memory or bounded temporary storage, uploads each object with an explicit content type, and checks the known keys before the state transition to `ready`. The exact SDK can vary; the invariants cannot.

This small Go example shows the part that is easy to miss: publication is a single state transition after all expected artifacts exist. The storage and database clients are interfaces so the same test can exercise a managed service, a compatible endpoint, or a local test double without smuggling provider assumptions into the workflow.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type ObjectStore interface {
	Put(ctx context.Context, key string, contentType string, body []byte) error
	Head(ctx context.Context, key string) error
	Delete(ctx context.Context, key string) error
}

type Repository interface {
	MarkReady(ctx context.Context, documentID, generationID string) error
	MarkDeleteReview(ctx context.Context, documentID, generationID string, reason string) error
}

type Artifact struct {
	Key         string
	ContentType string
	Body        []byte
}

func publish(ctx context.Context, store ObjectStore, repo Repository, documentID, generationID string, artifacts []Artifact) error {
	for _, artifact := range artifacts {
		if err := store.Put(ctx, artifact.Key, artifact.ContentType, artifact.Body); err != nil {
			return fmt.Errorf("put %s: %w", artifact.Key, err)
		}
	}

	for _, artifact := range artifacts {
		if err := store.Head(ctx, artifact.Key); err != nil {
			return fmt.Errorf("verify %s: %w", artifact.Key, err)
		}
	}

	if err := repo.MarkReady(ctx, documentID, generationID); err != nil {
		return fmt.Errorf("publish generation: %w", err)
	}
	return nil
}

func deleteGeneration(ctx context.Context, store ObjectStore, repo Repository, documentID, generationID string, keys []string, deleteAt time.Time, now time.Time) error {
	if now.Before(deleteAt) {
		return errors.New("deletion deadline has not arrived")
	}
	for _, key := range keys {
		if err := store.Delete(ctx, key); err != nil {
			_ = repo.MarkDeleteReview(ctx, documentID, generationID, err.Error())
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}
```

In production, the resize worker would be Node.js with Sharp because that is the requested image-processing stack; the example deliberately keeps storage and persistence behind contracts. Test the real Sharp path with representative signed-document scans, including rotation metadata, oversized dimensions, corrupt input, and files whose decoded pixel count is much larger than their compressed size. Rejecting unsafe work before the queue grows is a capacity control, not just input hygiene.

Retries need an identity. A retry after a timeout must not publish a new generation, and a cleanup retry must not make a successfully deleted item look like a new failure. Back off on throttling, cap attempts, preserve the document and generation identifiers, and send exhausted work to a review queue. Do not retry an authorization denial as if it were a capacity event.

## Verify expiry, rollback, and the pager path

Before launch, run the workflow as a failure drill. Stop a worker after the source upload, after one derivative upload, and immediately before the ready transition. In each case, the old ready generation must remain downloadable, the partial generation must be discoverable from database state, and a replay must converge without changing the logical document. Then advance a test clock beyond `delete_at`: the URL issuer should deny access, the cleanup job should process every key, and the dashboard should show the result rather than only a queue acknowledgment. I would also unplug one cleanup consumer during a batch, restore it after the deadline, and compare the resulting object inventory with the database's expected key set. That exercise catches a deceptively common gap: the job reports success for the source, but a derivative was created by a later retry and was never added to the cleanup manifest. The recovery test should prove that the manifest is immutable for a generation, that repeated deletes are harmless, and that an operator can identify the exact item needing review without reading raw worker logs.

Measure queue age, transform duration, upload latency, verification latency, ready-publication latency, and overdue-delete age separately. One combined “thumbnail success” metric hides the distinction between a saturated CPU pool and a storage policy that is preventing deletion. Set alert thresholds from the product's retention and freshness requirements, then load-test the tail. Average latency is not a capacity plan.

Rollback is a database pointer change while the previous generation still exists. Keep it until the operational rollback window closes, and schedule deletion only after that window is compatible with `delete_at`. If the legal deadline is earlier than the rollback window, the legal rule wins: retain audit metadata without retaining the document bytes. A lifecycle expiration rule is useful as a second line of defense; the primary workflow still records intent, authorization, attempts, and observed completion.

The decision rule is plain: use a private, generation-based layout when the document is sensitive; use application-controlled signing when every download needs authorization; and make deletion a measured, idempotent workflow rather than a side effect of resizing. The design is not suitable when the storage system cannot provide the retention evidence or deletion semantics your policy requires. In that case, change the storage boundary before adding more worker code.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://www.backblaze.com/cloud-storage/pricing
