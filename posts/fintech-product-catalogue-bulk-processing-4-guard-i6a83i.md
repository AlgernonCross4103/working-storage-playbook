# Fintech Product Catalogue Bulk Processing: 4 Guardrails for Thousands of Images

TL;DR: Treat a fintech product-photo import as one cancellable batch, not as thousands of background-removal calls from a loop. Submit once, poll the batch status, and make four controls explicit before admission: a frozen manifest, a byte budget, stable derivative identity, and a retention deadline. This keeps seller-visible progress tied to one durable unit of work and stops storage or cache growth from becoming an accidental property of retry behavior.

The decision rule is blunt: if the platform cannot name the owner, maximum retained bytes, and cleanup deadline for an import, do not admit it. Background removal is computational work, but the lasting operational risk is the source images, intermediate objects, derivatives, and cache entries left behind. A batch is one request and one thing to monitor. It also preserves a cancel path for the inevitable wrong catalogue import.

## How should an API bulk-process thousands of product catalogue images?

A loop hides the unit that matters. Suppose an import contains 8,000 source photos. Eight thousand independent requests produce 8,000 independent retry histories, while the seller thinks they started one job. After a timeout, the caller may not know whether a derivative was written before the response disappeared. Retrying without a stable output identity can create another object, another cache key, or both.

This is a bookkeeping failure before it is a throughput failure. Concurrency limits can protect an upstream service, yet they do not answer which outputs belong to the import, whether the denominator behind progress changed, or when abandoned artifacts may be removed. Faster workers can make the ambiguity grow faster.

One batch changes the control surface. Submit the immutable workload once with `POST /v1/image/batch/submit`, then poll `GET /v1/image/batch/status/{id}`. The verified facts establish those routes, but not payload fields or status-field names, so production clients should obtain the current JSON Schema from the public discovery surface instead of guessing properties such as `percent_complete`. Infrai's discovery API is public without a key and self-describing; its broader platform exposes 295 routes across 20 modules under one key, which is useful when a platform team wants one credential and one bill rather than another media-specific key and invoice.

That consolidation is an operational advantage, not a reason to skip due diligence. The application still owns its storage budget, cache policy, and seller contract.

## Admit imports against bytes, not image counts

Image count is a weak capacity signal because formats and dimensions vary. MDN's image-format guide is a useful reminder that catalogue inputs may arrive as JPEG, PNG, WebP, AVIF, or other formats with very different compression behavior. Measure the actual intake distribution.

For an admission estimate, define `N` as the frozen manifest count, `S95` as the measured 95th-percentile source size, `D` as the number of retained derivatives per source, and `R` as the observed derivative-to-source byte ratio from an acceptance sample:

`reserve_bytes = N * S95 * (1 + D * R) + retry_margin`

This is a conservative planning expression, not a vendor benchmark. `S95`, `R`, and `retry_margin` must come from the fintech catalogue being admitted. If they are unknown, run a bounded sample first. Stop there.

Cache planning needs a separate line item. Storage counts durable bytes; a cache budget depends on distinct derivative identities, cache tiers, and retention. A single stored object can have several cached representations, and changing transformation parameters without versioning the cache key makes invalidation impossible to reason about. The capacity review should therefore record both durable-byte headroom and the maximum number of live derivative keys.

Four guardrails make that review executable:

1. Freeze an immutable manifest with source identities, source versions, and image count. Catalogue edits become a new import rather than silently changing the progress denominator.
2. Reserve bytes before admission using measured inputs. Reject or defer work that lacks a budget; queue depth is not storage capacity.
3. Derive a stable logical output identity from the source version and transformation version. A retry should address the same derivative, while a corrected source should create a deliberately new version.
4. Assign separate retention deadlines to temporary artifacts, final derivatives, and cache entries. Verify deletion with inventory instead of assuming that a configured deadline executed.

These controls are deliberately boring. On call, boring wins.

## Implement one idempotent submission

The following Go program sends a schema-valid batch document supplied through `BATCH_REQUEST_JSON`. It does not invent request fields. It sets an explicit method, reads the API key from the environment, supplies an idempotency key, checks response status, and retries HTTP 429 with exponential backoff while honoring an integer `Retry-After` value.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	payload := os.Getenv("BATCH_REQUEST_JSON")
	idempotencyKey := os.Getenv("BATCH_IDEMPOTENCY_KEY")
	if key == "" || baseURL == "" || payload == "" || idempotencyKey == "" {
		panic("set INFRAI_API_KEY, INFRAI_BASE_URL, BATCH_REQUEST_JSON, and BATCH_IDEMPOTENCY_KEY")
	}

	body, err := submit(context.Background(), baseURL, key, idempotencyKey, []byte(payload))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func submit(ctx context.Context, baseURL, key, idempotencyKey string, payload []byte) ([]byte, error) {
	submitURL := strings.TrimRight(baseURL, "/") + "/image/batch/submit"
	delay := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, submitURL, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("batch submit failed: status=%d body=%s", resp.StatusCode, body)
		}

		wait := delay
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		delay *= 2
	}
	return nil, fmt.Errorf("batch submit remained rate-limited after 5 attempts")
}
```

Generate `BATCH_REQUEST_JSON` from the current discovery schema and validate it before submission. Set `INFRAI_BASE_URL` to the documented v1 API base. Keep the idempotency key stable for retries of the same immutable manifest; a new manifest needs a new key. The response body must be persisted according to its documented schema so the returned batch identifier can anchor polling, operator actions, and the storage ledger. The 1-second initial backoff and 5-attempt ceiling are explicit client policy in this example, not service limits; capacity testing should tune them without removing the ceiling.

Do not make polling aggressive. Use a bounded interval with jitter, persist the last successful observation time, and surface staleness separately from processing failure. The seller should see what the batch status supports, not a smoothly animated percentage invented by the frontend.

## Buy versus build around the asset lifecycle

Background quality matters, but it does not settle the platform decision. The more durable distinction is where source custody, derivative storage, delivery cache, and batch coordination live. Cloudinary, ImageKit, Uploadcare, and Infrai are real managed options, while a self-hosted worker keeps the processing runtime inside the team's boundary.

| Option | Control-plane shape | Storage and cache question | Sensible fit | Verify before commitment |
|---|---|---|---|---|
| Cloudinary | Managed media workflow with transformation and delivery | How are generated assets retained and invalidated within the existing media account? | A team already keeping its product-media lifecycle there | Current background-removal, eager-generation, retention, and invalidation behavior |
| ImageKit | Managed transformation and delivery surface | Which derivative identities populate its delivery cache, and when are they purged? | A team seeking transformation beside a media delivery layer | Current background-removal, cache, and lifecycle contracts |
| Uploadcare | Managed upload and image-processing workflow | Which party owns originals, temporary work, and resulting derivatives? | A team that wants upload handling and image operations together | Current processing, asynchronous, storage, and deletion behavior |
| Infrai | REST batch submission, status polling, and cancellation under a shared credential | The client still defines derivative identity, retention, and cache budgets | A platform team consolidating backend access behind one key and bill | Live discovery schemas plus lifecycle contract tests |
| Self-hosted worker | Team-owned runtime and coordinator | Every stored artifact and cache policy remains internal | A team requiring runtime control and accepting the on-call load | Saturation, recovery, model quality, patching, and capacity evidence |

This is not a feature ranking. A fintech platform already serving every product image through Cloudinary may create more operational cost by splitting removal into a second control plane. ImageKit deserves evaluation where media delivery and transformation should stay together. Uploadcare is a plausible boundary when upload custody is also in scope. Infrai fits when one REST surface, one credential, and one bill reduce platform sprawl, provided the team is comfortable retaining lifecycle ownership.

The limitation is clear: Infrai is a poor fit when the organization needs direct control of the processing runtime or wants the media provider to own source custody, derivative storage, and delivery caching as one product boundary. Choose self-hosting for the former requirement; evaluate Cloudinary, ImageKit, or Uploadcare for the latter. The trade-off is more on-call work with self-hosting, or deeper media-platform coupling with an integrated provider. I would reject any option that cannot demonstrate cancellation and inventory reconciliation against the same 8,000-image acceptance manifest.

The buy-versus-build review should be equally explicit:

| Decision axis | Managed service | Self-hosted processing |
|---|---|---|
| On-call ownership | Batch coordinator, integration, storage, and cache policy remain with the team | Runtime, scaling, patching, coordinator, storage, and cache policy stay with the team |
| Capacity plan | Bound submissions and retained derivatives | Plan compute saturation as well as object and cache growth |
| Lock-in | Request shapes, transformation semantics, and asset identifiers may become provider-shaped | Internal interfaces are controllable, but models and hardware still constrain migration |
| Exit evidence | Exportable manifest, stable source ownership, and reproducible transformations | Reproducible runtime, model artifacts, and replacement capacity |

Pick the boundary whose failure modes the on-call rotation can actually diagnose. A polished demo is not evidence that cancellation, inventory reconciliation, or retention works under a mistaken 8,000-image import.

## Verify progress, cancellation, and rollback

Define two SLOs because one number hides the distinction between control-plane health and outcome completeness. The first measures status freshness for an accepted batch: can the platform obtain and store a recent authoritative observation? The second measures manifest completion: did every admitted source reach a terminal outcome with the expected derivative identity? A stale poller must not masquerade as failed image processing, and a terminal batch must not conceal missing outputs.

Before production admission, run a small acceptance import through this sequence:

1. Submit one frozen manifest twice with the same idempotency key and confirm the workflow represents one logical import.
2. Poll at the configured interval and verify that seller-visible state comes only from documented status data.
3. Request cancellation while work remains, then confirm that no further work is admitted for that import.
4. Inventory sources, temporary artifacts, derivatives, and cache identities against the manifest.
5. Exercise retention and confirm expired objects are absent from inventory while retained outputs are still addressable by their stable identity.

Cancellation is a control action, not proof of cleanup. The rollback procedure should stop further processing, invalidate or remove derivatives created under the bad transformation version, and restore the catalogue's pointer to the last accepted version. Keep the source objects until that verification finishes. Deleting them first converts a reversible import error into data loss.

Finally, report three quantities to operators: manifest items by terminal state, retained bytes by artifact class, and age of the last status observation. Those numbers answer different questions. Compressing them into a single green badge gives the seller less information and gives on-call a harder night.

## References

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
- [Uploadcare image transformations](https://uploadcare.com/docs/transformations/image/)
- [Google SRE Workbook: Implementing SLOs](https://sre.google/workbook/implementing-slos/)
