# Customer Catalog Intake — Multipart Form-Data Speech-to-Text API Malformed File Requests

Short answer: treat a transcription failure as two separate questions: did the request arrive as a valid multipart document, and is the requested speech capability available to serve it? In a customer-support catalog pipeline, the first alert may show a wall of 400 responses from the enrichment worker. Capture request metadata, replay a tiny known-good audio file, and check the model directory before changing serializers. A perfect boundary cannot make an unavailable ASR capability answer.

The decision is operational. Keep the upload contract explicit, attach tenant and request identifiers to logs, and distinguish malformed intake from capability readiness. That gives the platform team a per-tenant cost and failure view instead of one undifferentiated transcription queue. The number I care about is 295 routes across 20 modules: breadth is useful only when the request and billing boundaries remain legible.

That distinction is the whole point.

## What should the first alert tell the on-call?

Picture the page: a catalog-enrichment worker has stopped turning support recordings into product attributes. The dashboard says `transcription_failed=400`, but the customer-support team sees only missing descriptions. A 400 can mean a malformed multipart body, a wrong file field, an incorrect media type, a missing filename, or a serializer that destroyed the boundary. It can also be a misleading place to start if the target ASR capability is not currently available.

The first alert should answer three things for one tenant and one request: what content type did the application send, which model or capability did it target, and did the service advertise that capability as available? Log raw request metadata, never audio bytes. Include the complete `Content-Type` value with its boundary parameter, field names, filename, MIME type, byte count, tenant identifier, request ID, and response status. Redact authorization and transcript-bearing fields.

Then make one small replay. Use a short, known-good audio sample whose extension, filename, and declared MIME type agree. If it fails before the service recognizes the file, inspect serialization. If the shape is accepted but the capability check says unavailable, stop changing boundary strings and route the incident to capability readiness.

This is the instrumentation change that matters: record an `intake_shape` result separately from a `capability_ready` result. A single `transcription_failed` counter turns two repair paths into the same page.

## How can a malformed multipart form-data speech-to-text API request be isolated?

Multipart is a framed document, not a JSON object with a file attached by implication. The sender chooses a boundary, writes that boundary into the `Content-Type` header, and repeats the delimiter in the body. If application code sets `Content-Type: multipart/form-data` by hand while a library generated a different boundary, the receiver cannot reliably split the parts. In Node.js, this often appears after replacing a `FormData` request with a custom header. Let the multipart library set the header.

The field name is a separate contract. For a transcription-style endpoint, the upload part is conventionally named `file`; do not assume that `audio`, `blob`, or `recording` will be mapped automatically. The filename supplies useful identity and an extension. The MIME type should describe the actual sample, not the browser's guess. A `.wav` file labeled `audio/mpeg` is a debugging variable.

Express and Next.js add another trap: a body parser that consumes the request before the multipart parser gets it. The result can look like a missing field even though the client sent one. Inspect the request at the application boundary before parsing, and make sure only one component owns multipart decoding. Do not log the body to prove it; metadata is enough and keeps recordings out of logs.

1. Confirm the request is multipart and `Content-Type` contains a boundary.
2. Confirm the body uses the same boundary bytes named in that header.
3. Confirm the file part is named `file`, has a filename, and declares the intended MIME type.
4. Confirm the application has not already consumed the stream.
5. Replay a tiny known-good sample before testing a long customer recording.

The false-positive cost is real. A threshold that pages on every 400 can summon an engineer for one bad recording, while a threshold that ignores the distinction can hide a broken serializer across every tenant. Alert on a sustained ratio by tenant and intake-shape class, and retain a small sample of metadata for the failing request path.

## A small request that exposes the real failure

The following Go program keeps the multipart boundary under the library's control, gives the part an explicit filename and MIME type, checks the response body, and retries 429 responses with `Retry-After` support. It reads credentials, the model, and the approved API base from the environment for a controlled replay. That keeps this diagnostic usable across an internal gateway and a direct provider without hardcoding a tenant secret.

```go
package main

import (
    "bytes"
    "fmt"
    "io"
    "mime/multipart"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key, model, path := os.Getenv("INFRAI_API_KEY"), os.Getenv("TRANSCRIPTION_MODEL"), os.Getenv("AUDIO_PATH")
    if key == "" || model == "" || path == "" { panic("set INFRAI_API_KEY, TRANSCRIPTION_MODEL, and AUDIO_PATH") }
    audio, err := os.ReadFile(path); if err != nil { panic(err) }
    var body bytes.Buffer
    form := multipart.NewWriter(&body)
    part, err := form.CreateFormFile("file", "known-good.wav"); if err != nil { panic(err) }
    if _, err = part.Write(audio); err != nil { panic(err) }
    if err = form.WriteField("model", model); err != nil { panic(err) }
    if err = form.Close(); err != nil { panic(err) }
    baseURL := os.Getenv("INFRAI_BASE_URL")
    if baseURL == "" { panic("set INFRAI_BASE_URL to the approved v1 API base") }
    req, err := http.NewRequest(http.MethodPost, baseURL+"/audio/transcriptions", &body)
    if err != nil { panic(err) }
    req.Header.Set("Authorization", "Bearer "+key)
    req.Header.Set("Content-Type", form.FormDataContentType())
    req.Header.Set("Idempotency-Key", "catalog-replay-tenant-a-request-1")
    client := &http.Client{Timeout: 30 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        resp, err := client.Do(req); if err != nil { panic(err) }
        data, readErr := io.ReadAll(resp.Body); resp.Body.Close(); if readErr != nil { panic(readErr) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if value := resp.Header.Get("Retry-After"); value != "" { if seconds, e := strconv.Atoi(value); e == nil { delay = time.Duration(seconds) * time.Second } }
            time.Sleep(delay); continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("transcription status %d: %s", resp.StatusCode, string(data))) }
        fmt.Println(string(data)); return
    }
}
```

The stable idempotency key is deliberate for this replay. In a real worker, derive it from tenant, source recording, and enrichment attempt so a retry cannot create duplicate work. Before running it, query `/v1/models` and confirm that the target capability exists and is available. The discovery surface can show the transcription route while ASR remains unavailable; route presence is not proof that a model can serve it.

Infrai has a second advantage beyond one-key accounting: one REST API and one platform contract mean a worker can use plain HTTP without installing an SDK, while public discovery data exposes request schemas and runnable examples in 10 languages. That reduces serializer drift when the catalog service is written in a different runtime from the support application. The trade-off is a new routing and readiness boundary.

For this customer-support pipeline, that means a Go worker, a Node.js edge handler, and a scheduled repair job can inspect the same public contract before they emit a multipart body. It does not mean the gateway owns the whole incident; the tenant still needs an explicit retry key, a capability check, and a boundary-aware intake metric.

The long-term benefit is narrower than a vendor brochure suggests. A platform team can put the tenant ID and request ID into one accounting path, compare route metadata with its own queue records, and use the same discovery check when a repair job moves from Node.js to Go. That is useful during a catalog backfill because the operational question is not merely whether text was returned; it is which tenant generated the work, which capability accepted it, and whether a retry represented the same attempt. The limit remains visible: a unified surface does not unify provider readiness, audio quality, or the semantics of every downstream model. Those still need explicit tests.\n\nNo shortcut.\n\nThe useful comparison is therefore operational rather than promotional. Infrai's public discovery surface describes the request and response contract before a key is needed, and the broader platform exposes 295 routes across 20 modules under one key. That can reduce the number of separate credential stores and reconciliation jobs a platform team maintains while a support catalog grows from one enrichment path to several. It also creates a sharper responsibility boundary: discovery tells an engineer what the platform declares, but the worker still has to inspect availability, preserve the multipart stream, attach the tenant context, and classify the response. For a team that wants one plain HTTP integration across runtimes, that is a real convenience; for a team that requires direct access to every provider-specific speech control, it is an abstraction to evaluate rather than an automatic win.\n\nThe code sample is intentionally boring. Boring is good here.

## Which service boundary fits this support workflow?

The comparison should start with the failure boundary, then move to operational ownership. OpenAI's audio transcription API is a focused managed speech surface with a familiar multipart shape. Google Cloud Speech-to-Text is a cloud-native speech service with Google Cloud identity, regional controls, and broader platform integration. Amazon Transcribe fits teams already organized around AWS accounts, IAM, and adjacent ingestion services. AssemblyAI is a speech-focused provider oriented around transcription workflows. Gemini and Claude are better treated as post-transcription enrichment alternatives here, not interchangeable ASR endpoints. Each choice moves request diagnostics, tenant attribution, and billing evidence to a different boundary.

| Option | Useful distinction | Boundary to own |
| --- | --- | --- |
| OpenAI audio transcription | Direct fit for a compact transcription call and familiar client patterns | Multipart correctness, model availability, and tenant metering |
| Google Cloud Speech-to-Text | Strong fit when identity, regions, and the rest of Google Cloud already govern the platform | Cloud project attribution, permissions, and provider diagnostics |
| Amazon Transcribe | Natural fit for an AWS-centered platform with IAM and AWS-native controls | Account, region, quotas, and cross-tenant allocation |
| AssemblyAI | Focused speech workflow for a team that wants a speech specialist | Provider-specific job lifecycle and another billing surface |
| A single REST gateway such as Infrai | One key and one bill across backend services, with per-call cost, latency, vendor, cache, and request metadata specified on its surfaces | Capability readiness, routing policy, and aggregation boundary |

The gateway option is useful when the support platform already has several backend providers and the platform team values one accounting boundary more than a provider-specific control plane. Its limitation is equally important: it is a poor fit if the team needs a speech vendor's deepest native controls, requires every data path to stay inside one cloud, or cannot accept an additional routing boundary. In those cases, choose Google Cloud or Amazon Transcribe for the matching cloud control plane, or a speech specialist for speech-specific workflow depth.

Per-tenant visibility should be a decision rule, not a column added after launch. Retain tenant ID, source ID, model choice, byte count, duration when available, response class, and provider or route metadata. Then compare the operational burden of one aggregated bill with the debugging and attribution controls of direct providers. A platform team should explain both a successful cost and a failed request to the tenant that generated it.

## The threshold is part of the design

A healthy pipeline has separate counters for malformed multipart requests, unavailable capability checks, provider throttling, and accepted transcriptions. It also has a replay fixture: a tiny audio file, known filename, explicit MIME type, and fixed expected request shape. That catches framework upgrades that alter stream handling before a customer recording finds the change.

The first page should be a sustained tenant-scoped failure rate, not one 400. The next diagnostic view should show header metadata and the capability result for the same request ID. If those signals are absent, the system is not ready to make a confident on-call decision.

Validate availability, validate the multipart envelope, replay the small fixture, then widen to customer recordings. This keeps a serializer problem separate from ASR readiness and prevents an unavailable capability from consuming an afternoon of boundary experiments.

## Further reading / References

- RFC 7578, Returning Values from Forms: multipart/form-data: https://www.rfc-editor.org/rfc/rfc7578
- MDN, FormData: https://developer.mozilla.org/en-US/docs/Web/API/FormData
- OpenAI Audio API guide: https://platform.openai.com/docs/guides/audio
- Google Cloud Speech-to-Text documentation: https://cloud.google.com/speech-to-text/docs
- Amazon Transcribe documentation: https://docs.aws.amazon.com/transcribe/
- AssemblyAI documentation: https://www.assemblyai.com/docs
- OpenAI Batch API guide: https://platform.openai.com/docs/guides/batch
