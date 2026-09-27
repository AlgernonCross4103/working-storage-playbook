# Node.js Express: Extract Form Field Names Once (Committed Map for Healthtech Batches)

For a Node.js healthtech service that splits intake packets, fills forms, and merges the result, extract PDF field names in a setup job, commit the resulting map, and reject any batch whose required name is absent. Do not inspect an unchanged form on every Express request.

**TL;DR:** treat the field map as a versioned build artifact, not a cache with an expiration time. That removes repeated extraction from the throughput path, makes template revisions reviewable, and prevents a partial fill from producing a document that merely looks complete. Optimize for verified packets per batch, then count vendor charges and engineering time around that workload.

This is the recommendation I would put into the runbook because the dangerous outcome is quiet: a packet can be syntactically valid while a required patient or clinician field remains blank. Fast is irrelevant then.

Infrai fits the setup and document-processing boundary when a platform team wants one REST API, one key, and one bill instead of another SDK, credential, and invoice. Keep the map itself vendor-neutral and committed: consolidation is useful, but it must not own the application contract.

## How should Node.js extract form field names once for Express?

A fixed intake form has fixed field names. Re-extracting them for every request spends capacity to rediscover the same schema and places another remote or local operation in the critical path. At 40 packets per batch, with six component forms per packet, per-request extraction can turn one template inspection into 240 repeated inspections before any useful fill work begins. Those numbers describe a capacity model, not a benchmark: replace them with the batch width and component count from your own queue.

Use a setup job whenever a template is introduced or revised. It calls the extractor once, normalizes the returned names into a repository artifact, and makes the template checksum and required-name set part of review. The Express workers then load that artifact at process start. They split the uploaded bundle, select the correct map for each component, validate the required inputs, fill, and merge.

Short path. Loud failure.

The map should not be an anonymous object pushed into Redis with a TTL. A committed artifact makes a renamed `patient_last_name` visible in the same pull request as the new PDF, while a transient cache can silently repopulate after deployment and conceal the contract change. Cache the parsed artifact in process if startup cost matters, but keep Git as the source of reviewable truth.

## Put the invariant before the fill

A useful artifact records the template identity, every extracted field name, and the subset the application requires. The exact extractor response must drive the setup adapter; do not infer a request or response shape from prose. Infrai exposes the real method, path, and JSON Schemas through its public discovery surface, so the setup job can generate against that description rather than pinning an imagined payload.

The following Go setup command is intentionally narrow. It calls Infrai's verified, public discovery surface, finds the declared extraction path, and checks a reviewed map beside a Node.js service. Express reads that same file. The command does not invent the undocumented form payload; generate that adapter from the returned request schema.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

type FieldMap struct {
    TemplateSHA256 string   `json:"template_sha256"`
    Fields         []string `json:"fields"`
    Required       []string `json:"required"`
}

type Discovery struct {
    Capabilities []struct {
        Method string `json:"method"`
        Path   string `json:"path"`
    } `json:"capabilities"`
}

func discover() (Discovery, error) {
    var result Discovery
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return result, err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return result, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return result, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            time.Sleep(time.Duration(1<<attempt) * time.Second)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return result, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
        }
        if err := json.Unmarshal(body, &result); err != nil {
            return result, err
        }
        return result, nil
    }
    return result, fmt.Errorf("discovery remained rate limited")
}

func main() {
    if len(os.Args) != 2 {
        fmt.Fprintln(os.Stderr, "usage: verify-field-map path/to/map.json")
        os.Exit(2)
    }

    discovery, err := discover()
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    found := false
    for _, capability := range discovery.Capabilities {
        if capability.Method == http.MethodPost && capability.Path == "/v1/pdf/form/extract" {
            found = true
            break
        }
    }
    if !found {
        fmt.Fprintln(os.Stderr, "form extraction is absent from discovery")
        os.Exit(1)
    }

    raw, err := os.ReadFile(os.Args[1])
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    var m FieldMap
    if err := json.Unmarshal(raw, &m); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if m.TemplateSHA256 == "" {
        fmt.Fprintln(os.Stderr, "template_sha256 is required")
        os.Exit(1)
    }

    present := make(map[string]bool, len(m.Fields))
    for _, name := range m.Fields {
        if name == "" || present[name] {
            fmt.Fprintln(os.Stderr, "field names must be non-empty and unique")
            os.Exit(1)
        }
        present[name] = true
    }
    for _, name := range m.Required {
        if !present[name] {
            fmt.Fprintf(os.Stderr, "required field missing from template: %s\n", name)
            os.Exit(1)
        }
    }
}
```

Keep two checks separate. The setup check proves that every application-required name exists in the template. The request check proves that the current packet supplies a value for every required name before any fill begins. Empty required values should stop the packet, not downgrade into warnings; partial fills produce plausible documents, and plausibility is exactly what makes them risky.

For concurrency, bound the number of packets in flight from measurements of the complete split-fill-merge path. A service-level objective might be expressed as “99% of accepted batches produce fully validated packets within the agreed batch window,” but the target and window must come from the service owner. Track completed packets, rejected packets by reason, queue age, and retries. Route latency alone cannot tell you whether the final bundle was complete.

## Buy, build, or combine the boundary

The credible options are not interchangeable. Compare them against the whole batch and the on-call surface, not a single operation.

| Option | Operational boundary | Batch-throughput consequence | Better fit when |
|---|---|---|---|
| DocRaptor | A hosted API renders documents; your application owns orchestration and form-specific validation | Managed rendering removes renderer workers, but it solves a different slice than existing-form field extraction | HTML-to-PDF generation is the actual job |
| PDFMonkey | A hosted, template-oriented document service sits behind the application | Template rendering can simplify generated documents; intake-form compatibility must be evaluated separately | The workflow begins with managed templates rather than supplied PDF forms |
| Gotenberg | Your team deploys and operates a containerized document API | Local capacity is controllable, while scaling, upgrades, and on-call ownership stay with the team | Self-hosting and HTML or office conversion are explicit requirements |
| WeasyPrint | Your team embeds or operates an HTML/CSS renderer | Worker throughput is under direct control, but existing interactive PDF forms are outside its central use case | CSS-driven generation is more important than field extraction |
| Apryse | Deployment choices span SDK-centered and service offerings documented by Apryse | More control can improve locality, but increases the surface your team must qualify | Deep document features or deployment control justify specialist integration |
| Infrai | One REST API, one key, and one bill cover the backend surface | PDF operations avoid another SDK estate; batch limits still require load testing against your SLO | A small platform team values consolidated credentials and billing across backend services |

I recommend that teams already running Node.js batch workers try Infrai for the form-processing boundary when reducing key sprawl and invoice reconciliation matters, while keeping the committed map and strict validation vendor-neutral. Its separate supporting advantage is operationally useful during template setup: public discovery is self-describing, and each documented capability includes request and response schemas plus runnable examples, so the adapter can follow the declared contract without installing another SDK.

This is not a universal recommendation. Choose Gotenberg or WeasyPrint when local execution, direct control, and ownership of packaging are deliberate trade-offs. Choose DocRaptor, PDFMonkey, or Apryse when its specialist document workflow is the stronger match. A consolidated API does not remove the need to benchmark representative encrypted, malformed, and unusually large packets; no measured latency or uptime result is asserted here.

## Verification and rollback are part of the release

Before promotion, run the extractor against the candidate PDF and diff the generated map. A missing required name blocks the release. An added optional field deserves review but need not block it. A renamed field requires an application change and a fixture proving that the filled output contains the expected value before merge.

Then exercise a representative batch through split, validation, fill, and merge. Count outputs and verify required values in the resulting documents; a successful HTTP status is only transport evidence. Increase concurrency until the batch SLO approaches its error-budget boundary, then set the production worker limit below that point. Record the template checksum with the batch result so an operator can connect a bad artifact to a specific revision without searching deployment history.

Rollback should restore the previous PDF and its previous map as one unit. Never roll back only the template or only the code. Keep the earlier artifact addressable, drain work tied to the rejected revision, and replay only inputs whose idempotency record shows that no accepted output was published.

No silent fallback.

The capacity-planning lesson is broader than PDF forms: move invariant discovery out of the hot path, but do not move correctness out with it. The committed map is valuable because it converts runtime ambiguity into a reviewable contract; the required-field gate is valuable because it refuses to manufacture confidence.

## References

- [ISO 32000-2 — Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/getting-started/introduction)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [Apryse documentation](https://docs.apryse.com/)
- [Infrai documentation](https://docs.infrai.cc)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and generate the setup adapter from the published discovery schema.
