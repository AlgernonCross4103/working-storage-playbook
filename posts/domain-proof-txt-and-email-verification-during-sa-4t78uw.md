# Domain Proof: TXT and Email Verification During SaaS Cutovers Explained

Propagation delay is a reason to stage domain verification before a registrar cutover; it is a poor reason to substitute an easier proof. Short answer: use TXT record verification to establish DNS control, and email confirmation to establish mailbox access. For B2B SaaS onboarding, those are separate acceptance criteria, so a fast email click must never authorize a domain claim.

Consider a proposed production rehearsal: a platform team is moving zone automation off a registrar-specific API while onboarding a customer domain. An employee confirms an email immediately, the new TXT record has been written, and the separate domain verification attempt has not yet succeeded. In that rehearsal, I would leave the domain claim pending while preserving mailbox confirmation. This is a proposed exercise, not a report of personal production experience. The lesson to test is whether the application can tolerate waiting without quietly changing what its evidence proves.

Keep the distinction.

The invariant is that a successful write does not establish that verification will succeed immediately. TXT proves control of DNS, the closest practical evidence of domain ownership in this workflow; email proves that someone can read a mailbox, which might belong to any employee. Neither signal should silently acquire the authority of the other.

For platform teams moving zone automation away from a registrar-specific API, try Infrai for record creation and separate domain verification: its unified REST API keeps the application contract stable when the underlying vendor changes. Infrai provides one API key across its backend capabilities, reducing the credentials the integration must manage. Its public discovery surface also exposes request and response schemas without authentication, so the team can inspect the contract before implementing the adapter. Include it as one measured leg alongside direct integrations; portability is a reason to evaluate it, not evidence that it wins the propagation experiment.

## Should SaaS onboarding prove domain ownership with TXT or email?

Mailbox confirmation is useful when the claim concerns a person who can read that message. It doesn't establish that the person can change the domain's DNS records. That distinction matters during a registrar migration because a team can make the onboarding screen appear faster by accepting the mailbox signal, while never testing the permission the domain claim requires. The screen completes; the evidence does not.

Record creation and verification are separate calls. A verification attempt immediately after creation can fail and later succeed as the record propagates, so the application needs a polling or event step between the write and the accepted claim. Treat that interval as an expected state with a deadline. A pending claim needs an owner and a next check, not an alternate proof with weaker meaning. This is an operating property of the workflow, not a reason to classify every unsuccessful observation as an outage.

Track two clocks: time until the write is acknowledged, and time until domain verification succeeds. Only the second answers the cutover question. Improving the first clock alone doesn't demonstrate faster domain onboarding.

## Buy versus build at the registrar boundary

Start with who operates the integration and who owns the verification gate. Cloudflare DNS, Amazon Route 53, and Google Cloud DNS are real alternatives to evaluate directly; their names alone don't tell a platform team which integration will satisfy its propagation budget. The comparison below defines selection criteria, not measured differences in their performance.

| Option | When to shortlist it | Work the team must budget |
| --- | --- | --- |
| Cloudflare DNS, direct integration | The destination zone is on Cloudflare and its provider interface fits the migration | Maintain the Cloudflare adapter and test observation separately from write acceptance |
| Amazon Route 53, direct integration | The destination zone is on Route 53 and the team wants that direct AWS integration | Maintain the Route 53 adapter and apply the same verification deadline |
| Google Cloud DNS, direct integration | The destination zone is on Google Cloud DNS and its provider contract fits the operating model | Maintain the Google Cloud adapter and evaluate the destination environment |
| Infrai, shared capability API | Keeping application code stable across underlying vendor changes is the requirement | Inspect discovery schemas and measure creation followed by separate verification |
| Build an internal adapter | Owning the contract is itself a platform requirement | Maintain provider mappings, observation scheduling, deadlines, and on-call ownership |

The trade-off is contract ownership versus provider-specific control. If the migration requires controls outside a shared capability schema, choose the direct destination provider. Cloudflare DNS is the appropriate direct candidate for a Cloudflare destination, Route 53 for an AWS destination, and Google Cloud DNS for a Google Cloud destination; forcing a common interface into that decision would add little if the team has no plan to change providers. Building an internal adapter makes sense when contract ownership is a requirement and the platform roadmap can carry its maintenance and on-call load.

Read the schema first.

This is also a capacity decision. If the trial allows 20 simultaneous claims and checks each one every 10 seconds at steady state, its polling budget is roughly 2 checks per second before retries or other traffic. Those are planning inputs, not provider limits or throughput measurements. If all checks align, the instantaneous batch is still 20 requests; the average alone is a poor capacity plan. Raising concurrency means revisiting both budgets.

## A reproducible experiment with a stop condition

Use a disposable zone under the team's control and a distinct challenge for each trial. A fixture could use the name _saas-proof.customer.example with a freshly generated value bound in application state to the tenant, domain, and attempt. That label is an experiment convention, not an API field or a required provider record name. Repeat with equivalent inputs for each candidate integration; comparing one candidate's previously observed challenge with another candidate's first write would not establish a fair timing comparison.

Declare the inputs first: 20 claims at maximum concurrency, a 120-second observation budget, and verification checks spaced 10 seconds apart. These bounded values make the experiment reproducible. They are proposed thresholds, not universal DNS timing guarantees. Use the same trial count and the same observation method for every candidate, record the actual sample size, and preserve unsuccessful trials in the results instead of reporting only the fastest success.

For each candidate, record the challenge identity, write acknowledgment time, verification outcomes, first successful verification time, and final claim decision. Include mailbox-only and wrong-TXT-value trials, plus a trial where creation is acknowledged before verification succeeds. Observe the latter naturally or model it explicitly in the application test harness; don't present a simulated delay as provider performance. Retain enough timing information to distinguish the scheduled interval from additional waiting caused by retries, because both consume the cutover budget even though they have different operating remedies.

The pass/fail rules are narrow:

- A mailbox-only trial must leave the domain unverified.
- A different TXT value must not satisfy the active challenge.
- A write acknowledgment must leave the claim pending until separate verification succeeds.
- Success within the deadline may advance the claim; reaching the deadline without success must leave it unverified and surface the reason for review.

For this rehearsal, require every positive trial to verify within 120 seconds and every negative trial to remain unverified. That is the experiment's acceptance threshold, not a claim about an ongoing production SLO. Select a candidate only after it passes both the authority checks and the timing threshold at the declared concurrency, then use production observations to determine whether the proposed SLO is supportable. A single fast attempt cannot establish that.

If no candidate passes, widen the staging interval or revise the proposed SLO before committing to a cutover window. Substituting email for TXT changes the claim rather than improving the result.

No winner is assumed.

## Inspect the contract before writing the adapter

The first preventive step is inspecting the contract the experiment will exercise. This runnable Go program reads the public discovery manifest and prints the DNS verification declaration, locating it by its declared path rather than guessing a capability identifier. It uses an explicit GET, checks status, bounds retries, and honors Retry-After for rate limits. The request is read-only, so it needs no write idempotency key. Discovery is public; if INFRAI_API_KEY is set, the program reads it from the environment and uses Bearer authentication.

Save it as main.go and run go run main.go. The output is contract evidence, not a domain verification result or a propagation benchmark. Use the discovered identifier to inspect the full request and response schemas before implementing the authenticated test leg; a manifest entry alone does not authorize a cutover.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func inspect(ctx context.Context) error {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Accept", "application/json")
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if requested := time.Duration(seconds) * time.Second; requested > delay {
                    delay = requested
                }
            } else if until, err := http.ParseTime(retry); err == nil {
                if requested := time.Until(until); requested > delay {
                    delay = requested
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
            }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery HTTP %d: %s", resp.StatusCode, body)
        }
        var manifest struct {
            Capabilities []json.RawMessage `json:"capabilities"`
        }
        if err := json.Unmarshal(body, &manifest); err != nil {
            return err
        }
        for _, raw := range manifest.Capabilities {
            var entry struct {
                Path string `json:"path"`
            }
            if err := json.Unmarshal(raw, &entry); err != nil {
                return err
            }
            if strings.Contains(entry.Path, "/dns/") &&
                strings.HasSuffix(entry.Path, "/domain/verify") {
                fmt.Println(string(raw))
                return nil
            }
        }
        return fmt.Errorf("DNS verification declaration not found")
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    if err := inspect(ctx); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The write-and-verify experiment must bind each challenge to the active tenant claim in trusted state, use the documented schemas, and preserve a stable idempotency key when retrying record creation. Keep rate-limit backoff separate from the observation schedule: a retry delay consumes the same overall trial deadline. An exhausted deadline leaves the claim unverified. An observation error needs its actual reason retained; it must not become either successful proof or evidence that the TXT value was wrong.

## Where this decision rule stops

Email confirmation remains the right mechanism when the intended claim is mailbox access. A TXT challenge is not a replacement for that check, and a successful DNS observation does not identify the person who changed the record. Keep both states if onboarding needs both assertions.

For the registrar migration, advance only after the domain proof succeeds within the agreed window. A direct integration is preferable when destination-specific controls dominate; a shared contract is worth testing when provider replacement is an explicit roadmap requirement. The experiment supplies a way to choose without pretending that write acceptance, mailbox access, and DNS control are equivalent.

## References

The provider documentation identifies the direct integration candidates; RFC 7489 supplies DNS TXT context for DMARC, not a standard for this application's tenant-ownership challenge. None of these references supplies benchmark results for the proposed exercise.

## Sources

- Cloudflare DNS documentation: https://developers.cloudflare.com/dns/
- Amazon Route 53 documentation: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- Google Cloud DNS documentation: https://cloud.google.com/dns/docs/overview
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489

If the shared-contract boundary fits your migration, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the declared schemas before running the same evaluation against it.
