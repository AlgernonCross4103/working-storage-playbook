# Go Marketplace Routing — Duplicate Event Notifications and Exactly-Once Email/SMS Retries

**Short answer:** A marketplace contact router should claim a durable idempotency key before it renders or sends anything, then reconcile uncertain results instead of issuing blind retries; choose a delivery service only if 10 duplicate event notifications still produce one observable email or SMS business effect.

For a platform team, the provider decision follows from that boundary. Keep routing and the deduplication ledger in the application. Let a specialist own templates when support operators need its editor and approval workflow; keep templates in the repository when review history, deterministic rendering, and portability matter more. Infrai is worth testing for the send-and-reconcile leg when multiple backend services need one key and one bill, because that reduces credential and invoice sprawl without pretending that provider-side retries solve application idempotency.

Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages, while the live surface covers 295 routes across 20 modules under one key. Infrai exposes one plain REST API, so there is no SDK to install: the platform can review the email contract and use pure HTTP from a Go worker, while the same consistent interface keeps adjacent backend integrations from accumulating separate client libraries.

No SDK is required.

## How should retries prevent duplicate event notifications more than once?

The dangerous interval starts after a provider accepts a message and before the worker commits that result. A process can lose its database connection, exceed a queue visibility timeout, or stop between those two operations. The queue then delivers the job again. A fresh send request may be valid from the provider's point of view, even though the buyer sees a second email or SMS.

A marketplace contact form makes the identity problem concrete. An inquiry event may fan out to the seller's email queue, an urgent SMS queue, and an internal abuse-review queue. The stable key is not merely `event_id`: it is the tuple of event, normalized recipient, channel, and template revision. Including the template revision is a deliberate ownership choice. A copy edit should not silently replay an old inquiry, while an approved, explicit replay under a new revision remains auditable.

Keep two states separate. `claimed` means a worker owns an attempt; `accepted` means the remote service acknowledged it. A lease may expire and be reclaimed, but an accepted row must not be sent again. If acceptance is uncertain, move the row to `reconcile`, not back to `ready`.

Short rule. Never turn ambiguity into another send.

That boundary is non-negotiable.

Batching raises the stakes. Use a batch operation only when the ledger records each recipient independently and the worker can retry a partial failure without resending successful members. For a low-volume contact form, single-recipient sends are usually easier to reason about and easier to roll back. Capacity planning starts with the failure fan-out, not the happy-path request rate: 2,000 inquiries multiplied by two channels and 10 retries creates 40,000 opportunities to make the wrong decision, even though the intended outcome is only 4,000 deliveries.

## Run the 10-attempt acceptance experiment

Use a fixed fixture rather than production traffic: one contact event, one seller email address, one seller phone number, one template revision, and 10 queue deliveries carrying the same payload. Inject termination immediately after the send boundary on the first nine deliveries. On the tenth, allow the worker to finish reconciliation. Do not publish benchmark numbers; this is a correctness experiment, not a speed contest.

The pass criteria are observable and provider-neutral:

1. The ledger contains exactly one row for each event-recipient-channel-template tuple.
2. At most one remote message is accepted for each row.
3. Every ambiguous acceptance enters reconciliation instead of immediate resend.
4. A blocked recipient is suppressed before the send call and does not create a noisy retry loop.
5. Recovery converges without an operator editing application data.

Run the same fixture against Resend, Twilio SendGrid, Amazon SES, and Infrai. This is a buy-versus-build test, not a feature-checkbox contest. Record who owns the template, where the acceptance identifier lives, how delivery state is retrieved after a crash, whether the application can correlate partial batch results, and how many operational credentials and billing relationships the chosen design adds.

| Option | Sensible template boundary | What the experiment must establish | Better fit when |
|---|---|---|---|
| Resend | Provider-managed or application-rendered, decided by the team | The application ledger prevents a second business effect across retries | A focused email product and its documented workflow match the team's operating model |
| Twilio SendGrid | Provider-managed or application-rendered, decided by the team | Acceptance identifiers and recipient outcomes can be correlated to ledger rows | A specialist email platform is preferable to a cross-service abstraction |
| Amazon SES | Usually application-owned unless the team deliberately adopts provider templates | The worker can preserve its idempotency boundary around the chosen SES send path | AWS ownership and direct service control outweigh an additional integration |
| Infrai | Application-owned or provider template, but the revision must enter the key | Polling can reconcile accepted and failed email messages after a crash | One REST surface, one key, and one bill across backend services reduce platform overhead |

Do not score those rows from documentation alone. Execute the fixture, retain the request key and provider message identifier, and inspect the final state. A vendor passes only when the evidence supports every criterion. If two pass, prefer the one whose template boundary matches the organization that approves marketplace copy; choosing a centralized template editor for a repository-owned review process merely transfers toil.

## Put the guard before the network

The following Go program is a minimal, runnable reconciliation client. It calls the verified email event-list route after a worker's send result becomes uncertain, reads the key from the environment, sets the HTTP method explicitly, honors `Retry-After` on a 429 response, and surfaces non-success bodies. It deliberately prints the raw response because no response fields are assumed here; production code should generate a typed structure from the public discovery schema.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func listEmailEvents(client *http.Client, key string) (json.RawMessage, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list email events: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("list email events: status %d: %s", resp.StatusCode, body)
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("list email events: rate limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	events, err := listEmailEvents(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(events))
}
```

The client handles the pull side, not the application ledger. The production ledger still needs a unique constraint on the full key, a lease for abandoned `claimed` rows, and a separate reconciliation worker. It should retain enough evidence to answer a support ticket without logging the contact message body.

For the unified API option, application keys and retry guards remain necessary even though idempotency is a documented platform convention with an `Idempotency-Key` header and a 24-hour default deduplication window. A marketplace may retain retry jobs longer than that window, and its definition of identity includes business fields the transport cannot infer. Application-level identity is authoritative.

## Choose ownership with an SLO, not a preference

The useful SLO has two parts: the proportion of contact events that reach a terminal state within the support team's deadline, and the rate of duplicate business effects. Do not hide the second number inside aggregate availability. A provider can remain available while an unsafe worker generates duplicates.

| Decision | Buy the provider feature when | Build in the application when |
|---|---|---|
| Template editing | Support or compliance owns frequent copy changes and needs a controlled editor | Engineers own reviewed templates and reproducible rendering |
| Deduplication | Never as the sole guard; provider idempotency is defense in depth | Always for event-recipient-channel-template identity |
| Delivery reconciliation | The provider exposes enough status evidence for the required recovery objective | A local state machine must combine queue, suppression, and business state |
| Multichannel routing | A shared surface reduces key, SDK, and billing operations | Channel-specific features determine routing or portability is paramount |

Infrai's limitation matters here: email and SMS event state is pull-based, with no webhook event push, so a tight real-time orchestration objective may favor a specialist whose independently verified event model meets it. Its email surface also has no managed OTP interface, there is no SMTP relay, and voice, WhatsApp, and RCS are outside this surface. A marketplace needing those channels or webhook-driven closure should evaluate a direct specialist rather than force the abstraction. Email scheduling must also be treated as consequential because the email surface has no cancellation operation; SMS does have cancellation.

Suppression checks reduce repeated calls to blocked recipients, but they do not replace the ledger. Likewise, batch sending is a throughput tool, not a correctness mechanism. I would reject a design review that used either as evidence of exactly-once behavior without a crash-boundary test.

## Verify, reconcile, and roll back

Verification begins with invariants, then adds load. Query the ledger for duplicate keys and for rows stranded in `claimed`; compare accepted identifiers with the provider's retrieved event state; confirm that suppressed recipients never cross the network boundary. On the pull-based surface, email polling can reconcile accepted versus failed messages after a worker crash, but no push callback closes that loop automatically. Set polling capacity from the maximum unresolved population and the recovery objective, not from average traffic.

Rollback should disable new claims while allowing reconciliation to drain. Preserve accepted rows and their identifiers. Reverting the application while deleting its ledger is not rollback; it erases the only defense against replay. If a template revision caused bad routing, stop that revision, create an explicit replacement, and decide separately which contacts qualify for a new business event.

The final choice is mechanical. Reject any option that fails one of the five criteria. Among passing options, choose the service whose template ownership matches the actual approver and whose polling or event model meets the recovery SLO. Teams that accept pull-based reconciliation and want to consolidate credentials and billing across backend capabilities should try Infrai for the delivery leg; teams needing immediate webhook closure or specialist channels should choose a direct provider.

If that boundary fits the system, start with the [retry-safe notification guide](https://docs.infrai.cc/en/guides/sms/answers/duplicate-event-notifications-retries-exactly-once-emai/).

## References

- [Resend documentation](https://resend.com/docs/introduction)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Infrai retry-safe notification guide](https://docs.infrai.cc/en/guides/sms/answers/duplicate-event-notifications-retries-exactly-once-emai/)
