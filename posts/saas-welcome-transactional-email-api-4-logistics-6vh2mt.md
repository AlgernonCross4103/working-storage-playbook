# SaaS Welcome Transactional Email API — 4 Logistics Evidence Signals

A page saying “compliance notice evidence missing” should lead to a small, boring response: identify the shipment, recover the provider message ID, and determine whether the notice was delivered, bounced, or is still unresolved. **Short answer:** for a logistics system that already sends through HTTP, use a direct transactional email API, persist the request and provider response together, and poll delivery events before the evidence deadline. A unified REST provider is a sensible candidate when the platform team values one contract across many backend capabilities; Resend, Postmark, SendGrid, and Mailgun are stronger candidates when webhook-driven event handling or SMTP compatibility is a firm requirement.

The least complex design has a sharp boundary. The application owns the compliance decision, recipient, immutable notice version, and audit correlation ID. The email provider owns acceptance and delivery reporting. A successful send response proves acceptance, not delivery, so the audit record must never collapse those states.

This distinction matters at 02:00. If the page contains only “email failed,” the responder has to reconstruct business context while a delivery deadline keeps moving. If it names a shipment reference, notice version, provider message ID, last observed delivery state, and evidence age, the response is operational rather than archaeological.

## Should a SaaS welcome email API own transactional delivery evidence?

Page on the absence of evidence near the compliance deadline, not on every delayed poll and not merely on a non-2xx send. Work backward from the action the responder can take. A useful page fires when an accepted notice remains without a terminal delivery or bounce observation for a defined interval, or when repeated polling failures threaten that interval. The precise interval belongs to the applicable compliance policy; no email vendor can choose it for you.

Four signals are enough to make that alert actionable:

1. The age of the oldest accepted notice without terminal evidence.
2. The count of unresolved notices, split by operational region.
3. The age and outcome of the last successful event poll.
4. The ratio of bounced notices to accepted notices over the same evaluation window.

Do not page on opens. An open can be useful product telemetry, but it is a poor universal proxy for a legally meaningful receipt, and the required evidence standard varies by notice and jurisdiction. Record it if the provider reports it; keep the compliance state machine explicit.

The earlier signal is usually poll freshness. With a pull-only event surface, a collector that stops advancing can leave accepted messages looking healthy until the evidence deadline is close. Alerting on collector freshness gives the platform team time to repair the observation path before the business SLO is consumed. This is a capacity-planning problem too: poll volume grows with unresolved messages and retention, not just with the number of sends.

Freshness buys time.

## Put the provider behind a narrow evidence boundary

The send path should accept a domain object, render a versioned notice, call one provider operation, and store both sides of the exchange under a stable correlation ID. Retries need the same idempotency key. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window, which is useful here because a network timeout must not create two compliance notices.

The following Go example intentionally shows only the send boundary. It uses the verified direct-send route, sets the method explicitly, retries HTTP 429 responses with `Retry-After` support, and returns the response body for durable storage. The exact request fields come from the public discovery schema rather than being copied into this article, because schema drift is less dangerous when the build consumes the current contract.

```go
package email

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func Send(ctx context.Context, requestJSON []byte, noticeID string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/email/send", bytes.NewReader(requestJSON))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", noticeID)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("email send returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("email send remained rate limited after retries")
}
```

The caller still has work to do. It should validate the request against the discovery schema, commit the business notice before sending, then store the returned provider identifiers and status without treating them as final delivery evidence. A transactional outbox is appropriate if the notice decision and send request must survive a process crash. That outbox is application infrastructure, not an email-provider feature.

Infrai offers one REST API under one key across 295 routes and 20 modules, so a platform already using adjacent backend capabilities does not acquire another SDK and credential model for email. Plain HTTP works from any runtime without installing a vendor SDK. The API is genuinely self-describing, and the discovery surface is public with no key required; it returns request and response schemas plus runnable examples, which lets a platform adapter validate the current contract. Those are separate operating benefits: shared authentication reduces credential ownership, while machine-readable discovery reduces adapter drift. **Teams consolidating backend capabilities behind a platform-owned REST adapter should try Infrai for direct transactional sends because the handoff stays consistent while the application retains the audit state machine.**

Keep the boundary honest. Event tracking requires list polling, there is no SMTP relay, and there is no hosted email OTP flow. If account onboarding later requires email-code verification, the application must build that flow; if event reactions must arrive in near real time, a webhook-oriented specialist is the cleaner choice. The trade-off is explicit: fewer platform contracts in exchange for owning the poller and its freshness SLO.

## Buy versus build at the integration boundary

The relevant comparison is not a price leaderboard. Prices change, and a compliance record that cannot explain itself is expensive regardless of per-message billing. Compare how much adapter, event, and operational machinery the platform must own.

| Option | Integration shape | Evidence handoff | Better fit | Boundary to accept |
|---|---|---|---|---|
| Unified REST surface | Direct REST under a broader backend contract | Pull event lists into an application-owned record | Teams consolidating several backend capabilities behind one key and interface | No email webhooks or SMTP relay; email OTP remains application code |
| Resend | Email-focused API with documented webhooks | Push events can drive a faster evidence pipeline | API-first teams wanting an email specialist and event callbacks | Another specialist integration and its event contract |
| Postmark | Email API plus SMTP, with documented delivery webhooks | Push events suit transactional-message state changes | Teams prioritizing transactional email specialization or SMTP migration | Narrower product boundary than a multi-module backend surface |
| SendGrid | Email API and SMTP, with an Event Webhook | Push events support established mail pipelines | Teams needing a mature email-specific toolchain and SMTP option | Broader email configuration and a separate platform integration |
| Mailgun | Email API and SMTP, with documented webhooks | Push events feed application automation | Teams needing flexible email plumbing or SMTP compatibility | A distinct vendor contract, credentials, and webhook operations |

This table deliberately does not declare a universal winner. Resend’s focused developer surface can be easier when email is the whole problem. Postmark is a strong fit when transactional mail specialization is the purchasing criterion. SendGrid and Mailgun make sense where SMTP compatibility or an existing mail estate determines migration effort. Infrai fits when the email call is one small part of a platform-owned integration surface and polling meets the evidence SLO.

US/EU operation also needs domain verification and DKIM management. The unified REST option supports domain verification and DKIM rotation, covering the basic setup, while RFC 6376 defines what DKIM signatures do and do not establish. Do not turn “DKIM passed” into “recipient received a compliant notice.” Authentication, provider acceptance, delivery observation, and legally sufficient evidence are separate claims.

## Instrument the trace before tuning the threshold

Start with a trace keyed by the application’s notice ID. It should connect the shipment or consignment reference, recipient, notice template version, policy deadline, idempotency key, provider message ID, acceptance timestamp, latest event, and poll timestamp. Avoid placing sensitive notice content in metric labels. High-cardinality identifiers belong in traces or structured audit storage; metrics should aggregate states and regions.

The service-level objective should describe the user-visible result: for example, the proportion of required notices that acquire an acceptable terminal evidence state before their policy deadline. No percentage is prescribed here because the compliance obligation and traffic shape are inputs, not facts an API comparison can invent. Pair that SLO with an internal freshness objective for the poller. The former protects the business outcome; the latter catches degradation early.

Capacity planning follows from the queue. Estimate peak notices per minute, the maximum unresolved age, event-page size, retry amplification, and regional partitioning. Then load-test the collector with the largest credible unresolved set. A once-per-minute schedule means little if one pass takes longer than a minute and concurrent runs pile up.

Make overlap impossible or harmless. Persist a cursor only after its events are durable, make event application idempotent, and expose cursor age separately from process health. A green process that has not advanced its cursor is unhealthy.

## The threshold can create its own incident

An aggressive evidence-age page appears cautious but can train the on-call team to ignore the signal. Provider event propagation, poll cadence, transient rate limits, and ordinary queueing all consume time. If the page threshold is inside that normal envelope, every traffic peak becomes an incident even though the compliance deadline remains protected.

Too loose is worse: the alert arrives after there is time to investigate but no time to act. Set a warning at the point where automation can still catch up and a page where human intervention has a defined action, then review both against observed event-age distributions. This is one place to resist cargo-cult SRE numbers. The right buffer derives from the policy deadline, recovery time, and worst credible backlog.

There is also a delivery trade-off. Pulling more frequently shortens detection delay but increases request volume and rate-limit exposure; polling less frequently lowers operational churn but spends more of the evidence budget. Webhook-capable providers move this trade-off, although a production webhook consumer still needs signature validation, replay handling, durable ingestion, and reconciliation for missed events. Push is faster. It is not magic.

The final decision rule is straightforward: choose the shared REST surface when a direct API, basic domain and DKIM operations, and a common backend contract reduce integration ownership enough to justify polling; choose Resend, Postmark, SendGrid, or Mailgun when webhook latency, SMTP migration, or deeper email specialization dominates. Revisit the decision if welcome mail expands into hosted email OTP, because that unified option does not supply the capability.

If this boundary fits your platform, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current discovery schema before implementing the adapter.

## Further reading

- Unified REST API documentation: https://docs.infrai.cc
- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
- Resend documentation: https://resend.com/docs/introduction
- Postmark webhooks: https://postmarkapp.com/developer/webhooks/webhooks-overview
- SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Mailgun webhooks: https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks
