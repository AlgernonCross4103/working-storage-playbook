# Marketplace Evidence for Node.js Password Reset Email APIs (Without SMTP Relay)

Short answer: for a Node.js marketplace that sends an order receipt after payment settles, the least complex API-first design is an internal notification command, an outbox tied to settlement, and a narrow HTTP delivery adapter. Do not operate an SMTP relay merely to move bytes. Treat the remote acceptance response as one piece of evidence, correlate it with later delivery events, and preserve an immutable receipt record. The same boundary works for password-reset email, but its content and retention policy must remain separate.

The page says `receipt_delivery_gap`: settled payments have exceeded the receipt SLO without a terminal delivery event. On-call sees a count, the oldest affected order age, and the last successful event-ingestion time. That is enough to decide whether the fault lies in payment-to-outbox publication, API submission, or event intake.

A raw API error rate is not enough.

An HTTP acceptance result proves only that another system accepted a request under a particular identifier. It is not inbox-delivery evidence. Compliance evidence needs a chain: the business event, exact rendered-message version, submission attempt, remote identifier, and later disposition. Missing links should page before customers report missing receipts.

## What should have fired before the receipt gap?

The earlier signal is the age of the oldest settled payment with no durable notification command. It sits upstream of the delivery API, so it detects a broken transaction boundary even when the external service is healthy and returning no errors. A second signal watches accepted submissions that have no correlated terminal event within the SLO window. These are different failure domains and should not be collapsed into one success-rate chart.

Use explicit states: `settled`, `queued`, `submitted`, `delivered`, `failed`, and `suppressed`. A transition carries `order_id`, `message_id`, timestamp, reason, and actor. Keep the recipient address encrypted or tokenized according to organizational policy; dashboards and page payloads need an opaque recipient key, not an email address. The audit record should identify the template revision and a content digest, letting an investigator establish what was intended without copying full bodies into every operational log.

The sharp edge is dual writing. Updating a payment row and then calling an email API can lose the receipt if the process dies between operations; calling first can send a receipt for a transaction that later rolls back. Put the notification command in an outbox in the same database transaction as settlement, then let a worker claim and submit it.

This is a consistency decision, not a vendor feature.

Retries reuse a stable logical message ID. Network timeouts are ambiguous because the remote system may have accepted the request even though the worker did not receive the response. An idempotency key, when the selected API supports one, narrows that ambiguity. The internal state machine still must tolerate duplicate callbacks and repeated polling results.

## Instrument the evidence chain, not the HTTP client

A useful metric records each transition with bounded labels. `channel`, `template_class`, `state`, and `reason_class` are reasonable candidates; `order_id`, recipient, and remote message ID belong in trace or audit storage, not metric labels. High-cardinality business identifiers make monitoring expensive and awkward to query precisely when the page fires.

The focused Go example validates correlation at event ingestion. An adapter converts an authenticated remote event into this internal shape before calling the transition function. Signature verification belongs ahead of the handler, using the selected API's documented scheme.

```go
package receipt

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type DeliveryEvent struct {
	MessageID string
	State     string
	Reason    string
	Occurred  time.Time
}

type Submission struct {
	MessageID       string
	OrderID         string
	TemplateVersion string
	SubmittedAt     time.Time
}

type Store interface {
	FindSubmission(context.Context, string) (Submission, error)
	RecordTransition(context.Context, DeliveryEvent) error
}

var ErrUnknownMessage = errors.New("delivery event has no matching submission")

func RecordDelivery(ctx context.Context, store Store, event DeliveryEvent) error {
	if event.MessageID == "" || event.Occurred.IsZero() {
		return fmt.Errorf("invalid delivery event")
	}

	submission, err := store.FindSubmission(ctx, event.MessageID)
	if err != nil {
		return fmt.Errorf("find submission: %w", err)
	}
	if submission.MessageID == "" {
		return ErrUnknownMessage
	}

	switch event.State {
	case "delivered", "failed", "suppressed":
		return store.RecordTransition(ctx, event)
	default:
		return fmt.Errorf("unsupported delivery state %q", event.State)
	}
}
```

Do not discard an unknown message ID. Quarantine it, count it, and alert on sustained growth. It can indicate event reordering, retention mismatch, an adapter mapping error, or traffic intended for another environment. Make transition writes idempotent using a remote event identifier or deterministic event key because webhook delivery can be retried; exactly-once processing is not a credible assumption at this boundary.

Measure duration from `settled` to `queued`, `queued` to `submitted`, and `submitted` to a terminal event. Alert on the oldest incomplete item as well as the fraction meeting the SLO. Averages hide a stranded order.

Capacity planning starts with settlement peaks, not average daily orders. If a marketplace can settle 30,000 payments in a ten-minute batch, a worker pool designed around the daily mean creates an avoidable backlog. Model drain rate, API rate limit, retry budget, and event-ingestion lag together. The queue must absorb the peak without letting the oldest receipt cross the compliance or customer-communication objective.

## The buy-versus-build boundary

An API removes SMTP relay operation; it does not remove ownership. The platform team still owns business-event capture, recipient authorization, template review, suppression policy, audit retention, and reconciliation. Keep the service boundary narrow enough to test a second adapter without rewriting payment code, but do not flatten every external event into an unhelpful `success` boolean.

| Concern | Buy behind an adapter | Build and operate internally | Decision test |
|---|---|---|---|
| Mail transfer and destination retries | External HTTP delivery API | SMTP infrastructure and queue operations | Can the team support authentication, abuse response, and delivery incidents? |
| Compliance evidence | Remote event plus internal ledger | Fully internal event and delivery ledger | Can an auditor join a settled order to an exact revision and disposition? |
| Templates | Remote or internal rendering | Internal rendering and artifact storage | Which side provides deterministic revisions and review history? |
| Channel fallback | Separate email and SMS adapters | Internal orchestration layer | Does policy permit SMS for this message class, with consent recorded? |
| Exit cost | Stable command schema and exportable evidence | Protocol and storage ownership | Can historical evidence be read after an adapter changes? |

For a small platform team, outbound mail transfer is a substantial on-call commitment. Buying that layer can be the narrower ownership choice, but the decision is conditional: reject an API that cannot expose stable message identifiers, authenticated events, suppression outcomes, required data-location terms, or evidence exports. Those are acceptance criteria, not a product ranking.

SMS is a separate channel with different consent, addressability, length, and delivery semantics. Twilio's documentation is primary evidence for one commercial SMS API's messaging model, but it does not make SMS an automatic fallback for a receipt or password reset. Define fallback in policy, including which message classes may cross channels, rather than allowing a retry worker to improvise.

## Should a password reset email implementation use an API first?

A password-reset message and an order receipt can share the command envelope, worker mechanics, observability, and delivery adapter. They should not share payload retention by accident. A reset message carries a short-lived credential capability; retain a one-way digest of the reset token server-side, bind it to a user and expiration, invalidate it after use, and keep the raw token out of logs and audit events. The receipt record may need a longer business retention period because it documents a completed transaction.

That split argues for an internal interface with a message class and evidence policy, rather than a generic `sendEmail(to, subject, html)` helper scattered through Express or Next.js handlers. The web request creates the security or business event. The outbox owns eventual submission. The adapter knows the external API. None should decide retention ad hoc.

Usually, yes. The limitation of an API-first adapter is that the external service controls part of the submission and event vocabulary, so evidence portability depends on retaining normalized internal transitions alongside the original remote identifiers. It is unsuitable when policy requires the organization to operate every mail-transfer component, when a disconnected environment cannot reach an external endpoint, or when required evidence cannot be exported under acceptable terms. A self-operated SMTP path can fit those constraints, but it transfers queue durability, authentication changes, bounce processing, abuse handling, reputation work, and delivery incident response to the platform team. The trade-off is concrete: an external API reduces mail-transfer operations while adding dependency and exit risk; a self-operated relay increases control while expanding the on-call surface. Express and Next.js do not alter that decision. They should authorize the reset request and commit the outbox item, then return without waiting for final delivery. For marketplace receipts, payment settlement creates the command instead. Both flows gain a stable audit boundary, yet a reset token must expire and disappear while a receipt's business evidence follows its separately approved retention schedule. This is why one reusable transport adapter is sensible and one universal evidence policy is not.

Keep those policies apart.

Domain authentication also remains on the launch checklist. SPF, defined by RFC 7208, lets a receiving system evaluate whether an SMTP client is authorized to use a domain in the envelope identity. It is one control, not proof that a particular customer received a message. Preserve SPF configuration and change history as configuration evidence, while keeping delivery-state evidence in the message ledger.

Test the failure paths before production: commit settlement while the worker is stopped, time out after submission, replay a remote event, deliver events out of order, rotate event-verification secrets, and exhaust the retry budget. Deployment can support an adapter shadow mode that renders and validates commands without contacting an external API. Synthetic messages may exercise the full path, but they need an isolated recipient and must never be confused with customer evidence.

## Where should the page threshold sit?

Set it from the user-facing receipt SLO and demonstrated drain capacity. Page when an engineer can act before the oldest pending receipt breaches the objective, not at the first transient API error. A warning can surface a falling submission rate or rising queue age earlier; the page should identify the affected stage and link to quarantined evidence records.

Backtest both age and error-ratio conditions against load tests and planned dependency interruptions. Record how long the queue takes to drain at peak input, then reserve headroom for retries. A threshold that fires on every brief callback delay trains on-call to distrust the page, while one based only on terminal failures misses a worker that has stopped consuming entirely.

False positives have a measurable cost: interrupted sleep, rushed production access, and eventual alert muting. False negatives leave buyers without receipts and erase the time available to reconstruct evidence before support tickets arrive. The defensible design pages on an SLO-threatening gap in the state chain, carries enough context to locate that gap, and keeps external API health as supporting evidence rather than the definition of success.

## Further reading

- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- Twilio SMS documentation: https://www.twilio.com/docs/sms
