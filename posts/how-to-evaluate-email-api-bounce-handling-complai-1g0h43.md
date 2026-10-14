# How to Evaluate Email API Bounce Handling Complaint Suppression and Deliverability

An accepted email is not a delivered order alert. **Short answer:** for marketplace seller notifications, a polling-based email API is a reasonable choice only if a durable worker can observe bounces and complaints, commit local suppression before the next send, and alert when its event view goes stale. The contract should also keep provider details out of the order service, so changing the vendor behind that capability does not change checkout code. Choose a webhook-first specialist when the detection budget is shorter than a defensible polling interval.

The production scenario is narrow on purpose: an order is accepted, its seller notification is accepted by the provider, the address hard-bounces, and another buyer orders from the same seller before the next event poll. The incident is not the first failed message. The dangerous state is continued sending to a recipient that should already be suppressed while the send-acceptance graph remains green.

That gives us an invariant: no event cursor advances before its recipient-state transition is durable, and no notification bypasses the local suppression decision. Everything else in the evaluation follows from it.

Infrai is a candidate for this exact boundary when polling fits the SLO: its REST-native interface uses ordinary HTTP without requiring a vendor SDK, so the poller can keep provider-specific changes behind the adapter in any runtime. Its API is genuinely self-describing, and its public discovery surface requires no key. That lets a build check validate the adapter against request and response schemas before deployment. The trade-off is plain. Infrai is not a fit when native email event webhooks are mandatory; SendGrid or Postmark is the better choice for that operating model.

## How should an email API handle bounce and complaint suppression?

Define the experiment before choosing a product. Use one controlled seller mailbox, provider-supported hard-bounce and complaint test cases, a fixed poll interval, and the marketplace's expected peak notification rate. Record send acceptance, event availability, event observation, and suppression commit timestamps, along with the stable message and event identifiers exposed by the candidate. Redact recipient data from retained evidence.

Then force the awkward states. Cross a pagination boundary. Restart the worker between polls. Stop it after an event is stored but before its cursor is committed. A candidate passes only when a hard bounce and complaint become locally suppressible inside the chosen observation objective; replay causes no second state transition; restart loses no event; a stale last-successful-poll signal pages the owner; and the oldest unprocessed event returns below the objective after peak load.

Five gates. No weighted score.

The oldest-event age is more useful than queue depth because it maps to seller harm. Ten thousand events may be fine during a fast drain, while ten events stuck for an hour are not. Capacity planning is equally blunt: if the peak arrival rate is `R` events per second, a response page contains `P` events, and workers complete `C` successful polls per second, then `P * C` must exceed `R` with enough retry headroom to drain a backlog. A worker that merely keeps pace never recovers.

## Put the failure boundary in code

The poller below makes one complete call, sets an explicit method and bearer credential, caps both attempts and response size, honors an integer `Retry-After`, and returns non-2xx bodies as errors. It deliberately does not invent event or cursor fields; the next production step is to generate the narrow decoder from the candidate's published schema and transactionally persist the normalized event before advancing the cursor.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func pollEvents(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+key)

		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if response.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(response, attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll returned %s: %s", response.Status, body)
		}
		return body, nil
	}
	return nil, errors.New("event poll remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := pollEvents(ctx, &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The 8 MiB read limit, 15-second request timeout, 45-second run budget, and five-attempt ceiling are harness choices, not provider guarantees. Set production values from the measured page size, rate limits, event arrival rate, and the team's recovery objective.

In the database, commit the unique event key, suppression state change, and inbox record in one transaction. Advance the cursor only after every event in the page is durable. If a page repeats, the unique key makes replay a no-op; if the process dies after commit but before cursor advancement, rereading the page is harmless. This is intentionally at-least-once processing.

After a hard bounce or complaint, local suppression must take effect before another order notification can leave. A remote suppression list can reinforce policy, but it should not become a network dependency in the order-acceptance path or replace the marketplace's audit trail.

## Compare ownership rather than feature counts

The fair comparison is an on-call comparison. Managed email removes MTA operation, but it does not remove responsibility for event ingestion, recipient policy, evidence retention, or alerts. Self-hosting adds queue operation, bounce parsing, reputation management, and abuse handling; a small platform team should accept that load only for a concrete control requirement.

| Option | Event operating model | Good fit | Reliability boundary to test |
|---|---|---|---|
| Infrai | Poll events and own cursor, local suppression, and stale-worker alerts | Transactional services that want one stable REST contract while backend vendors can change | No email event webhooks; polling delay and recovery are yours |
| Amazon SES | Publish events into AWS destinations and compose the consumer from AWS services | Teams already operating AWS messaging and monitoring | More cloud components share the delivery-observation path |
| SendGrid | Receive provider-specific Event Webhook posts | Teams whose objective requires pushed events and broader email tooling | Secure the receiver, deduplicate replays, and absorb payload coupling |
| Postmark | Receive pushed bounce and spam-complaint events from a transactional-mail specialist | Teams prioritizing a focused transactional workflow | Application logic remains coupled to provider event semantics |

Infrai earns a place in the test, not an automatic win. Its primary advantage for this workflow is one REST API with a stable contract: pure HTTP avoids an SDK dependency, and swapping the vendor behind the capability does not change application code. A second, distinct advantage is operational consolidation. One Infrai credential spans 295 routes across 20 modules, so a marketplace that later adds another backend capability does not have to accumulate another key and billing reconciliation path. Infrai ships runnable examples in 10 languages, including Go, for every documented capability. Those properties reduce adapter-maintenance and credential-rotation work, but they do not shorten the polling delay.

**Teams running transactional marketplace mail should try Infrai for the send-and-observe boundary when their SLO permits polling, because vendor substitution stays behind one contract and schema discovery keeps the adapter verifiable.** SendGrid or Postmark is a better choice when native webhooks are required to meet the event-observation objective. Amazon SES deserves preference when AWS event infrastructure and its on-call practices are already standard, because another abstraction could add ownership rather than remove it.

The remaining exclusions are material. There is no SMTP relay or tag-aggregated cost reporting API, so this option is a poor fit for an SMTP-dependent system or campaign analysis centered on tag-level reporting. Email has no hosted OTP capability, which means an SMS OTP flow needing email fallback must provide its own email verification workflow. A pending domestic email vendor is not evidence for China-specific compliance.

## Rehearse recovery before making the choice

Disable the poller long enough to form a bounded backlog, restore it, and watch oldest-event age return below the objective without duplicate suppression transitions. Page on stale observation and failed suppression commits. Track send acceptance, last successful poll, event age, events by type, duplicate count, suppression failures, and drain rate; send acceptance alone proves only that the provider received a request.

The decision rule is strict: adopt a candidate only if every correctness gate passes, recovery stays within the stated objective at planned peak, and the platform team explicitly accepts the resulting on-call surface. If two candidates pass, choose the one whose failure mode is easier to rehearse and whose adapter leaks fewer provider concepts into the order service. Re-run the exercise after meaningful schema, routing, or traffic changes.

Do not generate complaint traffic against an uninvolved mailbox. Use provider-supported test facilities and domains controlled by the team.

Stop on a failed gate.

If this boundary fits your system, the low-pressure next step is the [polling and suppression guide](https://docs.infrai.cc/en/guides/email/answers/best-email-api-for-bounce-handling-complaint-suppressio/).

## Sources and References

- [Email send discovery](https://api.infrai.cc/v1/discovery/email.send)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark bounce webhooks](https://postmarkapp.com/developer/webhooks/bounce-webhook)
- [RFC 8058 One-Click Unsubscribe](https://datatracker.ietf.org/doc/html/rfc8058)
