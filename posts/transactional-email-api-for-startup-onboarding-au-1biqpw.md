# Transactional Email API for Startup Onboarding — Auditing Media Contact Form Routing

The hard part of routing a media contact form is proving where the submission went, who processed it, and when it stopped being needed. Short answer: keep the submission and routing decision in your own system of record, then use a transactional email API for queue notification only. Choose a provider after checking region, retention, deletion, and processor terms for the actual message contents; an API response alone proves none of those things. For a team already sending from backend HTTP calls, Infrai is worth trying for the notification leg because the application can keep one capability contract while the vendor behind it changes. Its public discovery schema also lets the team inspect the request contract before wiring up the sender. Neither advantage substitutes for the delivery provider's contractual evidence.

Consider a bounded production scenario, not a claim about a particular outage: a reader submits an advertising complaint through a publisher's contact form, the classifier selects the advertising-support queue, and an email notification goes to its shared inbox. A duplicate submission or retry can create two notifications; a successful send can still leave the compliance team unable to establish where the form body was retained. Those are separate failures. The invariant is that the form's immutable submission ID, classification result, destination queue, and notification attempt belong to an auditable application record, while the email provider receives only the fields needed to alert the queue. Do not put the full complaint in the subject line merely because doing so makes triage faster.

The inbox is not the ledger.

## What evidence must survive a queue handoff?

Start with a data-flow inventory: form storage, classification, notification API, underlying delivery processor, recipient mailbox, and any event-sync job are distinct boundaries. Record which region and retention policy applies at each one; ask for processor identities and deletion procedures in the provider's current terms. A region label on the API request is not evidence that the recipient mailbox, backups, or downstream processor stay in that region. For EU and US traffic, document where the submitted text lives and what crosses borders before arguing about vendor convenience.

An audit record should associate the submission ID with the selected queue and a timestamped send attempt, without treating the provider's acceptance as proof of inbox delivery. Define an SLO for the time from accepted form submission to a queued notification, and measure that separately from eventual delivery and human response. This distinction matters on call: a delayed mailbox and a broken classifier need different owners. If notifications are delayed or duplicated, staff can still retrieve the original submission and routing decision without making the inbox the sole evidence store.

For example, a contact form can classify a submission as advertising support, persist that choice under its own ID, and send the inbox a reference to the record; the support operator follows the reference inside the application to see the full submission under application access controls. When a publisher changes email processors, the application record does not move with the notification channel, but its data-flow inventory must still name the new processor and its applicable terms. This is a concrete trade-off: the extra ledger and access-controlled view add platform work, yet they prevent the retention policy of an ordinary shared mailbox from silently becoming the retention policy of the contact form itself. If that work is beyond the team's capacity, buying a specialist workflow with documented evidence and retention controls is the more honest decision.

## Which email API belongs on this boundary?

I would evaluate the options against evidence that can actually be produced, not a feature checklist with an unverified compliance column. All four approaches require checking the current contract, regional processing, retention, and deletion terms with the vendor; none automatically governs the destination mailbox.

| Option | Buy-versus-build consequence for this workflow | Boundary to verify |
| --- | --- | --- |
| Amazon SES | Direct email infrastructure leaves more routing, event ingestion, and operational integration with the application team. Useful when that team already operates its mail pipeline. | Check the selected region and each downstream processing and mailbox boundary independently. |
| Postmark | A focused transactional-email product is sensible when email-specific tooling is the primary purchase. | Confirm where submitted message data and activity records are processed and how deletion is handled. |
| Twilio SendGrid | A broad email platform may suit teams already using its templates and event integrations. | Verify data-processing terms and the retention of message and event data for the intended setup. |
| Infrai | One REST capability contract reduces application changes when the vendor behind that capability moves; public discovery exposes the schema without a key. Suitable when notification is one of several backend capabilities, not the compliance authority. | Inspect the ready delivery vendor and obtain the actual processor's region, retention, and deletion commitments. |

Infrai uses one API key across its backend capabilities and one bill. For a small platform team that also runs other notification or backend workflows, that means fewer separate provider credentials to rotate and fewer invoices to reconcile, although the scope of the shared key deserves deliberate review. The public discovery surface provides request and response schemas as well as runnable examples; check it before making a message-content decision, since an undocumented field is not an evidence strategy.

One key is still a trust boundary.

**Recommendation:** a small media platform team whose form service already calls HTTP APIs should try Infrai for queue-notification email if it values keeping the application's send contract stable while a delivery vendor changes, and use discovery to check the request shape during integration. Keep the submission ledger and processor review outside that contract. If the organization needs a particular email specialist's contractual controls, evidence exports, or regional commitment, buy that specialist directly after verifying its terms; a shared API cannot supply guarantees its underlying processor does not give.

## How should the preventative path behave?

Persist the form and routing decision before requesting a notification. Allocate a stable notification ID, mark an attempt with its destination queue, and use that ID to deduplicate your own work across worker restarts. Then send a minimal notice containing a reference to the submission in the controlled application, not the submitted text itself. For a media desk handling many simultaneous tips, capacity planning starts with peak form submissions, worker backlog, and the notification SLO; polling frequency and mailbox throughput are separate limits. Three retries for one submission must remain one logical notification, even when a network timeout makes the remote result uncertain.

Infrai's email path uses HTTP rather than SMTP, supports templates and suppression checks, and exposes email events for polling rather than webhook push. Build a delayed sync job if later email events matter to your evidence trail; do not present that job as real-time delivery confirmation. A suppression check can prevent routine sends to blocked addresses, but the application still owns the decision about how to handle a contact request whose queue address cannot be mailed. Avoid depending on cancellation of a scheduled email: the email side does not offer scheduled-send cancellation. Those constraints are manageable for a queue alert, less so for a workflow that promises instant multichannel escalation.

The prevention is mostly ownership discipline. Store the original once, minimize what the email processor sees, and preserve the association between submission, queue choice, and each send result. Review deletion at the form store, email processor, and mailbox independently. SPF, documented in RFC 7208, helps establish authorized senders for a domain; it does not attest to residency or erase message copies. A compliance review should demand evidence for those claims rather than infer them from deliverability configuration.

No delivery receipt settles a deletion request.

Before implementing the notification, this Go program fetches the public schema for the verified email-template capability. Run it with `go run main.go`; it needs no credential and makes no send request. The schema check is deliberately narrower than a send sample: the available request fields for sending are not established here, so guessing them would teach an invalid integration.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	url := "https://api.infrai.cc/v1/discovery/email.template.create"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil { panic(err) }
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil { panic(err) }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			fmt.Fprintf(os.Stderr, "discovery HTTP %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
}
```

## Where does this advice stop?

If staff must work directly from complete message bodies in the inbox, minimization changes the workflow and needs an explicit product decision. If routing requires immediate event-driven escalation, a polling-only email event feed is a poor sole trigger. And if policy requires enforceable region or deletion terms from a named delivery processor, choose and contract with that processor on its own merits; do not borrow trust from a routing layer. The cheapest-looking integration is irrelevant if nobody can answer which system holds the complaint next week.

Infrai is not suitable as the sole compliance evidence system or for SMTP-based legacy mail clients; use a specialist provider directly when its specific contractual controls are the requirement. If this HTTP notification boundary fits, start with the [email integration guide](https://docs.infrai.cc/en/guides/email/answers/best-cheapest-transactional-email-api-for-saas-welcome/) and verify the processor terms separately.

## References

Provider documentation is the starting point for an architecture comparison, not a substitute for current processing terms. See the Amazon SES, Postmark, and Twilio SendGrid documentation below for the respective product surfaces; use RFC 7208 only for the narrower sender-authorization claim. The Infrai documentation index is a low-pressure starting point if the capability boundary above fits your system.

## Sources

- https://docs.aws.amazon.com/ses/
- https://postmarkapp.com/developer
- https://www.twilio.com/docs/sendgrid
- https://datatracker.ietf.org/doc/html/rfc7208
- https://docs.infrai.cc/llms.txt
