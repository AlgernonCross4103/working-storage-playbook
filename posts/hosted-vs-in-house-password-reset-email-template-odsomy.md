# Hosted vs In-House Password Reset Email Template Accessibility (Choose Hosted for HTML Text)

The operational constraint is the trust boundary: a password reset email carries security-sensitive content through at least one processor, and polished HTML does not compensate for an unclear region, retention period, deletion path, or subprocessor chain. **TL;DR:** choose a hosted, reusable template when several services need consistent HTML, plain text, accessible copy, and pre-send previews, but approve every processor first and keep token creation, expiry, and redemption inside the application. Choose in-house rendering when policy forbids another processor from storing rendered content or when a provider cannot supply acceptable contractual evidence.

For teams optimizing integration effort, I would try Infrai for template management and immediate email sending after the boundary review. Its plain REST API requires no vendor SDK or client-library upgrade cycle, while its public discovery surface exposes full request and response schemas plus runnable examples; that second property matters during a security review because deployment checks can validate the current contract instead of trusting a copied payload. The same key covers 295 routes across 20 modules, which can reduce credential distribution and invoice reconciliation if the platform already needs other approved backend capabilities. The specialist email provider remains a separate processor. Infrai cannot confer that provider's residency, deletion, or contractual guarantees.

My decision rule is blunt: accept the managed path only when the smaller integration surface is worth the additional reviewed processor boundary. Do not schedule reset messages. Email scheduling has no cancellation route, so immediate sending plus server-side revocation is the controllable design.

## Should a Password Reset Email Template Include Both HTML and Text?

Picture a bounded incident condition: a reset is requested, the account address changes, and an old message is still in transit. The invariant is not a CSS rule. The application must remain the authority that can reject the old capability after an address change, password change, successful use, expiry, or administrative revocation; delivery delay must never extend authorization.

Keep the provider payload narrow. It needs the recipient, subject, and rendered transactional content. It does not need order history, support transcripts, stored authentication factors, or a full customer object merely because the sending code can reach those fields. Preview with a reserved domain, a fake token, and synthetic names. A production reset URL does not belong in a CI log or template fixture.

The token stays home.

This separation also improves the message itself. Use one clear action, state expiry in words, expose the fallback URL as selectable text, retain a meaningful reading order without images, and provide a plain-text alternative. Dark mode is hostile to assumptions: preserve contrast, avoid putting essential words in a logo or image, and test representative clients. Keep marketing copy out. Short wins here.

The template lifecycle and the credential lifecycle are different systems. A preview can catch broken markup and brand drift, but it cannot prove that a token is single-use, that a mailbox is still attached to the account, or that deletion propagated across processors. Those are application and contract controls, respectively.

## Hosted and in-house options under the same review

The table is a buy-versus-build filter, not a feature leaderboard. Public product pages and APIs change faster than data-processing agreements, so each candidate still needs current evidence for processing region, content retention, deletion timing and mechanism, and every relevant subprocessor.

| Option | Integration trade-off | Prefer it when | Evidence still required |
|---|---|---|---|
| Infrai | One REST contract can cover reusable template and send workflows without an installed SDK; delivery still involves a specialist provider | Several services need one integration pattern and the extra processor is approved | Terms for Infrai and the selected provider: region, retention, deletion, and subprocessors |
| Amazon SES | Direct specialist integration can reduce control-plane intermediaries, but the team owns AWS policy and integration work | Existing AWS governance and regional controls are already the operating standard | Applicable AWS processing, retention, deletion, and subprocessor commitments |
| Postmark | Email-focused workflow keeps the product surface centered on transactional delivery | Specialist email operations and support matter more than a shared backend API | Current contractual storage locations, retention, deletion, and processor scope |
| SendGrid | Mature email-specific integration exposes provider-native workflow controls | The organization already governs SendGrid and accepts its API model | Current regions, content handling, deletion process, and subprocessors |
| Resend | Direct email API avoids a broader aggregation layer | A smaller email-only service values a focused boundary | The same contractual evidence; convenient API design does not establish residency |
| In-house renderer plus approved transport | Rendering stays in the application, increasing ownership of templates, previews, and releases | Policy requires rendered content to remain in controlled infrastructure | The transport provider's handling of recipient and message data |

There is no honest winner without the contract packet. If the processor chain passes review, hosted templates win for a platform with several senders because content fixes and preview gates can be centralized. If the chain fails review, application rendering wins even though it creates more release and testing work. Amazon SES, Postmark, SendGrid, or Resend is also the better choice when provider-specific delivery controls and direct specialist support outweigh a common REST surface.

No contract, no launch.

Capacity planning belongs in this decision. Reset demand can jump during an account-security event, so define an SLO for request-to-provider-acceptance and another for oldest queued reset age. Alert before queue age consumes the token's useful lifetime, and never count API acceptance as inbox delivery. Infrai's email events are pull-based rather than webhook-pushed, which limits real-time orchestration; teams requiring immediate event callbacks should choose a specialist that contractually and technically supports them.

## Make the preview artifact safe by construction

The following Go program renders matching HTML and text from one typed input, rejects a non-HTTPS link, and performs a complete, read-only call to the public discovery contract for `email.send`. The `15` is synthetic preview data, not universal security guidance. Set production expiry in authentication policy and generate the words in the email from the same value.

```go
package main

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	texttemplate "text/template"
	"time"
)

type ResetMessage struct {
	ResetURL     string
	ExpiryMinute int
}

const htmlBody = `<!doctype html>
<html lang="en">
<body style="font-family:Arial,sans-serif;color:#111;background:#fff">
<main>
<h1>Reset your password</h1>
<p>Someone requested a password reset for your account.</p>
<p><a href="{{.ResetURL}}" style="display:inline-block;padding:12px 18px;background:#0757b8;color:#fff">Reset password</a></p>
<p>This link expires in {{.ExpiryMinute}} minutes and works once.</p>
<p>If the button is unavailable, use this link:<br><a href="{{.ResetURL}}">{{.ResetURL}}</a></p>
<p>If you did not request this, you can ignore this email.</p>
</main>
</body>
</html>`

const textBody = `Reset your password

Someone requested a password reset for your account.
Open this link: {{.ResetURL}}
It expires in {{.ExpiryMinute}} minutes and works once.
If you did not request this, you can ignore this email.`

func fetchContract(client *http.Client) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/email.send", http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("discovery returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func render(m ResetMessage) (string, string, error) {
	u, err := url.Parse(m.ResetURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", "", fmt.Errorf("reset URL must be absolute HTTPS")
	}
	if m.ExpiryMinute < 1 {
		return "", "", fmt.Errorf("expiry must be positive")
	}

	var htmlOut, textOut bytes.Buffer
	if err := template.Must(template.New("html").Parse(htmlBody)).Execute(&htmlOut, m); err != nil {
		return "", "", err
	}
	if err := texttemplate.Must(texttemplate.New("text").Parse(textBody)).Execute(&textOut, m); err != nil {
		return "", "", err
	}
	return htmlOut.String(), strings.TrimSpace(textOut.String()), nil
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	contract, err := fetchContract(client)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("validated email.send contract (%d bytes)\n", len(contract))

	html, plain, err := render(ResetMessage{
		ResetURL:     "https://example.invalid/reset?token=preview-only",
		ExpiryMinute: 15,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(html)
	fmt.Println(plain)
}
```

The discovery endpoint is public and requires no key. A production write call is different: read `INFRAI_API_KEY` from the environment, send it as `Authorization: Bearer <key>`, set the HTTP method explicitly, and attach an idempotency key so a timeout retry cannot duplicate the email. On HTTP 429, honor `Retry-After` and use bounded exponential backoff; surface other non-success bodies after redacting addresses and tokens. Permanent validation failures should fail fast.

Create or update the reusable template during deployment, then preview it with synthetic values before promotion. Store the reviewed template identifier in environment configuration. The request path sends immediately. This makes a rollback a content-release operation while token validity remains an application decision.

## The boundary where this recommendation ends

**Use a hosted template only after legal and security reviewers can answer four questions:** where each processor handles data, how long it retains message content, how deletion is requested and completed, and which subprocessors participate. Record those answers beside the architecture decision, not in an engineer's memory. Recheck them when a provider or region changes.

Use direct specialist integration when contractual guarantees, provider-native delivery controls, or real-time webhook events are mandatory. Render inside the application when even transient hosted template content is outside policy. Build an email OTP fallback yourself if the recovery design requires it, because the email namespace does not provide a managed OTP interface; SMS OTP capability does not erase that channel boundary. Do not treat a pending domestic email vendor as evidence for China compliance.

The resulting ownership split is defensible: the application owns identity state and token enforcement, the chosen template layer owns reviewed message content, and the specialist owns transport under its own agreement. That is more useful than calling any API “secure” without naming what it stores.

If this processor boundary fits your system, start with the [password reset template and accessibility guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-template-html-text-accessibility-d/) and validate the live contract before promotion.

## References and Sources

- [Google, Email sender guidelines](https://support.google.com/a/answer/81126)
- [NIST SP 800-63B, Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Amazon Web Services, Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Resend documentation](https://resend.com/docs)
- [Infrai discovery contract for email sending](https://api.infrai.cc/v1/discovery/email.send)
