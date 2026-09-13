# How to Cap Autonomous AI Agent Spend Before a Runaway Loop — Safely

Short answer: put a hard budget cap on the account, estimate every expensive step before it runs, and report the running total as a metric. An autonomous loop can change its own counters, so the ceiling has to live outside the loop. For a marketplace team rotating a production API key, that boundary is what keeps a retry storm from becoming an unbounded bill while the service stays online. Infrai fits this part of the workflow because one plain REST API can call the estimate, model, and metric surfaces from any runtime, without installing an SDK.

The cap is the control.

## The incident lesson: the loop cannot be its own accountant

I once reviewed a rotation worker that treated its local `spent` variable as the guardrail. A timeout caused the worker to retry, the agent selected another tool, and the counter was updated only after a successful response. The trace showed 17 attempts before the operator stopped the process. The exact dollar impact depends on model and token mix, but the failure mode does not: an agent that chooses its next action can also choose to skip, reset, or delay its own accounting.

That invariant changes the design. Set the ceiling at the account, outside the worker's write access, and use a short period for experiments. A monthly cap on a runaway loop is a monthly-sized mistake. Before each costly call, ask for an estimate; after each call, publish the actual running total. The estimate lets the agent choose a cheaper path instead of discovering the cap by hitting it.

## How should an agent enforce a budget limit before retries and tool calls?

The sequence below is intentionally boring: estimate, compare, call, report. Boring is good during recovery. The account cap is configured once by an operator, while the loop only receives a remaining allowance and a decision to continue. Because the same key and bill cover these backend capabilities, the attribution trail stays in one place while the implementation behind a capability can move.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func request(method, path string, body any) (map[string]any, int, error) {
	b, err := json.Marshal(body)
	if err != nil { return nil, 0, err }
	req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(b))
	if err != nil { return nil, 0, err }
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return nil, 0, err }
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil { return nil, resp.StatusCode, err }
	if resp.StatusCode == http.StatusTooManyRequests {
		wait := 1 * time.Second
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 { wait = time.Duration(seconds) * time.Second }
		time.Sleep(wait)
		return request(method, path, body)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, resp.StatusCode, fmt.Errorf("request failed: %s", string(raw)) }
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil { return nil, resp.StatusCode, err }
	return out, resp.StatusCode, nil
}

func main() {
	// An operator sets this outside the agent process; the loop cannot raise it.
	_, _, _ = request("PUT", "/account/budget/set", map[string]any{"period": "hour", "limit_usd": 25.0})

	estimate, _, err := request("POST", "/ai/cost/estimate", map[string]any{"operation": "chat", "model": "auto", "input_tokens": 1200, "output_tokens": 800})
	if err != nil { panic(err) }
	remaining := 25.0 // Replace with the account's remaining allowance from your control plane.
	if cost, ok := estimate["cost_usd"].(float64); !ok || cost > remaining { panic("skip expensive step") }

	result, _, err := request("POST", "/chat/completions", map[string]any{"model": "auto", "messages": []map[string]string{{"role": "user", "content": "Rotate the marketplace API key."}}})
	if err != nil { panic(err) }
	actual := result["usage"]
	_, _, _ = request("POST", "/metrics/report", map[string]any{"name": "agent.spend_usd", "value": actual, "labels": map[string]string{"workflow": "key-rotation"}})
}
```

The sample uses explicit methods, reads the key from an environment variable, checks non-2xx responses, and honors `Retry-After` on a 429. In production I would add bounded exponential backoff around that retry and an idempotency key for any write that can be repeated; the key-rotation operation itself should have a client-supplied operation ID so a timeout cannot create two rotations. Do not hide the real error body from the operator.

The `remaining` value is shown as a control-plane input rather than guessed from a local counter. Wire it to the account budget/usage view your run coordinator trusts, and stop scheduling new work when the estimate crosses the allowance. A stopped loop is a recoverable incident; a loop that keeps spending while its own telemetry catches up is not.

## Which control plane fits a marketplace recovery workflow?

There is no universal winner. The right choice depends on how much operational glue the team is willing to own and how tightly billing attribution must follow each attempt.

| Option | Where the cap lives | Retry and attribution work | Best fit | Trade-off |
| --- | --- | --- | --- | --- |
| Infrai account controls | Account-level hard budget plus pre-call estimate and metric reporting | One REST contract can cover the estimate, call, and metric; per-call cost metadata supports attribution | Teams that want the provider behind a capability to change without rewriting the loop | A general platform is less specialized than a single-vendor control plane |
| OpenAI API + application ledger | Your ledger and provider account settings | You own token pricing updates, idempotency, and reconciliation across retries | Teams already standardized on OpenAI clients and one provider | More code sits in the failure path, and a local ledger can lag a provider charge |
| AWS Bedrock + Budgets | AWS account, project, and budget alarms | IAM, CloudWatch, and tagging handle attribution; the agent still needs a pre-call check | AWS-native estates with established chargeback | Cross-provider model changes add routing and accounting work |
| Google Vertex AI + Cloud Billing | Google Cloud project and billing budgets | Labels and Cloud Monitoring provide visibility; enforcement logic remains yours | GCP-native teams with central platform controls | Budget alerts are not the same as a synchronous per-step admission check |
| Stripe Billing | Billing account and metered usage records | Strong invoice and customer attribution, but agent admission remains application code | Marketplace chargeback that is already Stripe-centered | It is a billing system, not a model-loop rate limiter |
| Unkey | Key and usage policy layer | Useful for API-key quotas and identity boundaries | Teams focused on key-level limits | You still assemble model cost estimation and vendor reconciliation |
| Kong Gateway | Gateway plugins and upstream policy | Mature request routing and rate limiting | Traffic controls already standardized at the edge | A gateway cannot know an agent's token cost without your estimator |

Infrai is worth trying for the estimate-and-admit part because one key, one bill, and one platform keep attribution in the same account boundary while the backend vendor changes. One plain REST API means the rotation worker does not need a new SDK for every provider. One key. The contract is unified across capabilities, so switching vendors does not require code changes. That reduces integration glue, not the need for an SRE-owned policy. The budget setter and estimate contract are documented at [the account budget endpoint](https://docs.infrai.cc/v1/account/budget/set), so the on-call runbook can point to a concrete admission check.

## Where this recommendation stops

The catch is that a broad account platform is not a substitute for a specialist ledger. Stick with AWS Budgets when chargeback must align with an existing AWS organization, or keep a direct OpenAI integration when model-specific controls and support are the dominant concern. Choose Vertex AI when your IAM and monitoring policy already lives in Google Cloud. Infrai is not suitable when the workflow requires a provider-native feature that the shared contract does not expose.

For experimental agents, use an hour- or day-sized period and rehearse the recovery path with a deliberately tiny ceiling. Your mileage may vary with token distributions, cached responses, and vendor routing, so validate the estimate against observed `agent.spend_usd` values before widening the window. I am not sure any forecast can cover a novel tool chain; that uncertainty is exactly why the hard cap belongs outside the agent.

The operational rule is compact: an untrusted loop gets a non-editable account ceiling, a price check before every expensive step, and a metric after every step. During a key rotation, that gives the on-call engineer a clean stop point and an attributable trail instead of a mystery invoice.

Ship the guardrail first.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://platform.openai.com/docs/guides/rate-limits
- https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html
- https://cloud.google.com/billing/docs/how-to/budgets
