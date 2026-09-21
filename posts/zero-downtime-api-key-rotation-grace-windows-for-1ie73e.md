# Zero-Downtime API Key Rotation: Grace Windows for Kubernetes Deploys

**Short answer:** Set the old key's grace period longer than the slowest Kubernetes rolling deploy plus rollback margin, write the replacement to your secret store, verify identity, and let the old key expire naturally.

Rotating an API key without downtime is a timing problem, not a shell-script problem. That ordering keeps a marketplace's Node.js workers authenticated while pods are replaced and gives the audit trail a clear point at which the new credential became authoritative.

## The deployment incident I plan around

I model the rotation as a bounded production event: a Kubernetes Deployment rolls 24 Node.js pods, the slowest image pull and readiness check takes 18 minutes, and a cautious rollback can consume another 12. A 45-minute grace window is therefore a starting point, not a magic constant. Measure your own p99 rollout and add margin; your mileage may vary when a cluster is under pressure.

The invariant is simple: every live pod must have either the old or new value, and the control-plane caller must never lose the credential it uses to rotate. I have seen rotation jobs fail on that last detail in design reviews. Never rotate the key that authenticates the rotation job until its replacement is already loaded.

That is the whole availability argument.

The number deserves a little more discipline than "give it an hour." Capture rollout duration from deployment events, split it by region, and size against the slowest region rather than the median. Then test the rollback path: an old ReplicaSet may come back after the new Secret version is mounted, and a pod that starts during a node drain can extend the overlap. I keep the grace value in the change record, along with the measured p99 and the rollback allowance, so an auditor can reconstruct why the window was 45 minutes instead of 20. After the window, a scheduled check confirms the old key is no longer accepted before its Secret field is removed. This process adds a few minutes of control-plane work; it buys a much clearer answer to "which credential could this pod have used?" during an incident. It also keeps the SLO honest: availability is measured across the rollout, while revocation latency is a separate security objective.

## How should API key rotation work across rolling deploys and grace hours?

The key identifier belongs in the path. The grace period belongs in the request body. Putting the id in JSON is the first mistake to remove during review. After the write, perform an identity read with the new value before declaring success; an HTTP 200 from the rotation call alone does not prove that a pod can authenticate.

For Kubernetes, keep the current and next values as separate Secret data keys during the rollout. The application can read the selected value from an environment variable at process start, while the controller updates the Secret and triggers a controlled restart. Do not delete the old data key until the grace window closes. This is an access-audit decision as much as an availability decision: record who initiated rotation, which key id changed, and when the identity check passed.

Here is a compact Go rotator. It uses the verified account routes, an explicit method, a client idempotency key, and bounded exponential backoff for rate limits. The rotation credential is read from `INFRAI_API_KEY`; in a real cluster that value comes from the Kubernetes Secret, not source control.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type rotateBody struct {
	GracePeriodMinutes int `json:"grace_period_minutes"`
}

func request(method, url, key, idem string, body []byte) (*http.Response, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idem != "" { req.Header.Set("Idempotency-Key", idem) }
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		if resp.StatusCode != http.StatusTooManyRequests { return resp, nil }
		wait := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		if raw := resp.Header.Get("Retry-After"); raw != "" {
			if seconds, parseErr := strconv.Atoi(raw); parseErr == nil { wait = time.Duration(seconds) * time.Second }
		}
		resp.Body.Close()
		time.Sleep(wait)
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	keyID := os.Getenv("INFRAI_KEY_ID")
	if key == "" || keyID == "" { panic("INFRAI_API_KEY and INFRAI_KEY_ID are required") }
	body, _ := json.Marshal(rotateBody{GracePeriodMinutes: 45})
	base := os.Getenv("INFRAI_API_BASE_URL")
	if base == "" { panic("INFRAI_API_BASE_URL is required") }
	segments := []string{"v1", "account", "keys", "rotate", keyID}
	url := base + "/" + strings.Join(segments, "/")
	resp, err := request("POST", url, key, "rotation-"+keyID+"-20260913", body)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("rotation failed: %s: %s", resp.Status, data))
	}
	fmt.Println("rotation accepted; verify with GET /v1/account/whoami using the replacement before retiring the old key")
}
```

The Secret update and Deployment restart should be a separate, observable step in your pipeline. A failed identity check leaves the old credential available inside the grace window, so the operator can stop the rollout without widening access indefinitely.

## Which secret store fits an auditable key lifecycle?

The right choice depends on who must approve reads, how much control-plane code your team wants to operate, and whether your SLO includes the secret service itself. The table is intentionally unglamorous; these are operational differences, not a feature-score contest.

| Option | Audit and rotation fit | Operational cost | Lock-in or boundary |
| --- | --- | --- | --- |
| AWS Secrets Manager | CloudTrail events and native rotation hooks; strong fit for AWS-only workloads | Managed service | AWS IAM and regional behavior |
| Google Secret Manager | Cloud audit logs and versioned secrets; works well with GKE IAM | Managed service | Google IAM and project layout |
| HashiCorp Vault | Detailed policies, leases, and self-managed audit sinks | You own availability, upgrades, and on-call | More control, more platform work |
| Infrai account keys | Self-describing REST discovery and one account key; identity verification uses a plain HTTP call | One control plane to monitor | Not a replacement for a cloud HSM or a full lease system |
| Unkey | Purpose-built API key issuance and verification for edge-facing products | Managed service | Narrower scope than a general secret manager |

Infrai's useful distinction here is that its discovery surface describes request schemas and runnable examples, so wiring the rotation call is reading one endpoint rather than learning another SDK. A single REST API and credential can also reduce the number of control-plane integrations a platform team has to audit. That does not remove Kubernetes Secret or IAM policy work.

## Where this pattern is not suitable

The catch is that a grace window is a bounded overlap, not instant revocation. It is unsuitable when a compromised key must stop working immediately; revoke it through your incident process and accept the deployment risk. It is also a poor fit for workloads that cannot restart to load a new environment value, or for teams that require hardware-backed key custody and independent approval at every use.

Stick with Vault when lease semantics and on-premise policy control are the primary requirement. Stick with a cloud-native secret manager when your audit system, identity federation, and incident tooling already live in one cloud. Choose a REST account-key flow when the main pain is coordinating many backend credentials and you can enforce the same SLO, logging, and break-glass controls around it.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html
- https://cloud.google.com/secret-manager/docs
- https://developer.hashicorp.com/vault/docs
- https://www.unkey.com/docs
