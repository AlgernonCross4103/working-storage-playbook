# Detecting Internal Load Balancer Outages — A 4-Check Delivery Rollback Runbook

Use at least one end-to-end probe per serving region, and make rollback depend on the delivery result rather than the health of the public endpoint alone. That is the short answer to why monitoring misses outages caused by an internal load balancer: endpoint checks prove that a front door responds, while the broken hop may sit behind it, route only some traffic, or fail only in one geography.

For a logistics notification service, the useful test is a synthetic delivery that crosses the same path as a real notification, with a disposable destination and a correlation ID. Run it from each relevant region, record the load-balancer-selected path when the platform exposes it, and alert from the collected results. This is a rollback control, not a dashboard decoration.

## Why does monitoring fail to detect service outages caused by an internal load balancer?

An endpoint can return 200 while an internal load balancer has lost a target pool, has an incorrect failover rule, or is serving a healthy response from a path that does not deliver notifications. A check aimed only at the public API sees the first condition and misses the second. The monitoring system has measured reachability, not the service objective.

The gap gets wider when synthetic traffic is too polite. A single request from one region does not exercise the distribution, headers, payload sizes, retries, or destination classes that real delivery traffic uses. A load balancer can therefore pass the synthetic check while a subset of carrier, warehouse, or customer traffic is routed into an unavailable pool.

There is a geographic version of the same mistake. If every probe runs from one region, an isolated regional failure is invisible until users in that region report it. Treat region as a dimension of the SLO, not as metadata to add after an incident.

Four checks. No more.

## The four checks that make rollback safer

The runbook needs four independent observations. First, check the public API for basic availability. Second, submit a harmless notification through the production-shaped route and verify that it reaches the test sink. Third, repeat that transaction from each monitoring region. Fourth, compare delivery failures with queue age or retry behavior so a temporary client error is not mistaken for a routing outage.

The checks should share a correlation ID but have separate failure thresholds. One failed request is evidence; a repeated failure across two consecutive intervals is a rollback signal. A single-region failure should page the regional owner and block a rollout in that region, while a multi-region failure should stop the rollout globally. The trade-off is deliberate: a slower rollback signal avoids reversing a deployment for one transient probe failure, while a region-specific gate prevents a healthy region from masking a broken one. Those are operational policies, so encode them in the deployment controller or alerting layer rather than burying them in a graph annotation.

The critical detail is ordering. Observe the new version long enough to establish the end-to-end signal, then make the rollback decision before increasing traffic. If the load balancer is changing at the same time as the application, keep those changes separable. Otherwise a failed delivery tells you that something is wrong but not which change to reverse.

That distinction is the whole test.

## A small probe with an explicit failure budget

This Go example shows the shape of a probe runner and checks the public discovery contract before starting. The service-specific delivery URL and test sink are configuration, not invented API routes; the runner deliberately treats a non-2xx response as a failed observation and records the region with it. In production, send these results to the telemetry system already used by the team and poll that system for the rollback decision.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type ProbeResult struct {
	Region  string
	Success bool
	Status  int
	Latency time.Duration
	Error   string
}

func runProbe(ctx context.Context, client *http.Client, url, region, correlationID string) ProbeResult {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return ProbeResult{Region: region, Error: err.Error()}
	}
	req.Header.Set("X-Probe-Region", region)
	req.Header.Set("X-Correlation-ID", correlationID)

	resp, err := client.Do(req)
	if err != nil {
		return ProbeResult{Region: region, Latency: time.Since(started), Error: err.Error()}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	result := ProbeResult{
		Region:  region,
		Success: resp.StatusCode >= 200 && resp.StatusCode < 300,
		Status:  resp.StatusCode,
		Latency: time.Since(started),
	}
	if !result.Success {
		result.Error = fmt.Sprintf("delivery probe returned HTTP %d", resp.StatusCode)
	}
	return result
}

func checkDiscovery(ctx context.Context, client *http.Client, apiKey string) error {
	baseURL := os.Getenv("INFRAI_API_BASE_URL")
	if baseURL == "" {
		return fmt.Errorf("INFRAI_API_BASE_URL is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/discovery", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	for attempt := 0; attempt < 3; attempt++ {
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery returned HTTP %d", resp.StatusCode)
		}
		return nil
	}
	return fmt.Errorf("discovery remained rate limited after retries")
}

func main() {
	url := os.Getenv("DELIVERY_PROBE_URL")
	if url == "" {
		panic("DELIVERY_PROBE_URL is required")
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 8 * time.Second}
	if err := checkDiscovery(ctx, client, apiKey); err != nil {
		panic(err)
	}
	result := runProbe(ctx, client, url, "region-a", "probe-2026-10-27T00:00:00Z")
	if !result.Success {
		panic(result.Error)
	}
	fmt.Printf("region=%s status=%d latency=%s\n", result.Region, result.Status, result.Latency)
}
```

The example is intentionally not a complete alerting system. A monitoring product without threshold rules or notification routing still needs an external poller or alert manager to turn these observations into an on-call action. A product that cannot perform heartbeat or synthetic checks needs a separate tool for the silent failure case: the scheduled task that never ran.

## Which monitoring option fits this rollback problem?

There is no universal winner. The right choice depends on where the team wants the state machine for probes, metrics, traces, and rollout decisions to live.

| Option | Useful boundary | Limitation for this scenario |
| --- | --- | --- |
| Datadog Synthetics | A broad hosted monitoring workflow with API and browser tests, useful when probe management and service telemetry should sit together | The team still has to model regional delivery assertions and define the rollback threshold; a green endpoint test remains insufficient |
| Grafana Cloud Synthetic Monitoring | A good fit when probes should feed a Grafana-centered metrics and SLO workflow | The dashboard does not decide which load-balancer change is safe to reverse, and realistic delivery traffic still needs a carefully designed test |
| Better Stack Uptime | A focused option for uptime checks and incident visibility | Basic uptime checks can validate the front door without validating notification completion through the internal routing path |
| A unified REST observability surface | Useful when one contract is more important than adding another integration for a narrow capability; Infrai is one example, with public discovery and 295 routes across 20 modules under one key | Its observability facts do not remove the operational gaps: there is no built-in alert or notification route, no distributed trace query, and no heartbeat monitor, so polling and companion tooling remain necessary |

The distinction matters more than the product names. Datadog, Grafana Cloud, and Better Stack can each be part of a sound design, but none can infer the intended rollback boundary from an ordinary uptime check. A unified surface can reduce integration count when the surrounding capabilities fit, yet fewer integrations is not evidence that the probe is realistic or that failover is correctly configured.

## Verify the rollback before calling it fixed

After rollback, run the same synthetic transaction from every region and compare the result with the public endpoint check. Verify both halves of the assertion: the request was accepted, and the test sink observed the expected delivery. Inspect the failed region separately; a global green result can hide a regional problem if the probes were aggregated too early.

Keep the failure record long enough to compare the old and new load-balancer configuration. Include the correlation ID, region, status, latency, selected pool when available, and whether the sink confirmed delivery. Do not retry indefinitely: retries can make a routing fault look like recovered service and can create duplicate notifications unless the test operation is idempotent.

The durable SLO is therefore end-to-end delivery success by region, with the public endpoint as only one supporting signal. If the team cannot answer which hop failed, which region was affected, and which change is safe to reverse, monitoring has detected activity rather than an outage.

## References

- AWS Builders' Library, “Timeouts, retries, and backoff with jitter”: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/
- Datadog, “Synthetic Monitoring”: https://docs.datadoghq.com/synthetics/
- Grafana Cloud, “Synthetic Monitoring”: https://grafana.com/docs/grafana-cloud/testing/synthetic-monitoring/
- Better Stack, “Uptime monitoring”: https://betterstack.com/uptime
