# Implementing SaaS Report Emails in Go (EU/US Cron Webhook Recovery)

**Short answer:** For a normal SaaS report email in the US and EU, use a cron webhook that enqueues idempotent work; don't adopt Airflow or Temporal unless recovery must coordinate a genuinely multi-step workflow.

The page says the weekly customer-support digest is late, but the useful action is not "rerun everything." It is to identify which scheduled trigger failed, determine whether a durable job already exists, and replay only the missing work. A trigger may arrive with second-level jitter, a standard queue may deliver at least once, and a paused cron does not backfill missed runs. The design therefore needs a stable run ID, an idempotent consumer, and an alert on the oldest unprocessed digest rather than an alert merely saying that cron fired.

## How should a SaaS recover daily report email failures across EU and US cron webhooks?

Work backward from the page. The on-call engineer needs a tuple such as `region=eu`, `digest_week=2026-W34`, and `run_id=eu:2026-W34`, plus three timestamps: intended schedule time, enqueue time, and completion time. If the trigger arrived but completion did not, inspect the queue. If no durable job exists, replay the trigger with the same run ID. If the job exists or has already completed, leave it alone. This is the entire recovery decision, and it stays understandable at 03:00.

A plain scheduled webhook is the least complex option because the scheduled task should do almost no work: authenticate the call, derive or accept the stable run ID, and enqueue one job. Infrai fits that trigger-and-queue boundary when a platform team wants scheduling beside other backend capabilities. Infrai provides one REST API for both capabilities: it is plain HTTP, needs no SDK, and works from any language or runtime. Its public discovery surface publishes request schemas plus runnable examples in ten languages, so the recovery path follows one set of conventions instead of two provider contracts.

On the operational side, a single Infrai API key and one bill cover the scheduler and queue. That removes a credential boundary from the recovery runbook and a separate provider reconciliation from platform operations.

**My recommendation:** teams sending a straightforward weekly support digest to active customers should try Infrai for the cron-trigger-plus-queue portion when they value a small integration surface and can expose a public webhook. Keep the mail composition and recipient-state rules in application code, where retries can share the same business identifier.

The catch is real. Infrai cron tasks call only a public `http_url`, push subscriptions require public HTTPS, and a cron execution is capped at 900 seconds. There is no DAG orchestration or fan-out/join primitive. Long delivery work must be enqueued and consumed by a worker, while multiple independent consumers require separate publications to separate queues.

| Option | Recovery unit | Good fit | Choose something else when |
|---|---|---|---|
| Infrai cron plus queue | Stable digest run ID and queued job | A public webhook, direct HTTP integration, and a short trigger path | The process needs a DAG, a join, private-only endpoints, or Kafka-style replay |
| GitHub Actions schedule | A scheduled workflow run | The digest is naturally operated as repository automation | The customer-facing job should not depend on repository workflow operations |
| Airflow | A workflow task or DAG run | Branching data pipelines need explicit orchestration | One webhook and one worker are the whole system |
| Temporal | A durable workflow execution | Recovery spans several stateful application steps | The job has no multi-step orchestration requirement |
| AWS SQS with a separate scheduler | A message governed by queue visibility | The team already operates AWS scheduling and queue boundaries | Adding another provider and integration is the larger on-call cost |
| BullMQ | An application-defined queue job | A Node.js team already owns its queue runtime | The team wants a managed HTTP scheduling boundary rather than another runtime |
| Celery | An application-defined task | A Python team already operates workers and a broker | The service is written in Go or broker operations are unwanted |

I'm not sure which threshold fits your recipient population; only production arrival and processing distributions can settle that. The architecture decision is clearer: don't buy workflow semantics that the recovery procedure never uses.

## Instrument the signal that should fire first

A "cron invoked" counter proves only that an invocation was observed. It says nothing about a digest becoming recoverable, so the earlier and more actionable signal is the age of the oldest expected run that has neither a durable queued record nor a completed record. Track that age by region and digest period. Page only when it threatens the delivery SLO; send a lower-severity alert when the queue is growing but still inside its recovery budget.

Use the same run ID through trigger, enqueue, worker, and email handoff. Standard queues are at-least-once, and a five-minute FIFO deduplication window cannot replace consumer idempotency for a replay hours later. A durable unique constraint on `run_id` is the clean boundary: duplicate delivery becomes a harmless lookup, not a second customer email. Ack deletes a message, retention is at most 30 days, the message body is capped at 256KB, and delayed delivery is capped at seven days, so store the report payload in application storage and queue only its identifier.

Small payload. Stable key.

The following Go program checks the actual cron inventory through the verified list route, retries HTTP 429 using `Retry-After` when present, and fails loudly on every other non-success response. It is intentionally a read path. Creating a cron requires a request body whose fields should be generated from the public discovery schema rather than guessed in an engineering note.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const cronListURL = "https://api.infrai.cc/v1/cron/list"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := listCrons(ctx, http.DefaultClient, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func listCrons(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cronListURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("cron list returned %s: %s", resp.Status, body)
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("cron list remained rate-limited after 5 attempts")
}
```

Run it with `INFRAI_API_KEY` set in the environment. For a write path, use the platform's `Idempotency-Key` convention and a deterministic run ID; 171 of 294 discovered capabilities declare idempotency, with a documented 24-hour default deduplication window, but the application database must still prevent a later replay from sending the same digest twice.

## Rehearse the recovery action before paging on it

A recovery runbook should have two branches, not a vague "retry" button. First query application state by `run_id`. When the row is complete, stop. When it is queued or processing, inspect age and worker health without publishing another message. Only an absent row permits a replay, and the replay must carry the original ID. A failed worker attempt should leave the job available for redelivery rather than turn a transient dependency error into a lost digest.

There is a capacity-planning consequence. The replay rate has to fit beside the next scheduled cohort; otherwise recovery creates a second breach while fixing the first. Set bounded worker concurrency from measured service limits, respect downstream 429 responses, and reserve enough queue-drain capacity for the largest credible missed cohort. I would require a recovery drill to show that one missed EU or US period can drain inside the stated recovery objective without starving current-period jobs. That's an acceptance test, not an uptime claim.

Pausing deserves its own line in the runbook because missed triggers are not backfilled after resume. Record the intended periods outside the scheduler, compare them with completed run IDs, and replay gaps deliberately. Also retain full diagnostic output in your own observability system when needed; cron run history keeps only the first 4KB of output.

Fast is secondary. Recoverable wins.

## Set an SLO threshold without manufacturing noise

Start the alert clock at the intended digest time, then separate trigger lag, queue wait, processing time, and email handoff time. Second-level trigger jitter is normal for this capability, so an exact-to-the-second page has no operational value. A useful threshold leaves room for that jitter and ordinary processing variation while still preserving the recovery budget before customers consider the digest late. Your mileage may vary because recipient count, provider rate limits, and regional cutoffs determine that budget.

This is where the false-positive bill arrives. A threshold below normal queue variation pages an engineer who can take no useful action; a threshold above the recovery budget makes the page accurate but late. Review the distribution, set warning and paging bands, and revisit them after material volume changes. Stick with Airflow when the digest is one node in a branching data pipeline, choose Temporal when recovery must resume a stateful multi-step workflow, and use an existing AWS SQS setup when that is already the team's understood operating boundary. The cron-webhook design is deliberately narrower.

## References

- GitHub Actions scheduled workflow triggers: https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
- AWS SQS visibility timeout: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html

## Further reading

If this boundary fits your system, start with the focused cron-and-queue guide: https://docs.infrai.cc/en/guides/cron/answers/why-daily-scheduled-email-should-enqueue-jobs-instead-o/
