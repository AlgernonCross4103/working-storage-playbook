# Private SaaS Export Throughput: Signed Object Downloads Across US and EU

Short answer: for an e-commerce SaaS that serves generated reports, choose private object storage behind an authenticated application, issue a short-lived signed download URL only after the export permission check, and select the service by sustained large-file throughput, regional placement, recovery controls, and operational load rather than by API compatibility alone.

The URL is not the security boundary. It is a temporary bearer credential issued after the security boundary has already done its work. That distinction is the first line in the runbook because it prevents a common design error: letting a storage listing, a predictable object key, or a client-side filename decide who may download a report.

## How should a SaaS export service size private signed downloads for US and EU files?

Start with the byte path, not the SDK. For each export, the application should record the tenant, requester or audience, export state, object key, selected region, byte size, creation time, and retention deadline. The object remains private. A download request authenticates the customer, checks that database record, and asks the storage layer for a signed URL only when the export is ready and the requester is entitled to it.

For large reports, throughput is a capacity problem with two separate SLOs. The control-plane SLO covers authentication, authorization, and URL issuance. The data-plane SLO covers the customer's ability to receive the bytes before the link expires. Combining them hides the failure mode: an application can issue links quickly while a congested path, a small egress budget, or an unsuitable region makes the actual download miss its target.

Capacity planning needs at least four inputs: export bytes per day, peak concurrent downloads, retention days, and regeneration cost. Add the largest expected object and the slowest supported client network to the test matrix. A ten-minute URL may be generous for a small CSV and too short for a multi-gigabyte report. Your mileage may vary; measure completion time at the percentiles that matter to the customer instead of choosing an expiry by habit.

Keep the object key opaque and tenant-scoped. The database owns the mapping from an export ID to an object; storage listing is an operational tool, not an authorization database. A user who changes an export ID must receive a denial, not a link for another tenant's object.

Keep it private.

## Where do signed links fail in production?

The first failure is usually an authorization race. If a handler accepts an object key from the browser and signs it before consulting the export record, a private bucket has not protected the application from confused-deputy behavior. Check tenant, user or role, state, region, and object identity before signing. Do not put the URL in ordinary request logs, analytics events, or exception payloads.

The second failure is an expiry mismatch. URL lifetime must cover authorization, queue delay if the export is handed off, and the expected transfer window. Expiry and deletion are different controls: deleting an object removes the content, while expiry limits a bearer credential. A lifecycle policy can remove old exports, but it does not replace a permission check and it does not tell you whether a customer can finish downloading a large file.

The third failure is filename handling. The application should treat the display filename as metadata, not as a path. When the response supplies a download filename, use the HTTP `Content-Disposition` response header and test both ordinary ASCII names and names requiring `filename*`; the header's grammar and browser behavior are part of the contract, not cosmetic polish.

The fourth is a misleading regional test. A US and an EU deployment are not validated by checking only that both can create an object. Test where the bytes are stored, where the application issues the link, how the customer reaches the object, and what the recovery procedure does after a region or provider change. Document data residency and retention as explicit acceptance criteria.

The fifth is choosing the wrong serving path. A proxy through the application can simplify authorization and response headers, but it also puts report bytes, connection concurrency, and retry behavior on the application fleet. A direct signed URL moves the byte path away from that fleet, but it makes expiry, metadata, observability, and regional routing explicit responsibilities. That trade-off is why a storage decision belongs in capacity planning, not only in dependency review. I would trace one export from the job queue to the database row, from the row to the private object, and from the authenticated request to the final byte before comparing services. At each transition, ask which component can deny access, which component can retry, and which component owns the customer-visible SLO. If a customer sees a `403`, the application should know whether it denied an expired authorization decision or the storage request was never made; if the file starts and then slows, the data-plane measurement must distinguish object retrieval from the customer's network. Those distinctions matter during an incident because a single aggregate download metric tells the on-call engineer almost nothing about where the time went. A direct URL is a good fit for a large-file workload only after these ownership boundaries are observable.

Measure it.

| Serving approach | Integration shape | Fits when | Main limitation |
| --- | --- | --- | --- |
| Direct signed object download | Storage adapter plus authenticated application check | Large files and high concurrent byte transfer are the main concern | Expiry, metadata, and download observability need deliberate design |
| Application-proxied download | Application reads the private object and streams it to the customer | Centralized policy and response transformation matter more than fleet egress | Application capacity becomes part of the large-file throughput budget |
| Self-hosted object service | Operated storage cluster behind the same private-object contract | The team owns placement, hardware, and recovery operations | On-call load and recovery engineering move in-house |

## What should the implementation boundary look like?

The application owns authorization and export state; the storage adapter owns byte operations and signing. Keep that boundary narrow so a provider change does not spread through handlers, jobs, and customer-facing code. The following Go example shows the decision point without assuming a particular storage SDK or service.

```go
package main

import (
	"errors"
	"fmt"
)

type Export struct {
	TenantID  string
	OwnerID   string
	ObjectKey string
	Region    string
	Ready     bool
}

type Signer interface {
	SignDownload(objectKey, region string, lifetimeSeconds int) (string, error)
}

func issueDownloadURL(signer Signer, requesterTenant, requesterID string, export Export) (string, error) {
	if !export.Ready || export.TenantID != requesterTenant || export.OwnerID != requesterID {
		return "", errors.New("export is not authorized")
	}
	if export.ObjectKey == "" || export.Region == "" {
		return "", errors.New("export metadata is incomplete")
	}

	// The signed URL is created only after the application check succeeds.
	return signer.SignDownload(export.ObjectKey, export.Region, 900)
}

func main() {
	_ = fmt.Println
}
```

The `900` seconds in this example is a policy placeholder, not a universal answer. Make it configurable, cap it, and record the policy version with the issuance event without recording the credential itself. The download response can contain the URL because the customer is already authenticated to the application; the storage API key must never reach that customer.

For the adapter contract, test private upload, private retrieval, signing, expiry, deletion, region selection, and response metadata. Return provider errors to an internal metric with a correlation ID, but expose a stable application error to the client. Retry only operations that are safe to retry, use bounded backoff, and ensure a retry cannot create duplicate export records or silently sign an object belonging to a different tenant.

## How do you verify throughput, recovery, and rollback?

The release gate should exercise the complete customer path with realistic object sizes and concurrency. The pass criteria should be written before the test starts:

- Tenant A cannot obtain Tenant B's export, even when the export IDs are guessed.
- An unready export cannot produce a link, and an expired link cannot retrieve the object.
- Large downloads meet the data-plane SLO in both required regions at the selected concurrency.
- The response preserves the intended filename through `Content-Disposition`.
- A fresh link can be issued for an existing private object without regenerating the report.
- Storage growth, failed issuance, expired-link attempts, and download completion are observable by tenant and region.

Run the test against the largest supported report, not the median fixture. Include a slow consumer, a cancelled download, a repeated request, a revoked application permission, and a key collision attempt. Watch p95 and p99 completion time, link-issuance latency, bytes served, retry counts, and the age distribution of retained exports. I would also compare the forecasted peak byte rate with the measured test rate before calling the design ready.

Rollback needs a switch that stops new link issuance while leaving completed private objects intact. The runbook must name the owner of that switch, state how already-issued links are handled, pause new export jobs if necessary, and preserve enough database state to reconcile objects later. After a regional change, verify one known export, one denied export, one expired URL, and one rebuilt link before reopening issuance.

The catch is that this architecture is not suitable when the requirement is immutable retention, automatic cross-region replication, or a storage-native write-once policy that the selected service does not provide. In that case, choose a service and adapter whose documented controls meet that requirement, even if the integration is less portable. Stick with a simpler private-object design when the product does not need direct customer downloads or when reports are small enough for the application to proxy them within its SLO; adding signed links then adds another operational boundary without solving the real constraint.

I am not sure any storage choice is production-ready until its recovery drill has an owner and an observed result. The drill should cover an export in each required region, a revoked permission, an expired credential, an accidental overwrite, and a rebuild from the database record. Throughput gets the headline because customers feel it immediately, but recovery and authorization determine whether the headline remains true after the first awkward incident.

## References

The HTTP filename behavior is documented by MDN's Content-Disposition reference. Storage cost and transfer assumptions should be checked against the current Backblaze B2 pricing page during capacity planning; prices are not a substitute for a throughput or recovery test.

## Further reading

- [MDN: Content-Disposition](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition)
- [Backblaze B2 pricing](https://www.backblaze.com/cloud-storage/pricing)
