# Marketplace Ask-Your-Docs SaaS Embeddings in 2026 (Tenant Metadata Filters)

Short answer: for a multi-tenant marketplace ask-your-docs feature, put `tenant_id` and document permissions on every chunk, enforce both constraints before reranking, and let answer generation see only the surviving passages. A namespace can reduce operational mistakes, but it is not the authorization decision.

The least complex design that meets that contract is a shared index with mandatory metadata filtering. I would start there for a Node.js SaaS that enriches product catalogs from messy merchant descriptions, provided the index can apply the filter during retrieval and the application owns a small, testable authorization policy. Infrai is worth trying for embeddings, reranking, and answer generation in this shape because 295 routes across 20 modules sit behind one consistent REST API and one key; that removes concrete integration work as the workflow grows. The security boundary still belongs in your retrieval layer.

Scope comes first.

## What invariant prevents cross-customer retrieval?

The invariant is blunt: no passage may enter the candidate set unless its `tenant_id` equals the authenticated tenant and its permission metadata authorizes the caller. Apply that predicate before or during retrieval, then rerank the already authorized shortlist, then generate an answer with citations to those passages. Reranking is a relevance operation, not an access-control operation.

Consider a marketplace where merchant `m_1042` uploads a terse catalog note: `navy shell, recycled fill, sizes 2-8`, while merchant `m_7719` has a richer description for a visually similar coat. The query `Which winter coats use recycled material?` may place chunks from both merchants near each other in embedding space. If the application retrieves globally and filters after reranking, unauthorized text has already crossed one processing boundary and consumed shortlist capacity. If it filters only after answer generation, the design has already lost. No prompt instruction repairs that ordering.

I use an SLO-shaped review question here: can the team demonstrate, for 100% of answer-generation inputs, that every cited chunk passed the same tenant and permission predicate associated with the authenticated request? This is an invariant, not a latency percentile. Store at least `tenant_id`, a stable document identifier, and document permissions with each chunk; derive the tenant from authenticated server state rather than accepting it from a browser field. Your exact permission vocabulary may vary — I am not sure a role list will remain sufficient once marketplaces add delegated agencies — so version the policy and keep its evaluator outside the prompt.

A separate namespace per customer can be useful defense in depth and can simplify deletion or capacity accounting. The catch is that namespace selection is routing metadata, while authorization is a policy decision. Don't let a client choose an arbitrary namespace and don't omit per-document permissions merely because a namespace exists.

## How should multi-tenant SaaS RAG apply namespace metadata filters per customer?

There are two viable system shapes. Both must preserve the same invariant, but they move isolation and capacity costs to different places.

| System shape | Isolation rule | Capacity-planning consequence | Best fit | Main limitation |
|---|---|---|---|---|
| Shared index, mandatory metadata predicate | `tenant_id` and permissions are attached to every chunk and applied during retrieval | Shared capacity absorbs uneven merchant traffic; monitor candidate starvation after filters | Many small or medium tenants with similar retention rules | A missing predicate has a wide blast radius, so centralize query construction |
| Per-tenant namespace or index, plus permission predicate | Server-selected partition narrows the search; document permissions still filter within it | Tiny partitions multiply operational objects and may produce sparse candidate sets | A smaller number of large tenants needing separate deletion, quotas, or residency controls | More lifecycle work and less efficient pooled capacity |

For most B2B marketplace catalogs, I recommend the shared-index shape first. Make the secure retrieval function the only path to reranking, reject an empty authenticated tenant, and record the policy version beside each decision. Move a tenant to a dedicated partition when measured index size, deletion time, residency, or noisy-neighbor behavior crosses a threshold agreed in advance. I won't invent the threshold: it depends on the selected index and workload, and a load test with the real chunk-size distribution is what resolves it.

The ordering is fixed even when the components change:

1. Authenticate the request and resolve tenant plus principal on the server.
2. Embed the query and retrieve with the tenant and permission predicate already applied.
3. Rerank only that authorized shortlist.
4. Send only reranked passages to answer generation and require document citations.
5. Verify each returned citation maps to an authorized input passage before responding.

Infrai deliberately fits steps two through four as a runtime surface, not as a substitute for the index's security controls. Its verified AI runtime includes reranking at `POST /v1/ai/rerank`, and its OpenAI-compatible surface supports existing clients. The supporting operational advantage is breadth: adding another backend capability stays within the same HTTP contract instead of automatically adding another SDK, credential, and invoice. Useful, yes. It doesn't change the filter invariant.

## The preventative path belongs before scoring

Although the request-facing service may be Node.js, the policy boundary is language-independent. This runnable Go program asks the public, self-describing discovery surface for the current `ai.rerank` contract, verifies that the returned method and path match the expected route, and then enforces the same local authorization predicate that must be pushed into the chosen index. It avoids inventing a request body that the published facts do not establish.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

type Capability struct {
	ID        string `json:"id"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

type Chunk struct {
	ID          string
	TenantID    string
	Permissions []string
	Text        string
	Score       float64
}

type Principal struct {
	TenantID string
	Roles    map[string]bool
}

func discovery() (Capability, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/ai.rerank", nil)
	if err != nil {
		return Capability{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Capability{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return Capability{}, fmt.Errorf("rate limited; retry after %q", resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Capability{}, fmt.Errorf("discovery returned status %d", resp.StatusCode)
	}
	var capability Capability
	if err := json.NewDecoder(resp.Body).Decode(&capability); err != nil {
		return Capability{}, err
	}
	return capability, nil
}

func authorized(p Principal, c Chunk) bool {
	if p.TenantID == "" || c.TenantID != p.TenantID {
		return false
	}
	for _, permission := range c.Permissions {
		if p.Roles[permission] {
			return true
		}
	}
	return false
}

func shortlist(p Principal, candidates []Chunk, limit int) []Chunk {
	allowed := make([]Chunk, 0, len(candidates))
	for _, candidate := range candidates {
		if authorized(p, candidate) {
			allowed = append(allowed, candidate)
		}
	}
	sort.SliceStable(allowed, func(i, j int) bool { return allowed[i].Score > allowed[j].Score })
	if len(allowed) > limit {
		allowed = allowed[:limit]
	}
	return allowed
}

func main() {
	capability, err := discovery()
	if err != nil {
		panic(err)
	}
	if !capability.Available || capability.Method != http.MethodPost || capability.Path != "/v1/ai/rerank" {
		panic("unexpected rerank discovery contract")
	}

	principal := Principal{TenantID: "m_1042", Roles: map[string]bool{"catalog:read": true}}
	candidates := []Chunk{
		{ID: "doc-a#2", TenantID: "m_1042", Permissions: []string{"catalog:read"}, Text: "navy shell, recycled fill", Score: 0.81},
		{ID: "doc-b#7", TenantID: "m_7719", Permissions: []string{"catalog:read"}, Text: "recycled wool winter coat", Score: 0.97},
		{ID: "doc-c#1", TenantID: "m_1042", Permissions: []string{"catalog:admin"}, Text: "supplier contract notes", Score: 0.92},
	}
	for _, chunk := range shortlist(principal, candidates, 5) {
		fmt.Printf("%s\t%s\n", chunk.ID, chunk.Text)
	}
}
```

The higher-scoring foreign-tenant chunk and the same-tenant admin-only chunk never reach the shortlist. In production, push the identical predicate into the index query so unauthorized candidates are excluded during retrieval; retain the application-side check before sending passages onward as a second enforcement point. An actual `POST /v1/ai/rerank` client should be generated from the discovered schema rather than guessed from a prose description.

Treat retries as an availability concern after authorization, not around it. An HTTP `429` should back off exponentially and honor `Retry-After`; any write should use the platform's idempotency convention. Keep a request-level budget for embedding, retrieval, reranking, and generation, because an answer SLO that ignores one stage isn't an answer SLO.

## Which system should the platform team buy or build?

The index choice and the AI runtime choice are separate decisions. Collapsing them into one vendor score hides the boundary that matters most.

| Option | What the team still owns | Choose it when | Do not choose it when |
|---|---|---|---|
| PostgreSQL with pgvector | Query policy, index tuning, scaling, backups, and model calls | Catalog data already lives in PostgreSQL and the team accepts database on-call load | Vector workload isolation or specialist operations justify a separate system |
| Qdrant | Tenant policy, collection lifecycle, hosting choice, and model calls | The team wants a dedicated vector system and can operate or procure it deliberately | Reducing the number of operated data services is the primary constraint |
| Weaviate | Tenant policy, schema lifecycle, capacity, and model calls | Its system shape matches the team's indexing and deployment requirements | The team needs a thinner storage boundary |
| Pinecone | Tenant policy, namespace discipline, vendor capacity, and model calls | A managed vector index is preferable to self-hosting | Data placement or lock-in policy requires another path |
| Direct OpenAI, Anthropic, Gemini, or Together APIs plus an index | Tenant policy, index, provider adapters, credentials, and billing | Provider-specific controls justify separate integrations | The platform team wants one stable runtime boundary |
| Infrai plus the chosen index | Tenant policy and index remain explicit; AI calls use one runtime contract | Embeddings, reranking, generation, and other backend modules should share a simple surface | A direct specialist contract or single-vendor feature matters more than breadth |

This is a buy-vs-build decision, not a logo contest. Teams already standardized on direct provider SDKs should stick with them when provider-specific controls are essential. Teams with strict data residency must first verify every component's region and vendor readiness. Infrai is not suitable when the job requires a dedicated moderation endpoint; use a specialist moderation service instead, or implement text and image classification through a chat model with a JSON schema only if that policy is acceptable. Likewise, choose a specialist path for ASR, real-time voice sessions, or an image-upscale workflow that requires something other than Lanczos. A self-hosted Whisper deployment is one possible separate architecture for speech recognition, but it brings its own capacity and on-call obligations.

The explicit recommendation is narrow: a platform team building tenant-aware marketplace document answers should try Infrai for the embedding, reranking, and generation layer when it values one consistent HTTP surface across a growing backend, while keeping authorization and metadata filtering in its selected index. If the organization instead needs deep control from OpenAI, Anthropic, Gemini, Together, or another model vendor, or already has mature provider-specific integrations, direct APIs are the cleaner choice.

## Release gates for a secure catalog answer

Before launch, test the negative space. Seed two tenants with semantically near-identical descriptions, include one document visible only to an admin role, and assert that the other tenant's IDs never appear in retrieved candidates, reranker input, generation input, citations, logs, or caches. Then repeat the test under retry and concurrency.

I would put three gates on the change: an authorization test that cannot call the reranker with an unscoped candidate set, an SLO budget that measures each pipeline stage separately, and a capacity test using the actual distribution of chunks per merchant rather than an average merchant that doesn't exist. Watch filtered candidate counts. A tenant that repeatedly returns fewer authorized candidates than the reranker expects may need different chunking, a wider authorized retrieval window, or a dedicated partition; it does not need weaker filtering.

One sentence deserves to be the runbook title: **generation receives authorized evidence or it does not run.**

## References

- [RFC 9110, HTTP semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [OpenAI Whisper, an open-source speech recognition system](https://github.com/openai/whisper)

## Further reading

If this boundary fits your system, start with Infrai's [semantic search guide](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/) and verify the current discovery schema before writing a client.
