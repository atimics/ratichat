# Node processing and public sources

RatiChat uses the node processor for chat requests. Each request opens its own
channel node. Source nodes start as short descriptions. RatiChat can expand,
collapse, pin, and unpin the nodes listed for that request.

When a request needs fresh information, RatiChat calls a source tool. The result
becomes a source node. The next AI step reads it and prepares a reply with links.
A turn has at most five AI steps and three source lookups.
The last step writes an answer for the current source event. A temporary AI
failure gets up to three attempts. The last failed attempt sends a short service
message.
Each web search has one search call and at most three results. Page reads stop at 512 KB and return
at most 12,000 text characters. Feeds return up to ten entries.

## Sources

| Tool | Use | Account |
| --- | --- | --- |
| `web_search` | Current facts, news, project research, public docs | Linked OpenRouter account |
| `read_webpage` | Public pages, docs, raw GitHub files, public JSON | Public URL |
| `read_feed` | RSS and Atom news, blogs, GitHub releases | Public feed URL |

Web search uses the owner's linked OpenRouter credits. It uses OpenRouter's
`openrouter:web_search` server tool with Exa. `WEB_SEARCH_MODEL` chooses the
search model. The default is `openai/gpt-4o-mini`. See the
[OpenRouter search docs](https://openrouter.ai/docs/guides/features/server-tools/web-search).

Pages and feeds are fetched on request. For a GitHub release feed, use
`https://github.com/OWNER/REPO/releases.atom`. For source files, use the public
raw file URL.

## Try it in Discord

Mention RatiChat in a configured channel:

- `@ratichat Search the web for the latest Python release and cite the source.`
- `@ratichat Summarise https://www.python.org/about/`
- `@ratichat Read https://github.com/python/cpython/releases.atom and show the latest three releases.`

The same tools work in approved Matrix rooms. The app also supports Farcaster
through its existing Neynar connection. X account access can be added as a
separate source adapter when that account is connected.

## Request scope

Each request uses its own channel messages and its own fetched source results.
The source sender and event IDs come from the chat observer. Public source text
is marked as untrusted evidence. Tool permissions still apply at execution.
Matrix management keeps its operator and control room checks and sends the
actual result as a receipt.

URL readers use public HTTP or HTTPS addresses on standard ports. The connection
checks the resolved addresses and each redirect. Text size and time limits apply.
Requests are saved in SQLite as they arrive. Each channel processes them in order.
The saved request keeps its sender, source event, and nearby messages. A prepared
answer keeps the same reply target through retries and restarts. Delivery receipts
confirm the actual sent answer. A send with an uncertain receipt waits for a
platform check.

The `channel.memory` node holds recent confirmed answers and source records from
the current channel. Source results have fetch and expiry times. A fresh saved
result can be reused for five minutes. The `fresh` tool option requests a new fetch.
Turn content is kept for seven days by default. Event IDs remain as receipt records
after content expires. `RESEARCH_RETENTION_DAYS` and `RESEARCH_SOURCE_TTL_SECONDS`
control these limits. A fresh mention starts a new request.
Idle polling preserves the cycle budget for new chat activity.

## Operator view

`GET /api/system/status` reports the active processor and the latest turn's
lookup and step counts, plus saved turn counts under `research`.
`GET /api/worldstate` reports node state.
`GET /api/worldstate/ai-payload` shows the most recent AI node payload.
These routes use the existing admin authentication.

The system command `force_processing_mode` accepts `node-based` or `traditional`.
The traditional processor remains available through that explicit command or
when node processing is disabled before a turn starts.
