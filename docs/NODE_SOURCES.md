# Node processing and public sources

See [Shared awareness and task model routing](SHARED_AWARENESS.md) for the
proposed common store, saved channel views, Jev decisions, and task model choices.

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
`web` plugin to fetch results once for each search. General searches use Exa.
Social searches use Parallel with a filter for the chosen site's domains.
`WEB_SEARCH_MODEL` chooses the search model. The default is `openai/gpt-4o-mini`.
See the [OpenRouter search docs](https://openrouter.ai/docs/guides/features/plugins/web-search).

Pages and feeds are fetched on request. For a GitHub release feed, use
`https://github.com/OWNER/REPO/releases.atom`. For source files, use the public
raw file URL.

## Try it in Discord

Mention RatiChat in a configured channel:

- `@ratichat Search the web for the latest Python release and cite the source.`
- `@ratichat Summarise https://www.python.org/about/`
- `@ratichat Read https://github.com/python/cpython/releases.atom and show the latest three releases.`

The same source readers work in approved Matrix rooms. The app also supports Farcaster
through its existing Neynar connection. X account access can be added as a
separate source adapter when that account is connected.

## Public news and social sources

The agent has `list_public_sources`, `read_news`, `search_social`, and
`read_bluesky_feed`. News readers cover BBC News, BBC Technology, Hacker News,
and CoinDesk. Each story has a source link and publisher credit. Bluesky author
feeds use its public API. Reddit, X, Farcaster, and Bluesky search uses indexed
public pages and posts through the linked web search account. Results include the access
type and source links, so the agent can describe their coverage clearly.

Ask in plain English, such as “What is happening in world news?”, “Find Reddit
discussions about Python”, or “Read the latest posts from bsky.app on Bluesky.”
These readers share the node processor's three-lookup limit and saved source cache.

## Proactive speaking

RatiChat can share a useful public story and invite discussion in a configured
Discord channel. It rotates through tech, AI, developer news, crypto, Reddit,
social discussions, and world news. Its public source node holds the material
for each post. The model chooses one story or waits for better material.

Posts run from 08:00 to 22:00 in `PROACTIVE_TIMEZONE`, with a three-hour gap and
at most four posts per local day. Each channel gets at most eight source checks
and AI decision attempts per day, including at most three public web searches.
The server adds a publisher credit, source link, and receipt. Saved drafts and
delivery receipts survive restarts. An uncertain send gets a receipt check.

Set `PROACTIVE_DISCORD_CHANNEL_IDS` to approved Discord channels and
`PROACTIVE_PUBLIC_SOURCES_ENABLED=true` to seed their first settings. The Fly
deployment enables `#general` with `America/Vancouver` daytime hours. Saved
owner settings carry across deployments.

The agent has `configure_proactive` and `get_proactive_status`. Owners can ask
“Pause proactive speaking”, “Focus on AI and crypto”, or “Keep it to two posts
per day”. People in the channel can ask about its current topics and limits.
The saved sender and channel control these tools. Each settings change and its
receipt share one transaction, and retries restore that receipt before replying.

## Feed watches

Ask RatiChat in plain English to track a public feed, show current watches,
stop a watch, or summarise its latest entries. For example: “Keep an eye on
CPython releases and tell me about new versions each hour.”

The agent can call `create_source_watch`, `list_source_watches`,
`remove_source_watch`, and `get_source_digest`. The current saved request supplies
the sender and channel. Creating and removing watches requires the configured
owner. The agent reads the actual tool result before it confirms a change.
Each change and its tool receipt are saved together, so retries keep the same
result. The first feed check saves a quiet baseline. Later checks post new entries
with titles, short summaries, and links. Digest reads show the latest saved entries.
Each update has a receipt and a fixed destination. Saved updates survive restarts.

Set `DISCORD_OWNER_USER_IDS` to the owner's Discord user ID. Matrix watches use
`MATRIX_OPERATOR_USER_IDS`. Owner IDs and channel IDs come from server settings.
Each channel can have five watches. Intervals range from 15 minutes to 24 hours.
Each channel gets 24 feed checks per UTC day by default. Set
`SOURCE_WATCH_DAILY_LOOKUP_BUDGET` to change this limit. Polling uses public feed
reads; chat research uses the linked AI account as before.

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
