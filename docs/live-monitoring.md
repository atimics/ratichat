# Live monitoring

The owner can ask RatiChat to watch wallet addresses and keep the channel updated. For example: “Watch these addresses every five minutes and keep me updated.” Include the complete addresses. RatiChat saves the monitor through `create_live_monitor` and confirms its ID and check interval. Ask it to list monitors for check times, coverage and recent transaction links. Ask it to stop a monitor by ID to end its updates.

Saved monitors run between chats. They survive service restarts. Each successful stream starts with a quiet baseline. Later checks post new confirmed transactions. The same transaction appears once when it touches several watched addresses or appears in both transaction and token history. A repeated API failure sends one coverage update after two checks. Recovery sends another update. Unchanged checks stay quiet.

The reader checks Bitcoin transactions through Blockstream, TRON transactions and TRC20 transfers through TronGrid, and EVM transactions and token transfers through Blockscout. Named EVM networks are Ethereum, Base, Arbitrum, Optimism and Polygon. `auto` probes those five networks. Each result names its coverage and provider errors. Each stream reads up to three pages and reports the page limit when reached. Confirmed indexed records can arrive after the underlying chain event.

Transaction movement, theft attribution and loss totals are separate claims. Updates link to the observed transactions. Loss estimates require verified victim outflows and a method that counts each loss once.

Workers receive the full current request, the saved task goal, their selected evidence nodes and up to three live read calls. Each worker has its own fixed chat scope. Model calls and paid web searches share the root task budget. Read results are saved with the child task.

Monitoring uses the feed watch outbox, stable delivery keys and platform receipts. Discord uses a stable nonce and receipt checks. Matrix uses a stable transaction ID. A service restart recovers pending updates and reconciles uncertain sends before checking again. Update posts enter the shared awareness store through the usual message intake.

Configuration:

- `LIVE_MONITOR_DAILY_LOOKUP_BUDGET`: scheduled checks per channel per UTC day; default 7200. Feed watches keep their own budget.
- `ONCHAIN_REQUEST_GAP_SECONDS`: minimum gap between requests to the same explorer host; default one second. HTTP 403 and 429 responses pause that host with backoff. The reader honors numeric `Retry-After` values up to fifteen minutes.
- `TRONGRID_API_KEY`: optional provider key for production quota and access.
- `BLOCKSCOUT_API_KEY`: optional PRO API key. With a key, the reader uses the official multichain API and a bearer header. Public per-instance access depends on the explorer's current policy.

The owner and channel come from the observed request. Monitoring tools use the configured Discord or Matrix channel for updates. A channel supports up to five saved watches and monitors in total. A monitor accepts up to twelve targets and a check interval from one minute to one day.

Provider references: [Esplora API](https://github.com/Blockstream/esplora/blob/master/API.md), [TronGrid](https://developers.tron.network/docs/trongrid), [Blockscout API routes](https://docs.blockscout.com/endpoint-overview).
