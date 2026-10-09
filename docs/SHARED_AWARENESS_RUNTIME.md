# Shared awareness runtime

RatiChat uses one SQLite awareness store on the bot's `/data` volume. Approved Discord channels and public Matrix rooms belong to the same community. Each sender has a saved view per conversation. Expanded nodes hold details; collapsed nodes hold summaries. The current request supplies the reply destination.

A task saves its goal, topic, persona, exact model route, input versions, attempts, worker results, and spending. Jev chooses relevant nodes, a topic, a persona, and a model route from the current OpenRouter catalog. Haiku 5.5 and GPT-6 Luna are the fast defaults. GPT-6.1 Sol is available for complex work. The `get_model_catalog` tool lets the planner find other worker models by exact catalog ID.

The agent can use `run_task_workers` for up to three child jobs. Workers share the root task's budget and read the selected evidence. A saved worker result can be reused after a restart. A task has a $0.10 budget by default. Calls reserve their full input and output bound before dispatch. Provider usage records settle known costs. Uncertain outcomes use the reserved bound. Public search usage is recorded when the provider returns it.

Conversation snapshots use source event versions. New chat messages preserve completed tasks. Edits and deletions invalidate results based on the changed evidence. Confirmed replies enter shared awareness after delivery. Saved delivery receipts support recovery after a restart.

## Use in chat

Ask RatiChat to research a topic in Discord. In rati.chat, ask it to continue that topic. Jev can select the shared saved task. Each surface keeps its own node view and reply destination.

Ask for two or three views of a topic to use worker personas such as researcher, developer, and critic. Ask for task status to read saved progress. Ask to link an account to start a proof: present the code from the exact target account, then confirm from the original account. Proof codes expire after ten minutes.

## Settings

- `SHARED_AWARENESS_ENABLED=true` enables the approved community store.
- `TASK_MODEL_ROUTING_ENABLED=true` enables saved Jev routes and task tools.
- `TASK_BUDGET_USD=0.10` sets the root task budget.
- `DISCORD_MESSAGE_CONTENT_ENABLED=true` requests ordinary message text from Discord. The bot application must have the Message Content setting enabled. See [Discord application settings](https://docs.discord.com/developers/resources/application#edit-current-application).

Proactive posts use the same saved awareness and model routes. Their existing topic, daytime, spacing, and daily limits still apply.

## Validation

The suite covers cross-platform continuation after a SQLite reopen, separate views, approved audiences, source edits and deletions, account proofs, child budgets, worker replay, local model choices during parallel calls, typed Jev decisions, provider failures, and proactive shared context.
