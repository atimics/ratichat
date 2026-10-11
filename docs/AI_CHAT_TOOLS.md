# AI tools for Matrix and Discord

RATi's AI can inspect and manage its community through its tool loop. It reads
live room or channel state, chooses an action, reads the result, checks the
changed state, and explains what happened. The participation model can start
this work from an observed conversation, including a message without a bot
mention.

## AI control

```dotenv
AI_CHAT_CONTROL_ENABLED=true
AI_CHAT_MODERATION_ENABLED=false
DISCORD_ADMIN_ENABLED=true
```

`AI_CHAT_CONTROL_ENABLED` lets the AI choose room and channel changes in the
configured community. It uses the bot's own Matrix power levels and Discord
permissions. The message author supplies context. The configured AI role
supplies authority.

With `AI_CHAT_MODERATION_ENABLED=false`, room names, topics, slowmode, pins,
inspection, and the existing Matrix server tools are available to the AI.
Member changes, role changes, and message removal use explicit owner requests.
Set `AI_CHAT_MODERATION_ENABLED=true` to let the AI choose those actions too.
Discord role assignment still uses the configured managed role IDs.

The Fly config enables AI control with moderation through owner requests.
The AI receives its mode, current source event, current channel, and managed
Matrix room IDs in `community_control`. Tool results return to the model before
its next decision. Each observed event allows one change. Its receipt stays in
the database across restarts, so a repeated event returns the saved result.
An uncertain result asks RATi to check the target before another change.

`AI_CHAT_CONTROL_ENABLED=false` selects the owner request flow. Operators use
the Matrix control room; Discord owners mention the bot in a configured channel.

## Matrix

Set `BOT_CAPABILITY_PROFILE=matrix_steward`. Configure these settings:

| Setting | Value |
| --- | --- |
| `MATRIX_CONTROL_ROOM_ID` | Room where operators ask for admin work |
| `MATRIX_OPERATOR_USER_IDS` | Operator Matrix IDs, separated by commas |
| `MATRIX_MANAGED_ROOM_IDS` | Community rooms the bot manages |
| `PUBLIC_MATRIX_ROOM_IDS` | Approved bot rooms, including the control room |

The AI observes ordinary text messages in approved rooms. In AI mode it can
manage the current public room. The control room supports work across configured
managed rooms. The bot checks the live source event for new room controls.
Its chat account needs the room power levels for each action. Matrix checks
those power levels on every write.

| Tool | Actions |
| --- | --- |
| `inspect_matrix_room` | Room settings, member counts, or a page of members |
| `moderate_matrix_member` | Invite, kick, ban, or unban one member |
| `manage_matrix_message` | Pin, unpin, or redact one message |
| `manage_matrix_room` | Existing room name, topic, and directory visibility controls |
| `manage_matrix_server` | Existing uptime, memory, backup, and backup-list commands |

Room tools use the bot's Matrix session. The existing directory and server
tools use the dedicated management session described in
[Matrix management](../deploy/fly/bot/MANAGEMENT.md). The control and server admin
rooms remain reserved for their setup roles. Member removal protects the bot
and configured operators. Pin changes preserve the other saved pins.

Example requests:

- “Show the members of `!community:example.com`, with 10 members per page.”
- “Invite `@alex:example.com` to `!community:example.com`. Reason: new moderator.”
- “Ban `@spam:example.com` from `!community:example.com`. Reason: repeated spam.”
- “Pin message `$event` in `!community:example.com`. Reason: community rules.”

## Discord

Complete [Discord setup](DISCORD_SETUP.md), then set:

```dotenv
DISCORD_ADMIN_ENABLED=true
DISCORD_OWNER_USER_IDS=your-discord-user-id
DISCORD_MANAGED_ROLE_IDS=community-role-id,another-community-role-id
```

AI control works in configured Discord text channels and servers. RATi fetches
the source message, server, channel, and bot member before an action. In AI mode,
the bot's role and permissions govern its actions. In the owner request flow,
the bot also checks the requester's permissions. Member and role changes check
role order. The bot, server owner, and configured bot owners are protected
from member changes.

| Tool | Actions | Permission |
| --- | --- | --- |
| `inspect_discord_server` | Channel settings, configured channels, roles, or one member | View Channel |
| `manage_discord_channel` | Current channel name, topic, or slowmode | Manage Channels |
| `moderate_discord_member` | Timeout or clear timeout | Moderate Members |
| `moderate_discord_member` | Kick | Kick Members |
| `moderate_discord_member` | Ban or unban | Ban Members |
| `manage_discord_role` | Add or remove a configured role | Manage Roles |
| `manage_discord_message` | Pin or unpin in the current channel | Pin Messages |
| `manage_discord_message` | Delete one message in the current channel | Manage Messages |

Role changes use `DISCORD_MANAGED_ROLE_IDS`. Both the selected role and member
must sit below the bot's highest role. In the owner request flow, they must also
sit below the requester's highest role. Discord's server owner uses owner
authority. Editable roles must fit within the acting account's permissions. A ban preserves existing messages. Timeouts accept 1 second through
28 days. Slowmode accepts 0 through 21,600 seconds; 0 clears slowmode.

Example requests:

- “@RatiChat show this channel's settings and your permissions.”
- “@RatiChat set slowmode here to 30 seconds. Reason: busy event.”
- “@RatiChat timeout user `USER_ID` for 10 minutes. Reason: repeated spam.”
- “@RatiChat add role `ROLE_ID` to user `USER_ID`. Reason: event helper.”
- “@RatiChat pin message `MESSAGE_ID` here. Reason: event schedule.”

Member, role, and channel listings accept `offset` and `limit`. Use the returned
`next_offset` to request another page. In AI mode, the model can inspect and apply one change within the same turn,
then check the live result before replying.

The implementation follows the [Matrix client API](https://spec.matrix.org/latest/client-server-api/)
and the [discord.py API](https://discordpy.readthedocs.io/en/stable/api.html).
