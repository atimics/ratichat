# Matrix and Discord admin tools

RatiChat can inspect a community and apply an admin change from chat. Ask for
one change per message. Give the exact member, role, or message ID and a reason.
The reply reports the tool result and includes a receipt for each new admin
change. Receipts stay in the bot database across restarts. A repeated request
returns its saved result. An uncertain result asks the owner to check the target
before sending a fresh request.

## Matrix

Set `BOT_CAPABILITY_PROFILE=matrix_steward`. Configure these settings:

| Setting | Value |
| --- | --- |
| `MATRIX_CONTROL_ROOM_ID` | Room where operators ask for admin work |
| `MATRIX_OPERATOR_USER_IDS` | Operator Matrix IDs, separated by commas |
| `MATRIX_MANAGED_ROOM_IDS` | Community rooms the bot manages |
| `PUBLIC_MATRIX_ROOM_IDS` | Approved bot rooms, including the control room |

Use an ordinary text message in the control room. The bot checks the source
message through Matrix before running a new room admin tool. Its chat account
needs the room power levels for the requested action. Matrix checks those power
levels on every write.

| Tool | Actions |
| --- | --- |
| `inspect_matrix_room` | Room settings, member counts, or a page of members |
| `moderate_matrix_member` | Invite, kick, ban, or unban one member |
| `manage_matrix_message` | Pin, unpin, or redact one message |
| `manage_matrix_room` | Existing room name, topic, and directory visibility controls |
| `manage_matrix_server` | Existing uptime, memory, backup, and backup-list commands |

Room admin tools use the bot's Matrix session. The existing directory and server
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

The admin switch works with each supported capability profile. A configured
owner must mention the bot in a configured text channel. RatiChat fetches the
source message, server, channel, requester, and bot member before an action.
Both the requester and bot need the matching Discord permission. Member and role
changes also check their role order. The bot, server owner, requester, and
configured bot owners are protected from member changes.

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
must sit below the bot's highest role and the requester's highest role. Discord's
server owner uses owner authority. Editable roles must fit within the requester's
permissions. A ban preserves existing messages. Timeouts accept 1 second through
28 days. Slowmode accepts 0 through 21,600 seconds; 0 clears slowmode.

Example requests:

- “@RatiChat show this channel's settings and your permissions.”
- “@RatiChat set slowmode here to 30 seconds. Reason: busy event.”
- “@RatiChat timeout user `USER_ID` for 10 minutes. Reason: repeated spam.”
- “@RatiChat add role `ROLE_ID` to user `USER_ID`. Reason: event helper.”
- “@RatiChat pin message `MESSAGE_ID` here. Reason: event schedule.”

Member, role, and channel listings accept `offset` and `limit`. Use the returned
`next_offset` to request another page. Admin tools report one result per request;
use a fresh request to apply a change after an inspection.

The implementation follows the [Matrix client API](https://spec.matrix.org/latest/client-server-api/)
and the [discord.py API](https://discordpy.readthedocs.io/en/stable/api.html).
