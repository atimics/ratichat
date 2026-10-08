# Discord setup

RatiChat replies when a person mentions its Discord bot in a configured text
channel. Each request uses that channel's messages. The bot uses Discord's
Gateway through `discord.py` for event delivery, reconnects, and rate limits.

## Bot and channel

1. Open the [Discord Developer Portal](https://discord.com/developers/applications).
2. Select your application and open **Bot**. Copy the bot token. Complete
   **Reset Token** yourself when a new token is needed.
3. Under **Installation**, select **Guild Install**, add the `bot` scope, and
   select **View Channels**, **Send Messages**, and **Read Message History**.
4. Use the install link to add the bot to your server. Give it access to the
   chosen text channel.
5. In Discord, enable **User Settings → Advanced → Developer Mode**. Copy the
   server ID and text channel ID from their context menus.

Bot mentions have message content available with the basic guild message
intent. This setup uses those mentions as requests. See Discord's
[message content intent reference](https://docs.discord.com/developers/events/gateway#message-content-intent).

## Save the token on Fly

Run `fly auth login`, then run the helper from the repository:

```sh
python3 scripts/save_discord_token.py --guild-id SERVER_ID --channel-id CHANNEL_ID
```

The hidden prompt accepts the token. The helper checks the bot identity with
Discord, then sends the token and channel settings to Fly over standard input.
Fly stages the secrets for the next deployment.

The runtime settings are:

```dotenv
DISCORD_BOT_TOKEN=your-bot-token
DISCORD_ALLOWED_GUILD_IDS=your-server-id
DISCORD_ALLOWED_CHANNEL_IDS=your-text-channel-id
DISCORD_MESSAGE_RATE_LIMIT_PER_MINUTE=10
DISCORD_MAX_MESSAGE_CHARS=4000
```

The two ID settings also accept comma-separated lists. Every allowed channel
must belong to an allowed server. Configure `OPENROUTER_API_KEY` or finish the
existing OpenRouter link flow so RatiChat can generate replies.

## Deploy and check

Deploy the reviewed code using **Deploy RatiChat bot to Fly** from `main`.
Check the logs for `Discord connected`. In the configured channel, send
`@ratichat hello` using Discord's bot mention picker. Check that the bot posts
one reply to that message. Send a fresh mention after a bot restart.

The public and Matrix steward profiles support Discord replies. Discord
requests use the Discord reply tool and the current channel's messages. Bot
and webhook messages are skipped. Replies use the source message as their
reference and keep mentions quiet.
