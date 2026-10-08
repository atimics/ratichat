"""Check a Discord bot token, then stage it in Fly using a hidden prompt."""

import argparse
import getpass
import json
import shutil
import subprocess
import sys
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", default="ratichat-bot-prod")
    parser.add_argument("--guild-id", required=True)
    parser.add_argument("--channel-id", required=True)
    args = parser.parse_args()
    if not sys.stdin.isatty():
        raise SystemExit("Run this script in an interactive terminal for the hidden token prompt.")
    if not args.guild_id.isdecimal() or not args.channel_id.isdecimal():
        raise SystemExit("Use numeric Discord server and channel IDs.")
    fly = shutil.which("fly") or shutil.which("flyctl")
    if not fly:
        raise SystemExit("Install Fly's CLI, then run fly auth login.")
    token = getpass.getpass("Paste the Discord bot token, then press Enter: ").strip()
    if not token or any(character.isspace() for character in token):
        raise SystemExit("Copy the bot token as one line, then try again.")
    request = urllib.request.Request(
        "https://discord.com/api/v10/users/@me",
        headers={"Authorization": "Bot " + token, "User-Agent": "RatiChat-Setup/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            bot = json.load(response)
    except (urllib.error.URLError, ValueError):
        raise SystemExit("Discord could not verify this token. Check the token and try again.") from None
    if bot.get("bot") is not True:
        raise SystemExit("Use the token from the application's Bot page.")
    print("Discord verified bot:", bot.get("username"), "(" + str(bot.get("id")) + ")")
    result = subprocess.run(
        [fly, "secrets", "import", "--app", args.app, "--stage"],
        input=(
            "DISCORD_BOT_TOKEN=" + token + "\n"
            "DISCORD_ALLOWED_GUILD_IDS=" + args.guild_id + "\n"
            "DISCORD_ALLOWED_CHANNEL_IDS=" + args.channel_id + "\n"
        ),
        text=True, capture_output=True,
    )
    if result.returncode:
        raise SystemExit("Fly could not save the secrets. Run fly auth login, then try again.")
    print("Discord token and channel settings saved on Fly. The next deploy will use them.")


if __name__ == "__main__":
    main()
