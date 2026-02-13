import os
import secrets
import time

from discord import Interaction, app_commands

from stockbot.db import set_state_value


WEBADMIN_TOKEN_TTL_SECONDS = 15 * 60


def setup_webadmin(tree: app_commands.CommandTree) -> None:
    @tree.command(name="webadmin", description="Admin: generate one-time dashboard access link.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def webadmin(interaction: Interaction) -> None:
        token = secrets.token_urlsafe(24)
        expires_at = int(time.time()) + WEBADMIN_TOKEN_TTL_SECONDS
        set_state_value(f"webadmin:token:{token}", str(expires_at))

        base_url = os.getenv("WEBADMIN_BASE_URL", "https://716stonks.cfm1.uk").rstrip("/")
        link = f"{base_url}/?token={token}"
        minutes = WEBADMIN_TOKEN_TTL_SECONDS // 60
        await interaction.response.send_message(
            f"One-time web admin link (expires in {minutes} minutes):\n{link}",
            ephemeral=True,
        )
