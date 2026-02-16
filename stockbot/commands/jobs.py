from __future__ import annotations

import time
import json

import discord
from discord import ButtonStyle, Interaction, app_commands
from discord.ui import Select

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.db import get_user, get_user_running_timed_job
from stockbot.services.jobs import (
    abandon_running_timed_job,
    current_tick,
    get_active_jobs,
    get_quiz_choices,
    submit_quiz_choice,
    try_run_chance_job,
    try_start_or_claim_timed_job,
)


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(
        hasattr(ui, name)
        for name in ("LayoutView", "Container", "TextDisplay", "ActionRow")
    )


def _job_summary(row: dict) -> str:
    name = str(row.get("name", "Job"))
    kind = str(row.get("job_type", "chance")).lower()
    desc = str(row.get("description", "") or "No description.")
    reward_min = float(row.get("reward_min", 0.0))
    reward_max = float(row.get("reward_max", reward_min))
    lines = [
        f"### {name}",
        f"Type: **{kind}**",
        desc,
    ]
    if kind == "timed":
        duration_minutes = float(row.get("duration_minutes", 0.0) or 0.0)
        if duration_minutes <= 0.0:
            duration_minutes = float(max(1, int(row.get("duration_ticks", 1))))
        est_min = float(row.get("reward_min", 0.0) or 0.0)
        est_max = float(row.get("reward_max", est_min) or est_min)
        raw_cfg = str(row.get("config_json", "") or "").strip()
        if raw_cfg:
            try:
                cfg = json.loads(raw_cfg)
                if isinstance(cfg, dict):
                    est_min = float(cfg.get("timed_reward_min", est_min) or est_min)
                    est_max = float(cfg.get("timed_reward_max", est_max) or est_max)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        if est_max < est_min:
            est_min, est_max = est_max, est_min
        lines.append(f"Time to complete: **{duration_minutes:.2f} minute(s)**")
        lines.append(f"Estimated payout: **${est_min:.2f} - ${est_max:.2f}**")
    else:
        lines.append(f"Reward: **${reward_min:.2f} - ${reward_max:.2f}**")
    if kind == "quiz":
        question = str(row.get("quiz_question", "")).strip() or "No question configured."
        lines.append(f"Question: **{question}**")
    return "\n".join(lines)


class JobsV2View(discord.ui.LayoutView):
    def __init__(self, guild_id: int, user_id: int, rows: list[dict]) -> None:
        super().__init__(timeout=300)
        self._guild_id = guild_id
        self._user_id = user_id
        self._rows = rows[:5]
        self._build()

    def _build(self) -> None:
        self.clear_items()
        ui = discord.ui
        container = ui.Container()
        container.add_item(ui.TextDisplay(content="## Job Board (Current Rotation)"))
        lock_row = get_user_running_timed_job(self._guild_id, self._user_id, current_tick())
        if lock_row is not None:
            remaining_seconds = max(
                0.0,
                float(lock_row.get("running_until_epoch", 0.0) or 0.0) - time.time(),
            )
            remaining_minutes = remaining_seconds / 60.0
            job_name = str(lock_row.get("job_name", f"Job #{lock_row.get('job_id', '?')}"))
            container.add_item(
                ui.TextDisplay(
                    content=(
                        f"### Active Timed Job\n"
                        f"**{job_name}**\n"
                        f"Time remaining: **{remaining_minutes:.2f} minute(s)**."
                    )
                )
            )
            action_row = ui.ActionRow()
            abandon_btn = discord.ui.Button(
                label="Abandon",
                style=ButtonStyle.danger,
            )

            async def abandon_cb(interaction: Interaction) -> None:
                result = abandon_running_timed_job(self._guild_id, self._user_id)
                await interaction.response.send_message(result.message)
                self._rows = get_active_jobs(self._guild_id, limit=5)
                self._build()
                try:
                    await interaction.message.edit(view=self)
                except Exception:
                    pass

            abandon_btn.callback = abandon_cb
            action_row.add_item(abandon_btn)
            container.add_item(action_row)
            self.add_item(container)
            return
        if hasattr(ui, "Separator"):
            container.add_item(ui.Separator())
        if not self._rows:
            container.add_item(ui.TextDisplay(content="_No available jobs right now._"))
            self.add_item(container)
            return

        for idx, row in enumerate(self._rows):
            container.add_item(ui.TextDisplay(content=_job_summary(row)))
            action_row = ui.ActionRow()
            available = bool(row.get("available_this_tick", True))
            take_btn = discord.ui.Button(
                label="Take Job" if available else "Unavailable",
                style=ButtonStyle.green if available else ButtonStyle.secondary,
                disabled=not available,
            )

            async def take_cb(interaction: Interaction, selected=row) -> None:
                await self._handle_take(interaction, selected)

            take_btn.callback = take_cb
            action_row.add_item(take_btn)
            container.add_item(action_row)
            if hasattr(ui, "Separator") and idx < len(self._rows) - 1:
                container.add_item(ui.Separator())
        self.add_item(container)

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._user_id:
            await interaction.response.send_message(
                "Only the command user can use this panel.",
            )
            return False
        return True

    async def _handle_take(self, interaction: Interaction, row: dict) -> None:
        job_id = int(row.get("id", 0))
        active_ids = {int(j.get("id", 0)) for j in get_active_jobs(self._guild_id, limit=5)}
        if job_id not in active_ids:
            await interaction.response.send_message(
                "This job rotated out of the current board.",
            )
            return
        kind = str(row.get("job_type", "chance")).lower()
        if kind == "quiz":
            choices = get_quiz_choices(self._guild_id, job_id)
            if not choices:
                await interaction.response.send_message(
                    "Quiz choices are not configured for this job.",
                )
                return
            view = QuizChoiceView(
                guild_id=self._guild_id,
                user_id=self._user_id,
                job_id=job_id,
                job_name=str(row.get("name", "Quiz Job")),
                choices=choices,
                panel_view=self,
                panel_message=interaction.message,
            )
            prompt = str(row.get("quiz_question", "") or "").strip() or "Choose an option:"
            await interaction.response.send_message(prompt, view=view)
            return
        if kind == "timed":
            result = try_start_or_claim_timed_job(self._guild_id, self._user_id, job_id)
            await interaction.response.send_message(result.message)
            self._rows = get_active_jobs(self._guild_id, limit=5)
            self._build()
            try:
                await interaction.message.edit(view=self)
            except Exception:
                pass
            return
        result = try_run_chance_job(self._guild_id, self._user_id, job_id)
        await interaction.response.send_message(result.message)
        self._rows = get_active_jobs(self._guild_id, limit=5)
        self._build()
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass


class QuizChoiceView(discord.ui.View):
    def __init__(
        self,
        *,
        guild_id: int,
        user_id: int,
        job_id: int,
        job_name: str,
        choices: list[dict],
        panel_view: JobsV2View | None = None,
        panel_message: discord.Message | None = None,
    ) -> None:
        super().__init__(timeout=120)
        self._guild_id = guild_id
        self._user_id = user_id
        self._job_id = job_id
        self._job_name = job_name
        self._choices = choices
        self._panel_view = panel_view
        self._panel_message = panel_message
        options = [
            discord.SelectOption(
                label=str(c.get("label", "Option"))[:100],
                value=str(c.get("value", "")),
                description=None,
            )
            for c in choices
        ]
        self._select = Select(
            placeholder=f"Select answer for {job_name}",
            min_values=1,
            max_values=1,
            options=options,
        )
        self._select.callback = self._on_select
        self.add_item(self._select)

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._user_id:
            await interaction.response.send_message(
                "Only the command user can use this panel.",
            )
            return False
        return True

    async def _on_select(self, interaction: Interaction) -> None:
        value = str(self._select.values[0]) if self._select.values else ""
        result = submit_quiz_choice(
            guild_id=self._guild_id,
            user_id=self._user_id,
            job_id=self._job_id,
            choice_value=value,
        )
        await interaction.response.send_message(result.message)
        if self._panel_view is not None and self._panel_message is not None:
            self._panel_view._rows = get_active_jobs(self._guild_id, limit=5)
            self._panel_view._build()
            try:
                await self._panel_message.edit(view=self._panel_view)
            except Exception:
                pass


def setup_jobs(tree: app_commands.CommandTree) -> None:
    @tree.command(name="jobs", description="Open jobs board and work for extra income.")
    async def jobs(interaction: Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Please use this command in a server.")
            return
        user = get_user(interaction.guild.id, interaction.user.id)
        if user is None:
            await interaction.response.send_message(
                REGISTER_REQUIRED_MESSAGE,
                view=RegisterNowView(),
            )
            return
        rows = get_active_jobs(interaction.guild.id, limit=5)
        if not rows:
            await interaction.response.send_message("No jobs are available right now.")
            return
        if not _supports_components_v2():
            await interaction.response.send_message(
                "This runtime does not support Components v2 for `/jobs` yet.",
            )
            return
        view = JobsV2View(interaction.guild.id, interaction.user.id, rows)
        await interaction.response.send_message(view=view)
