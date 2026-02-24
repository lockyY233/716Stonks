from __future__ import annotations

import time
import json

import discord
from discord import ButtonStyle, Interaction, app_commands
from discord.ui import Select

from stockbot.commands.register import REGISTER_REQUIRED_MESSAGE, RegisterNowView
from stockbot.config.runtime import get_app_config
from stockbot.db import count_user_running_timed_jobs, get_state_value, get_user
from stockbot.services.jobs import (
    abandon_running_timed_job_by_id,
    current_tick,
    get_active_jobs,
    get_quiz_choices,
    list_user_running_timed_jobs,
    submit_quiz_choice,
    try_run_chance_job,
    try_start_or_claim_timed_job,
)
from stockbot.services.perks import evaluate_user_perks


def _supports_components_v2() -> bool:
    ui = discord.ui
    return all(
        hasattr(ui, name)
        for name in ("LayoutView", "Container", "TextDisplay", "ActionRow")
    )


def _minutes_until_next_refresh() -> float:
    tick_interval = max(1, int(get_app_config("TICK_INTERVAL")))
    last_tick_epoch_raw = get_state_value("last_tick_epoch")
    try:
        last_tick_epoch = float(last_tick_epoch_raw) if last_tick_epoch_raw is not None else 0.0
    except (TypeError, ValueError):
        last_tick_epoch = 0.0
    if last_tick_epoch <= 0:
        return tick_interval / 60.0
    elapsed = max(0.0, time.time() - last_tick_epoch)
    remaining = max(0.0, float(tick_interval) - elapsed)
    return remaining / 60.0


def _job_summary(row: dict) -> str:
    name = str(row.get("name", "Job"))
    kind = str(row.get("job_type", "chance")).lower()
    desc = str(row.get("description", "") or "No description.")
    reward_min = float(row.get("reward_min", 0.0))
    reward_max = float(row.get("reward_max", reward_min))
    cfg: dict = {}
    raw_cfg = str(row.get("config_json", "") or "").strip()
    if raw_cfg:
        try:
            parsed = json.loads(raw_cfg)
            if isinstance(parsed, dict):
                cfg = parsed
        except (json.JSONDecodeError, TypeError, ValueError):
            cfg = {}
    lines = [
        f"### {name}",
        f"Type: **{kind}**",
        desc,
    ]
    def _estimate_range() -> tuple[float, float]:
        display_min_raw = cfg.get("display_reward_min", None)
        display_max_raw = cfg.get("display_reward_max", None)
        if display_min_raw is not None and display_max_raw is not None:
            try:
                d_min = float(display_min_raw)
                d_max = float(display_max_raw)
                if d_max < d_min:
                    d_min, d_max = d_max, d_min
                return d_min, d_max
            except (TypeError, ValueError):
                pass
        if kind == "timed":
            low = float(cfg.get("timed_reward_min", reward_min) or reward_min)
            high = float(cfg.get("timed_reward_max", reward_max) or reward_max)
            if high < low:
                low, high = high, low
            return low, high
        if kind == "chance":
            outcomes = cfg.get("chance_outcomes", [])
            if isinstance(outcomes, list) and outcomes:
                lows: list[float] = []
                highs: list[float] = []
                for item in outcomes:
                    if not isinstance(item, dict):
                        continue
                    try:
                        lo = float(item.get("reward_min", item.get("reward", 0.0)) or 0.0)
                        hi = float(item.get("reward_max", item.get("reward", lo)) or lo)
                    except (TypeError, ValueError):
                        continue
                    if hi < lo:
                        lo, hi = hi, lo
                    lows.append(lo)
                    highs.append(hi)
                if lows and highs:
                    return min(lows), max(highs)
        if kind == "quiz":
            choices = cfg.get("quiz_choices", [])
            if isinstance(choices, list) and choices:
                lows: list[float] = []
                highs: list[float] = []
                for item in choices:
                    if not isinstance(item, dict):
                        continue
                    try:
                        lo = float(item.get("reward_min", item.get("reward", 0.0)) or 0.0)
                        hi = float(item.get("reward_max", item.get("reward", lo)) or lo)
                    except (TypeError, ValueError):
                        continue
                    if hi < lo:
                        lo, hi = hi, lo
                    lows.append(lo)
                    highs.append(hi)
                if lows and highs:
                    return min(lows), max(highs)
        if reward_max < reward_min:
            return reward_max, reward_min
        return reward_min, reward_max

    est_min, est_max = _estimate_range()
    if kind == "timed":
        duration_minutes = float(row.get("duration_minutes", 0.0) or 0.0)
        if duration_minutes <= 0.0:
            duration_minutes = float(max(1, int(row.get("duration_ticks", 1))))
        lines.append(f"Time to complete: **{duration_minutes:.2f} minute(s)**")
        lines.append(f"Estimated reward range: **${est_min:.2f} - ${est_max:.2f}**")
    else:
        lines.append(f"Estimated reward range: **${est_min:.2f} - ${est_max:.2f}**")
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
        container.add_item(ui.TextDisplay(content=f"Refresh in **{_minutes_until_next_refresh():.2f} minute(s)**."))
        slot_eval = evaluate_user_perks(
            guild_id=self._guild_id,
            user_id=self._user_id,
            base_income=0.0,
            base_trade_limits=0,
            base_networth=0.0,
            base_commodities_limit=int(get_app_config("COMMODITIES_LIMIT")),
            base_job_slots=1,
        )
        max_slots = max(0, int(slot_eval["final"]["job_slots"]))
        running_count = count_user_running_timed_jobs(self._guild_id, self._user_id, current_tick())
        active_timed_jobs = list_user_running_timed_jobs(
            self._guild_id,
            self._user_id,
            limit=50,
        )
        if active_timed_jobs:
            container.add_item(
                ui.TextDisplay(
                    content=(
                        f"### Active Timed Jobs\n"
                        f"Timed job slots: **{running_count}/{max_slots}**."
                    )
                )
            )
            for idx, active in enumerate(active_timed_jobs):
                job_id = int(active.get("job_id", 0))
                job_name = str(active.get("job_name", f"Job #{job_id}"))
                remaining_minutes = max(0.0, float(active.get("remaining_seconds", 0))) / 60.0
                container.add_item(
                    ui.TextDisplay(
                        content=(
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

                async def abandon_job_cb(interaction: Interaction, selected_job_id: int = job_id) -> None:
                    result = abandon_running_timed_job_by_id(
                        self._guild_id,
                        self._user_id,
                        selected_job_id,
                    )
                    await interaction.response.send_message(result.message)
                    self._rows = get_active_jobs(self._guild_id, limit=5)
                    self._build()
                    try:
                        await interaction.message.edit(view=self)
                    except Exception:
                        pass

                abandon_btn.callback = abandon_job_cb
                action_row.add_item(abandon_btn)
                container.add_item(action_row)
                if hasattr(ui, "Separator") and idx < len(active_timed_jobs) - 1:
                    container.add_item(ui.Separator())

            if running_count >= max_slots:
                self.add_item(container)
                return
            if hasattr(ui, "Separator"):
                container.add_item(ui.Separator())
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
        return True

    async def _handle_take(self, interaction: Interaction, row: dict) -> None:
        actor_user_id = interaction.user.id
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
                user_id=actor_user_id,
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
            result = try_start_or_claim_timed_job(self._guild_id, actor_user_id, job_id)
            await interaction.response.send_message(result.message)
            self._rows = get_active_jobs(self._guild_id, limit=5)
            self._build()
            try:
                await interaction.message.edit(view=self)
            except Exception:
                pass
            return
        result = try_run_chance_job(self._guild_id, actor_user_id, job_id)
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
        return True

    async def _on_select(self, interaction: Interaction) -> None:
        value = str(self._select.values[0]) if self._select.values else ""
        result = submit_quiz_choice(
            guild_id=self._guild_id,
            user_id=interaction.user.id,
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
