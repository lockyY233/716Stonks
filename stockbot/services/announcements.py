import discord
from discord import Embed
from typing import Any, cast


def supports_components_v2() -> bool:
    ui = discord.ui
    return all(hasattr(ui, name) for name in ("LayoutView", "Container", "TextDisplay"))


def build_close_ranking_lines(
    top_rows: list[dict],
    *,
    mention_users: bool = True,
    inactive_user_ids: set[int] | None = None,
) -> list[str]:
    inactive_user_ids = inactive_user_ids or set()
    lines: list[str] = []
    for idx, row in enumerate(top_rows, start=1):
        user_id = int(row["user_id"])
        display_name = str(row.get("display_name", "")).strip() or f"User {user_id}"
        final_networth = float(row.get("networth", 0.0))
        top_bonus = float(row.get("top_networth_bonus", 0.0))
        display_base = float(row.get("display_base_networth", final_networth - top_bonus))
        can_mention = mention_users and (user_id not in inactive_user_ids)
        user_label = f"<@{user_id}>" if can_mention else display_name
        if idx == 1:
            prefix = "🥇"
        elif idx == 2:
            prefix = "🥈"
        elif idx == 3:
            prefix = "🥉"
        else:
            prefix = f"#{idx}"
        if abs(top_bonus) > 1e-9:
            sign = "+" if top_bonus >= 0 else "-"
            lines.append(
                f"**{prefix}** {user_label} — Networth `${display_base:.2f}` {sign} **`${abs(top_bonus):.2f}`**"
            )
        else:
            lines.append(f"**{prefix}** {user_label} — Networth `${final_networth:.2f}`")
    return lines


def build_market_close_v2_view(
    announcement_md: str,
    ranking_lines: list[str],
    *,
    role_mention: str = "",
    timeout: float | None = None,
) -> discord.ui.LayoutView | None:
    if not supports_components_v2():
        return None
    ui = discord.ui
    try:
        view = ui.LayoutView(timeout=timeout)
        container = ui.Container()
        body_parts: list[str] = []
        if role_mention.strip():
            body_parts.append(role_mention.strip())
        if announcement_md.strip():
            body_parts.append(announcement_md.strip())
        if body_parts:
            container.add_item(ui.TextDisplay(content="\n\n".join(body_parts)))
            if hasattr(ui, "Separator"):
                container.add_item(ui.Separator())
        container.add_item(ui.TextDisplay(content="# **Market Close: Networth Leaders**"))
        container.add_item(ui.TextDisplay(content="\n".join(ranking_lines) if ranking_lines else "_No ranking data_"))
        view.add_item(container)
        return view
    except Exception:
        return None


def build_market_close_embed(
    announcement_md: str,
    ranking_lines: list[str],
    *,
    title: str = "# Market Close!",
) -> Embed:
    parts: list[str] = []
    if announcement_md.strip():
        parts.append(announcement_md.strip())
    parts.append("**Market Close: Commodity Networth Leaders**")
    parts.append("\n".join(ranking_lines) if ranking_lines else "_No ranking data_")
    return Embed(title=title, description="\n\n".join(parts))


def build_close_news_embed(row: dict, *, preview: bool = False) -> Embed:
    news_title = str(row.get("title", "")).strip() or "News"
    news_body = str(row.get("body", "")).strip()
    news_image = str(row.get("image_url", "")).strip()
    if preview:
        news_title = f"{news_title} *(preview)*"
    embed = Embed(title=news_title, description=news_body)
    if news_image:
        embed.set_image(url=news_image)
    return embed


async def send_market_close_v2(
    *,
    channel: discord.TextChannel | discord.Thread,
    role_mention: str,
    announcement_md: str,
    ranking_lines: list[str],
    content_suffix: str = "market close!",
) -> bool:
    view = build_market_close_v2_view(
        announcement_md,
        ranking_lines,
        role_mention=role_mention,
        timeout=None,
    )
    if view is None:
        print("[announce] V2 unavailable: could not build market-close LayoutView/container.")
        return False
    try:
        _ = content_suffix  # kept for caller compatibility; V2 messages cannot use top-level content
        await cast(Any, channel).send(view=view)
        return True
    except Exception as exc:
        print(f"[announce] V2 send failed: {exc}")
        return False
