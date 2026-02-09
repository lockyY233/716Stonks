from io import BytesIO

import matplotlib
from discord import ButtonStyle, Embed, File, Interaction, app_commands
from discord.ui import View, button

from stockbot.db import get_companies, get_users

matplotlib.use("Agg")
from matplotlib import pyplot as plt


_PAGE_SIZE = 10


class AdminShowPager(View):
    def __init__(self, owner_id: int, pages: list[Embed]) -> None:
        super().__init__(timeout=180)
        self._owner_id = owner_id
        self._pages = pages
        self._index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        self.prev.disabled = self._index <= 0
        self.next.disabled = self._index >= len(self._pages) - 1

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self._owner_id:
            await interaction.response.send_message(
                "Only the command user can change pages.",
                ephemeral=True,
            )
            return False
        return True

    @button(label="Prev", style=ButtonStyle.secondary)
    async def prev(self, interaction: Interaction, _button) -> None:
        self._index = max(0, self._index - 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._pages[self._index], view=self)

    @button(label="Next", style=ButtonStyle.secondary)
    async def next(self, interaction: Interaction, _button) -> None:
        self._index = min(len(self._pages) - 1, self._index + 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._pages[self._index], view=self)


def _chunks(rows: list[list[str]]) -> list[list[list[str]]]:
    return [rows[i : i + _PAGE_SIZE] for i in range(0, len(rows), _PAGE_SIZE)]


def _render_table_image(
    title: str,
    headers: list[str],
    rows: list[list[str]],
) -> BytesIO:
    height = max(2.6, 1.2 + (len(rows) * 0.42))
    fig, ax = plt.subplots(figsize=(14, height))
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=headers,
        cellLoc="left",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.3)
    ax.set_title(title, fontsize=13, pad=10)
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=170)
    plt.close(fig)
    buf.seek(0)
    return buf


def _build_user_pages(rows: list[dict]) -> tuple[list[Embed], list[File]]:
    if not rows:
        return [Embed(title="adminshow: users", description="No users found.")], []

    table_rows: list[list[str]] = []
    for row in rows:
        table_rows.append(
            [
                str(row["user_id"]),
                str(row.get("display_name", "")),
                f"{float(row['bank']):.2f}",
                str(row["rank"]),
                str(row["joined_at"]),
            ]
        )

    pages: list[Embed] = []
    files: list[File] = []
    chunks = _chunks(table_rows)
    for i, chunk in enumerate(chunks, start=1):
        filename = f"adminshow_users_{i}.png"
        image = _render_table_image(
            title="Users Table",
            headers=["user_id", "display_name", "bank", "rank", "joined_at"],
            rows=chunk,
        )
        files.append(File(image, filename=filename))
        embed = Embed(title="adminshow: users")
        embed.set_image(url=f"attachment://{filename}")
        embed.set_footer(text=f"Page {i}/{len(chunks)} • Total {len(rows)}")
        pages.append(embed)
    return pages, files


def _build_company_pages(rows: list[dict]) -> tuple[list[Embed], list[File]]:
    if not rows:
        return [Embed(title="adminshow: companies", description="No companies found.")], []

    table_rows: list[list[str]] = []
    for row in rows:
        table_rows.append(
            [
                str(row["symbol"]),
                str(row["name"]),
                f"{float(row['base_price']):.2f}",
                f"{float(row.get('current_price', row['base_price'])):.2f}",
                f"{float(row['slope']):.4f}",
                f"{float(row['drift']):.4f}",
                f"{float(row.get('player_impact', 0.5)):.2f}",
            ]
        )

    pages: list[Embed] = []
    files: list[File] = []
    chunks = _chunks(table_rows)
    for i, chunk in enumerate(chunks, start=1):
        filename = f"adminshow_companies_{i}.png"
        image = _render_table_image(
            title="Companies Table",
            headers=["symbol", "name", "base", "current", "slope", "drift", "impact"],
            rows=chunk,
        )
        files.append(File(image, filename=filename))
        embed = Embed(title="adminshow: companies")
        embed.set_image(url=f"attachment://{filename}")
        embed.set_footer(text=f"Page {i}/{len(chunks)} • Total {len(rows)}")
        pages.append(embed)
    return pages, files


def setup_adminshow(tree: app_commands.CommandTree) -> None:
    @tree.command(name="adminshow", description="Admin: show raw data tables.")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(target="Pick what data to show.")
    @app_commands.choices(
        target=[
            app_commands.Choice(name="users", value="users"),
            app_commands.Choice(name="companies", value="companies"),
        ]
    )
    async def adminshow(interaction: Interaction, target: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please use this command in a server.",
                ephemeral=True,
            )
            return

        if target == "users":
            pages, files = _build_user_pages(get_users(interaction.guild.id))
        elif target == "companies":
            pages, files = _build_company_pages(get_companies(interaction.guild.id))
        else:
            await interaction.response.send_message(
                f"Unknown target: `{target}`.",
                ephemeral=True,
            )
            return

        view = AdminShowPager(interaction.user.id, pages)
        await interaction.response.send_message(
            embed=pages[0],
            files=files,
            view=view,
            ephemeral=True,
        )
