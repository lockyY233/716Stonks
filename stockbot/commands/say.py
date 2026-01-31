import random
from discord import Interaction, Member, app_commands

def setup_hello(tree: app_commands.CommandTree) -> None:
    @tree.command(name="hello", description="say hello to the bot")
    async def hello(interaction: Interaction) -> None:
        user = interaction.user # discord user
        responses = [
            f"Hello {user.mention} !",
            f"Hello there {user.mention} ~",
            f"Don't touch me {user.mention}",
            f"I love you too {user.mention}",
            f"get gud {user.mention}",
            f"Your action has been reported to HR {user.mention}",
            f"buy licky_Y coin! (totally not sponsored)",
            f"SIX SEVEN SIX SEVEN SIX SEVEN SIX SEVEN SIX SEVEN SIX SEVEN SIX SEVEN SIX SEVEN SIX SEVEN",
            f"you are my least favorite one",
            f"huh? what did you just say???"
        ]
        await interaction.response.send_message(
            random.choice(responses)
        )

def setup_no(tree: app_commands.CommandTree) -> None:
    @tree.command(name="no", description="I will disagree" )
    async def No(interaction: Interaction, target: Member | None = None) -> None:
        if target:
            msg = f"I disagree with {target.mention}"
        else:
            msg = "No. I disagree."
        await interaction.response.send_message(msg)
