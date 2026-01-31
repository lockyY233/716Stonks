from discord import app_commands

from stockbot.commands.say import setup_hello, setup_no
from stockbot.commands.register import setup_register
from stockbot.commands.status import setup_status

def setup_commands(tree: app_commands.CommandTree) -> None:
    setup_hello(tree)
    setup_no(tree)
    setup_register(tree)
    setup_status(tree)
