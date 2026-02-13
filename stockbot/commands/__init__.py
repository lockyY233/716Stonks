from discord import app_commands

from stockbot.commands.addcompany import setup_addcompany
from stockbot.commands.addcommodity import setup_addcommodity
from stockbot.commands.buy import setup_buy
from stockbot.commands.purgeall import setup_purgeall
from stockbot.commands.sell import setup_sell
from stockbot.commands.say import setup_hello, setup_no
from stockbot.commands.register import setup_register
from stockbot.commands.promote import setup_promote
from stockbot.commands.donate import setup_donate
from stockbot.commands.ranking import setup_ranking
from stockbot.commands.price import setup_price
from stockbot.commands.status import setup_status
from stockbot.commands.list import setup_list
from stockbot.commands.help import setup_help
from stockbot.commands.shop import setup_shop
from stockbot.commands.resettradinglimit import setup_resettradinglimit
from stockbot.commands.wipe import setup_wipe
from stockbot.commands.adminshow import setup_adminshow
from stockbot.commands.desc import setup_desc
from stockbot.commands.webadmin import setup_webadmin
from stockbot.commands.bank import setup_bank
from stockbot.commands.nextreset import setup_nextreset
from stockbot.commands.feedback import setup_feedback
from stockbot.commands.admingive import setup_admingive
from stockbot.commands.perks import setup_perks

def setup_commands(tree: app_commands.CommandTree) -> None:
    setup_addcompany(tree)
    setup_addcommodity(tree)
    setup_buy(tree)
    setup_hello(tree)
    setup_no(tree)
    setup_purgeall(tree)
    setup_register(tree)
    setup_promote(tree)
    setup_donate(tree)
    setup_ranking(tree)
    setup_sell(tree)
    setup_price(tree)
    setup_status(tree)
    setup_list(tree)
    setup_help(tree)
    setup_shop(tree)
    setup_resettradinglimit(tree)
    setup_wipe(tree)
    setup_adminshow(tree)
    setup_desc(tree)
    setup_webadmin(tree)
    setup_bank(tree)
    setup_nextreset(tree)
    setup_feedback(tree)
    setup_admingive(tree)
    setup_perks(tree)
