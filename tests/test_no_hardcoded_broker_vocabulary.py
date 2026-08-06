"""No broker's words inside the edge.

The whole point of the capability layer is that `nakagai_edge/` does not know
what Robinhood calls anything. The connector's `capabilities:` map holds the
spelling, the edge holds the meaning. This test is what stops that from quietly
coming back one convenience at a time, the same way test_import_closure.py
stops the dependency tree from growing.

A leak is not cosmetic. One broker's vocabulary compiled into the portfolio
sweep, the quote feed and the position re-read is what made a second broker
unsupportable: the brake would stop seeing positions while every display went
on reporting them as guarded.
"""

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent / "nakagai_edge"

# word -> the files, relative to nakagai_edge/, genuinely entitled to say it.
# Everything else is a leak.
#
# Broker TOOL names belong nowhere. They are pure connector vocabulary now, and
# a connector's map is the only thing allowed to name one.
#
# Broker FIELD names are narrower than that. Three of them below have no
# canonical twin at all, so they are forbidden everywhere. `buying_power` is
# the exception: it is a canonical field of `get_balance` whose spelling
# happens to match Robinhood's. capability.py declares that vocabulary and
# runtime.py's `get_balance` docstring publishes it to the agent, beside
# `equity`, `cash` and `currency`, which no broker owns either.
#
# `account_number` is entitled in two unrelated ways. config.py's
# AccountFilter.arg_names carries the default account keys for check_accounts,
# which walks arbitrary args on the raw `call_connector` path and genuinely
# needs literals. And it survives as a field name of the portfolio DOCUMENT,
# which is the web's contract until its own migration lands: portfolio.py emits
# it and supervision.py reads it, both sourced through the map. Delete those
# two entries when the web migration retypes the Portfolio page.
FORBIDDEN = {
    "get_accounts": set(),
    "get_portfolio": set(),
    "get_equity_positions": set(),
    "get_quotes": set(),
    "get_equity_orders": set(),
    "place_equity_order": set(),
    "average_buy_price": set(),
    "last_trade_price": set(),
    "total_value": set(),
    # The guess-list executor.py used to try in order for an entry's fill
    # price, beside a hand-rolled peel of Robinhood's `data` envelope. It
    # predated the capability layer and survived it, because the layer landed
    # in the read paths and nobody re-read the write path. `fill_price` is a
    # canonical field now and the map says where it lives, so none of these
    # three has a canonical twin and all three are forbidden everywhere.
    "average_price": set(),
    "filled_price": set(),
    "execution_price": set(),
    "buying_power": {"capability.py", "edge/runtime.py"},
    "account_number": {"config.py", "edge/portfolio.py", "edge/supervision.py"},
}


def test_no_broker_vocabulary_in_the_edge():
    offenders = []
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        lines = path.read_text().splitlines()
        for word, allowed in FORBIDDEN.items():
            if rel in allowed:
                continue
            for n, line in enumerate(lines, 1):
                if re.search(rf"\b{word}\b", line):
                    offenders.append(f"{rel}:{n}: {word}")
    assert not offenders, (
        "broker vocabulary leaked back into the edge:\n  "
        + "\n  ".join(sorted(offenders))
        + "\n\nDeclare it in the connector's `capabilities:` map instead.")


def test_the_allowlist_does_not_outlive_its_files():
    """An exemption must not survive the file it was written for.

    A renamed or deleted file leaves a line here that reads as though something
    still needs it, and the next person to move code into a path this list
    already names inherits a hole nobody chose to open.
    """
    missing = sorted({rel for allowed in FORBIDDEN.values() for rel in allowed
                      if not (ROOT / rel).exists()})
    assert missing == [], (
        f"FORBIDDEN allowlists {missing}, which nakagai_edge/ no longer has. "
        f"Drop the entry: the word is forbidden everywhere now, which is the "
        f"answer this list should already be giving.")
