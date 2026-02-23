"""One-shot detection for arbitrage and middles."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage import detect_all_arbitrage
from middles import detect_all_middles
from utils import init_db, load_config

MAX_RESULTS = 10


def _format_arb(arb: dict) -> str:
    return (
        f"{arb.get('market', '')} "
        f"{arb.get('side_a', '')}@{arb.get('provider_a', '')} vs "
        f"{arb.get('side_b', '')}@{arb.get('provider_b', '')} "
        f"margin={arb.get('margin', 0):.2%} profit=${arb.get('guaranteed_profit', 0):.2f}"
    )


def _format_middle(mid: dict) -> str:
    return (
        f"{mid.get('market', '')} "
        f"{mid.get('description', '')} "
        f"gap={mid.get('gap', 0):.1f} ev=${mid.get('ev', 0):.2f}"
    )


def run() -> None:
    config = load_config()
    conn = init_db(config["storage"]["database"])

    arb_cfg = config.get("arbitrage", {})
    min_edge = arb_cfg.get("min_edge_percent", 0.5) / 100
    max_age = arb_cfg.get("max_data_age_seconds", 600)
    bankroll = arb_cfg.get("reference_bankroll", 100)

    arbs = detect_all_arbitrage(conn, min_edge, max_age, bankroll)
    all_arbs: list[dict] = []
    for group in arbs.values():
        all_arbs.extend(group)

    all_arbs.sort(key=lambda x: x.get("margin", 0), reverse=True)

    print(
        "arbitrage: total={} open={} sportsbook={} cross={} props={}".format(
            len(all_arbs),
            len(arbs.get("open_market", [])),
            len(arbs.get("sportsbook", [])),
            len(arbs.get("cross_market", [])),
            len(arbs.get("player_prop", [])),
        )
    )
    for arb in all_arbs[:MAX_RESULTS]:
        print(f"- {_format_arb(arb)}")

    middles = detect_all_middles(conn, config)
    print(f"middles: total={len(middles)}")
    for mid in middles[:MAX_RESULTS]:
        print(f"- {_format_middle(mid)}")

    conn.close()


if __name__ == "__main__":
    run()
