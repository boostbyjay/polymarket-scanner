#!/usr/bin/env python3
"""
Simple Polymarket Link Scanner
==============================
Run:
    python3 scan.py
Then simply paste your Polymarket link and press Enter!
"""

import sys
import os

# Import the core scanner engine
from scripts.polymarket_wallet_scanner import (
    extract_identifier,
    resolve_market,
    fetch_all_trades,
    fetch_holders,
    analyze_wallets,
    print_dashboard,
    argparse
)

def run():
    print("=" * 65)
    print("  POLYMARKET QUICK WALLET SCANNER")
    print("=" * 65)

    raw_market = ""
    submarket_idx = None
    scan_limit = None
    bot_filter = "all"

    args = sys.argv[1:]
    if "--whales" in args:
        import whale_scanner
        sys.argv = [sys.argv[0]] + [a for a in args if a != "--whales"]
        whale_scanner.main()
        return

    idx = 0
    while idx < len(args):
        arg = args[idx]
        if arg == "--submarket" and idx + 1 < len(args):
            submarket_idx = int(args[idx + 1])
            idx += 2
        elif arg == "--limit" and idx + 1 < len(args):
            scan_limit = int(args[idx + 1])
            idx += 2
        elif arg == "--bot" and idx + 1 < len(args):
            bot_filter = args[idx + 1]
            idx += 2
        elif not arg.startswith("-") and not raw_market:
            raw_market = arg
            idx += 1
        else:
            idx += 1

    if not raw_market:
        try:
            raw_market = input("\n👉 Paste Polymarket Link (or type 'whales' for 12h Whale Scanner): ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    if raw_market.lower() in ("whales", "whale", "w"):
        import whale_scanner
        whale_scanner.main()
        return

    if not raw_market:
        print("[!] No link provided. Exiting.")
        sys.exit(1)

    id_type, id_val = extract_identifier(raw_market)

    try:
        market_meta = resolve_market(id_type, id_val, submarket_idx=submarket_idx)
    except Exception as e:
        print(f"[!] Error resolving market: {e}")
        sys.exit(1)

    trades = fetch_all_trades(market_meta["conditionId"])
    holders = fetch_holders(market_meta["conditionId"])
    results = analyze_wallets(market_meta, trades, holders)

    class QuickArgs:
        top = 25
        sort = "lifetime_pnl"
        category = "all"
        min_invested = 0.0
        min_trades = 10
        max_avg_bet = 0.0
        scan_limit = None
        csv = "wallets.csv"
        json = None

    q_args = QuickArgs()
    q_args.scan_limit = scan_limit
    q_args.bot = bot_filter

    print_dashboard(market_meta, results, q_args)
    print("\n[✓] Done! Complete data saved to 'wallets.csv'")

if __name__ == "__main__":
    run()
