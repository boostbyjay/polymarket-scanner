#!/usr/bin/env python3
"""
Polymarket 12-Hour Whale Scanner & Smart-Money Finder
=====================================================
Scans all wallets active on Polymarket in the last 12 hours (or customizable window),
categorizes them into Bots vs. No Bot (Humans), and ranks them to uncover profitable whales.

Usage:
    python3 whale_scanner.py [options]

Examples:
    python3 whale_scanner.py                                # Default: Last 12h, top 30 markets
    python3 whale_scanner.py --bot nobot --profitable       # Only profitable human whales
    python3 whale_scanner.py --min-vol 10000                # Only whales ($10k+ 12h volume)
    python3 whale_scanner.py --hours 6 --top 50             # Last 6 hours, show top 50
    python3 whale_scanner.py --csv whales_12h.csv           # Export results to CSV
"""

import sys
import os
import time
import json
import argparse
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional

try:
    import requests
except ImportError:
    print("Error: 'requests' library required. Install with: pip install requests")
    sys.exit(1)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"


def fetch_top_active_markets(limit: int = 35) -> List[Dict[str, Any]]:
    """Fetches the top volume markets over the last 24h from Gamma API."""
    print(f"[*] Fetching top {limit} active markets on Polymarket...")
    try:
        r = requests.get(
            f"{GAMMA_API_BASE}/markets",
            params={"order": "volume24hr", "ascending": "false", "limit": limit},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data
    except Exception as e:
        print(f"[!] Error fetching active markets: {e}")
    return []


def fetch_market_trades(condition_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """Fetches recent trades for a specific market condition ID."""
    try:
        r = requests.get(
            f"{DATA_API_BASE}/trades",
            params={"market": condition_id, "limit": limit},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def fetch_global_recent_trades(max_batches: int = 3) -> List[Dict[str, Any]]:
    """Fetches latest platform-wide real-time trades from the global trade stream."""
    all_global = []
    for i in range(max_batches):
        offset = i * 1000
        try:
            r = requests.get(
                f"{DATA_API_BASE}/trades",
                params={"limit": 1000, "offset": offset},
                timeout=8
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    all_global.extend(data)
                else:
                    break
            else:
                break
        except Exception:
            break
    return all_global


def scan_recent_wallets(hours: float = 12.0, market_count: int = 35, scan_workers: int = 20) -> List[Dict[str, Any]]:
    """
    Scans trades across top markets and global feeds within the last N hours,
    aggregating trading volume, single max bet, trade count, and active markets per wallet.
    """
    now = int(time.time())
    cutoff_ts = now - int(hours * 3600)

    markets = fetch_top_active_markets(limit=market_count)
    print(f"[*] Aggregating trades across {len(markets)} top markets and global trade feeds...")

    raw_trades = []

    def worker_market(m):
        cid = m.get("conditionId")
        if not cid:
            return []
        return fetch_market_trades(cid)

    with ThreadPoolExecutor(max_workers=scan_workers) as executor:
        market_results = list(executor.map(worker_market, markets))

    for batch in market_results:
        raw_trades.extend(batch)

    # Pull global recent trades as well
    global_trades = fetch_global_recent_trades(max_batches=3)
    raw_trades.extend(global_trades)

    print(f"[*] Processing {len(raw_trades):,} total gathered trade records...")

    wallets_map: Dict[str, Dict[str, Any]] = {}
    valid_trades_count = 0
    total_volume = 0.0

    for t in raw_trades:
        ts = t.get("timestamp") or 0
        if ts < cutoff_ts:
            continue

        addr = t.get("proxyWallet")
        if not addr:
            continue

        size = float(t.get("size", 0) or 0)
        price = float(t.get("price", 0) or 0)
        cost = size * price

        valid_trades_count += 1
        total_volume += cost

        if addr not in wallets_map:
            wallets_map[addr] = {
                "wallet": addr,
                "name": t.get("name") or t.get("pseudonym") or "",
                "trades_count": 0,
                "volume_window": 0.0,
                "max_single_bet": 0.0,
                "markets_traded": set(),
                "latest_trade_ts": ts,
            }

        w_entry = wallets_map[addr]
        w_entry["trades_count"] += 1
        w_entry["volume_window"] += cost
        if cost > w_entry["max_single_bet"]:
            w_entry["max_single_bet"] = cost
        m_title = t.get("title") or t.get("slug") or "Market"
        w_entry["markets_traded"].add(m_title)
        if ts > w_entry["latest_trade_ts"]:
            w_entry["latest_trade_ts"] = ts
        if not w_entry["name"] and (t.get("name") or t.get("pseudonym")):
            w_entry["name"] = t.get("name") or t.get("pseudonym")

    print(f"[+] Found {len(wallets_map):,} unique active wallets across {valid_trades_count:,} trades in the last {hours:.1f} hours.")
    print(f"[+] Total window traded volume: ${total_volume:,.2f}")

    return list(wallets_map.values())


def enrich_wallets_lifetime(wallets: List[Dict[str, Any]], max_enrich: int = 120, workers: int = 25) -> List[Dict[str, Any]]:
    """
    Enriches candidate wallets with authoritative Lifetime P&L (via /v2/user-stats),
    Lifetime Win Rate, Total Markets, and Bot vs. No Bot classification.
    """
    # Sort primarily by window volume to prioritize whales
    sorted_candidates = sorted(wallets, key=lambda x: (x["volume_window"], x["max_single_bet"]), reverse=True)
    targets = sorted_candidates[:max_enrich]

    print(f"[*] Enriching top {len(targets):,} candidate wallets with lifetime ledger data and bot detection...")

    completed = 0

    def enrich_single(w_obj):
        nonlocal completed
        addr = w_obj["wallet"]
        try:
            r_stats = requests.get(f"{DATA_API_BASE}/v2/user-stats", params={"user": addr}, timeout=7)
            v2_data = r_stats.json().get("data") if r_stats.status_code == 200 and isinstance(r_stats.json(), dict) else None
            pnl_data = v2_data.get("all_time_pnl") if v2_data else None

            # Authoritative Lifetime PnL
            if pnl_data and ("trade_pnl" in pnl_data or "realized_pnl" in pnl_data):
                trade_pnl = pnl_data.get("trade_pnl")
                if trade_pnl is not None:
                    lt_pnl = float(trade_pnl)
                else:
                    realized = float(pnl_data.get("realized_pnl", 0) or 0)
                    unrealized = float(pnl_data.get("unrealized_pnl", 0) or 0)
                    lt_pnl = realized + unrealized
                total_positions = int(v2_data.get("trades", 0) or 0)
                if v2_data.get("user_name") and not w_obj["name"]:
                    w_obj["name"] = v2_data.get("user_name")
            else:
                lt_pnl = 0.0
                total_positions = len(w_obj["markets_traded"])

            # Query sample positions for win rate
            r_closed = requests.get(f"{DATA_API_BASE}/closed-positions", params={"user": addr, "limit": 60}, timeout=6)
            closed_pos = r_closed.json() if r_closed.status_code == 200 and isinstance(r_closed.json(), list) else []

            if closed_pos:
                wins = sum(1 for p in closed_pos if float(p.get("realizedPnl", 0) or 0) > 0.01)
                win_rate = (wins / len(closed_pos)) * 100.0
                lt_wins = int(round((win_rate / 100.0) * max(total_positions, len(closed_pos))))
            else:
                win_rate = 100.0 if lt_pnl > 0.01 else 0.0
                lt_wins = 1 if lt_pnl > 0.01 else 0

            # Bot vs No Bot (Human) Detection
            maker_rebate = float(pnl_data.get("maker_rebate", 0) or 0) if pnl_data else 0.0
            taker_rebate = float(pnl_data.get("taker_rebate", 0) or 0) if pnl_data else 0.0
            lifetime_trades_count = int(pnl_data.get("trade_count", 0) or 0) if pnl_data else 0
            recent_trades = w_obj["trades_count"]

            bot_signals = []
            if maker_rebate > 5.0 or taker_rebate > 20.0:
                bot_signals.append(f"Market Maker Rebates (${maker_rebate:.0f})")
            if lifetime_trades_count >= 2500:
                bot_signals.append(f"High Trades ({lifetime_trades_count:,})")
            elif total_positions >= 800:
                bot_signals.append(f"High Markets ({total_positions:,})")
            elif lifetime_trades_count >= 500 and (lifetime_trades_count / max(1, total_positions)) > 15:
                bot_signals.append(f"Laddering ({lifetime_trades_count/total_positions:.1f} tr/mkt)")
            elif recent_trades >= 30:
                bot_signals.append(f"Rapid Trades ({recent_trades})")

            is_bot = len(bot_signals) > 0
            bot_label = "BOT" if is_bot else "NO BOT"
            bot_detail = "; ".join(bot_signals) if is_bot else "Manual Human Trader"

            # Whale Tier
            vol = w_obj["volume_window"]
            if vol >= 50000:
                whale_tier = "🐋 Mega Whale"
            elif vol >= 10000:
                whale_tier = "🐳 Whale"
            elif vol >= 2500:
                whale_tier = "🐬 Shark"
            elif vol >= 500:
                whale_tier = "🐟 Active Trader"
            else:
                whale_tier = "🦐 Retail"

            w_obj.update({
                "is_bot": is_bot,
                "bot_label": bot_label,
                "bot_detail": bot_detail,
                "whale_tier": whale_tier,
                "lifetime_pnl": lt_pnl,
                "lifetime_win_rate": win_rate,
                "lifetime_wins": lt_wins,
                "lifetime_positions": total_positions,
                "lifetime_category": "PROFITABLE" if lt_pnl > 0.01 else ("UNPROFITABLE" if lt_pnl < -0.01 else "BREAKEVEN")
            })

        except Exception:
            w_obj.update({
                "is_bot": False,
                "bot_label": "NO BOT",
                "bot_detail": "Manual Human Trader",
                "whale_tier": "🐋 Whale" if w_obj["volume_window"] >= 10000 else "🐟 Trader",
                "lifetime_pnl": 0.0,
                "lifetime_win_rate": 0.0,
                "lifetime_wins": 0,
                "lifetime_positions": len(w_obj["markets_traded"]),
                "lifetime_category": "BREAKEVEN"
            })
        finally:
            completed += 1
            if completed % 10 == 0 or completed == len(targets):
                print(f"    -> Enriched {completed:,} / {len(targets):,} wallets...", end="\r", flush=True)

        return w_obj

    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(enrich_single, targets))

    print(f"\n[+] Successfully enriched {len(results):,} wallets.")
    return results


def display_whales_dashboard(wallets: List[Dict[str, Any]], args: argparse.Namespace):
    """Renders the 12-Hour Whale Leaderboard report with full filtering."""
    filtered = wallets

    # 1. Filter by Bot / Human
    if args.bot == "bot":
        filtered = [w for w in filtered if w.get("is_bot")]
    elif args.bot == "nobot":
        filtered = [w for w in filtered if not w.get("is_bot")]

    # 2. Filter by Profitability
    if args.profitable:
        filtered = [w for w in filtered if w.get("lifetime_pnl", 0) > 0.01]
    elif args.category == "profitable":
        filtered = [w for w in filtered if w.get("lifetime_pnl", 0) > 0.01]
    elif args.category == "unprofitable":
        filtered = [w for w in filtered if w.get("lifetime_pnl", 0) < -0.01]

    # 3. Filter by Min Volume
    if args.min_vol > 0:
        filtered = [w for w in filtered if w.get("volume_window", 0) >= args.min_vol]

    # 4. Filter by Min Single Bet
    if args.min_bet > 0:
        filtered = [w for w in filtered if w.get("max_single_bet", 0) >= args.min_bet]

    # 5. Filter by Min Win Rate
    if args.min_winrate > 0:
        filtered = [w for w in filtered if w.get("lifetime_win_rate", 0) >= args.min_winrate]

    # Sort
    if args.sort == "volume":
        filtered.sort(key=lambda x: x.get("volume_window", 0), reverse=True)
    elif args.sort == "bet":
        filtered.sort(key=lambda x: x.get("max_single_bet", 0), reverse=True)
    elif args.sort == "pnl" or args.sort == "profit":
        filtered.sort(key=lambda x: x.get("lifetime_pnl", 0), reverse=True)
    elif args.sort == "winrate":
        filtered.sort(key=lambda x: (x.get("lifetime_win_rate", 0), x.get("volume_window", 0)), reverse=True)
    elif args.sort == "trades":
        filtered.sort(key=lambda x: x.get("trades_count", 0), reverse=True)

    total_humans = sum(1 for w in wallets if not w.get("is_bot"))
    total_bots = sum(1 for w in wallets if w.get("is_bot"))
    profitable_humans = sum(1 for w in wallets if not w.get("is_bot") and w.get("lifetime_pnl", 0) > 0)
    profitable_bots = sum(1 for w in wallets if w.get("is_bot") and w.get("lifetime_pnl", 0) > 0)

    print("\n" + "=" * 115)
    print(f" 🌊 POLYMARKET {args.hours:g}-HOUR ACTIVE WHALE & SMART-MONEY REPORT")
    print("=" * 115)
    print(f" Window:           Last {args.hours:g} Hours")
    print(f" Total Enriched:   {len(wallets):,} Wallets | {total_humans:,} Humans ({profitable_humans} Profitable) | {total_bots:,} Bots ({profitable_bots} Profitable)")
    print(f" Showing:          {len(filtered[:args.top]):,} of {len(filtered):,} Wallets matching active filters")
    print("=" * 115)

    if not filtered:
        print("\n [!] No wallets match the selected filters. Try lowering thresholds (e.g. --min-vol 1000).")
        return

    header = f"{'#':<3} | {'Type':<8} | {'Tier':<16} | {'Wallet / Pseudonym':<22} | {f'{args.hours:g}h Vol ($)':>14} | {'Max Bet ($)':>12} | {'Trades':>6} | {'Lifetime P&L':>15} | {'Win %':>7}"
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for rank, w in enumerate(filtered[:args.top], 1):
        bot_label = f"[{w.get('bot_label', 'NO BOT')}]"
        tier = w.get("whale_tier", "Whale")
        name = w.get("name") or f"{w['wallet'][:6]}...{w['wallet'][-4:]}"
        if len(name) > 20:
            name = name[:17] + "..."

        vol_str = f"${w.get('volume_window', 0):,.2f}"
        bet_str = f"${w.get('max_single_bet', 0):,.2f}"
        trades_str = f"{w.get('trades_count', 0)}"

        pnl = w.get("lifetime_pnl", 0.0)
        pnl_sign = '+' if pnl > 0 else ('-' if pnl < 0 else '')
        pnl_str = f"{pnl_sign}${abs(pnl):,.2f}"
        win_str = f"{w.get('lifetime_win_rate', 0.0):.1f}%"

        print(f"{rank:<3} | {bot_label:<8} | {tier:<16} | {name:<22} | {vol_str:>14} | {bet_str:>12} | {trades_str:>6} | {pnl_str:>15} | {win_str:>7}")

    print("-" * len(header))

    if args.csv:
        export_whales_csv(args.csv, filtered, args.hours)


def export_whales_csv(filepath: str, rows: List[Dict[str, Any]], hours: float):
    import csv
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Rank",
            "Trader_Type",
            "Bot_Signals",
            "Whale_Tier",
            "Wallet",
            "Pseudonym",
            f"Volume_{hours:g}h_USD",
            "Max_Single_Bet_USD",
            f"Trades_Count_{hours:g}h",
            "Markets_Active_Count",
            "Lifetime_PnL_USD",
            "Lifetime_WinRate_Pct",
            "Lifetime_Wins",
            "Lifetime_Total_Markets",
            "Lifetime_Category"
        ])
        for idx, r in enumerate(rows, 1):
            writer.writerow([
                idx,
                r.get("bot_label", "NO BOT"),
                r.get("bot_detail", ""),
                r.get("whale_tier", ""),
                r.get("wallet", ""),
                r.get("name", ""),
                f"{r.get('volume_window', 0.0):.2f}",
                f"{r.get('max_single_bet', 0.0):.2f}",
                r.get("trades_count", 0),
                len(r.get("markets_traded", [])),
                f"{r.get('lifetime_pnl', 0.0):.2f}",
                f"{r.get('lifetime_win_rate', 0.0):.2f}",
                r.get("lifetime_wins", 0),
                r.get("lifetime_positions", 0),
                r.get("lifetime_category", "")
            ])
    print(f"\n[+] Successfully exported {len(rows)} whale records to CSV: {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Scan Polymarket wallets active in the last 12 hours, categorize Bots vs. Humans, and uncover profitable whales."
    )
    parser.add_argument("--hours", type=float, default=12.0, help="Scan lookback window in hours (default: 12.0)")
    parser.add_argument("--markets", type=int, default=30, help="Number of top volume markets to scan (default: 30)")
    parser.add_argument("--enrich", type=int, default=100, help="Number of top active wallets to enrich with ledger stats (default: 100)")
    parser.add_argument("--top", type=int, default=25, help="Number of wallets to display in table (default: 25)")
    parser.add_argument("--bot", choices=["all", "bot", "nobot"], default="all", help="Filter by trader type: all, bot, or nobot (default: all)")
    parser.add_argument("--profitable", action="store_true", help="Filter for only lifetime profitable wallets (Lifetime P&L > $0)")
    parser.add_argument("--category", choices=["all", "profitable", "unprofitable"], default="all", help="Category filter")
    parser.add_argument("--min-vol", type=float, default=0.0, help="Minimum volume in window (e.g. 10000 for $10k+ whales)")
    parser.add_argument("--min-bet", type=float, default=0.0, help="Minimum single bet size threshold")
    parser.add_argument("--min-winrate", type=float, default=0.0, help="Minimum lifetime win %% (e.g. 60 for 60%%+ win rate)")
    parser.add_argument("--sort", choices=["volume", "bet", "pnl", "profit", "winrate", "trades"], default="volume", help="Sort order (default: volume)")
    parser.add_argument("--csv", type=str, default=None, help="Save whale scan results to CSV file")

    args = parser.parse_args()

    # Step 1: Scan recent active wallets
    wallets = scan_recent_wallets(hours=args.hours, market_count=args.markets)
    if not wallets:
        print("[!] No active wallets found in specified time window.")
        sys.exit(0)

    # Step 2: Enrich top active wallets with Lifetime Ledger PnL & Bot signals
    enriched = enrich_wallets_lifetime(wallets, max_enrich=args.enrich)

    # Step 3: Display formatted dashboard
    display_whales_dashboard(enriched, args)


if __name__ == "__main__":
    main()
