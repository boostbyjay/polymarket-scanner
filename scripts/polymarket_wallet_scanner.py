#!/usr/bin/env python3
"""
Polymarket Market Wallet Scanner & PnL Ranker
=============================================
Scans all wallets that participated in a specific Polymarket market,
computes their realized and unrealized P&L, categorizes them as
Profitable or Unprofitable, and produces ranked leaderboards.

Usage:
    python3 polymarket_wallet_scanner.py <url_or_slug_or_conditionId> [options]

Examples:
    python3 polymarket_wallet_scanner.py kraken-ipo-in-2025
    python3 polymarket_wallet_scanner.py "https://polymarket.com/event/kraken-ipo-in-2025" --top 20
    python3 polymarket_wallet_scanner.py kraken-ipo-in-2025 --category profitable --csv winners.csv
    python3 polymarket_wallet_scanner.py kraken-ipo-in-2025 --sort losers --top 10
"""

import sys
import os
import re
import json
import argparse
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install it using: pip install requests")
    sys.exit(1)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"


def extract_identifier(raw_input: str) -> Tuple[str, str]:
    """
    Parses user input into either a condition ID or a market slug.
    Returns: (type, value) where type is 'condition_id' or 'slug'.
    """
    raw_input = raw_input.strip()

    # Check if raw input is a 66-character hex string (0x...)
    if raw_input.startswith("0x") and len(raw_input) == 66:
        return ("condition_id", raw_input)

    # If it's a URL, parse path
    if raw_input.startswith("http://") or raw_input.startswith("https://"):
        parsed = urllib.parse.urlparse(raw_input)
        path_parts = [p for p in parsed.path.split("/") if p]
        if "event" in path_parts:
            idx = path_parts.index("event")
            if idx + 1 < len(path_parts):
                return ("slug", path_parts[idx + 1])
        elif "market" in path_parts:
            idx = path_parts.index("market")
            if idx + 1 < len(path_parts):
                return ("slug", path_parts[idx + 1])
        elif path_parts:
            return ("slug", path_parts[-1])

    # Default to assuming raw string is a slug
    return ("slug", raw_input)


def resolve_market(id_type: str, id_val: str, submarket_idx: Optional[int] = None) -> Dict[str, Any]:
    """
    Queries Gamma API to retrieve market metadata, outcomes, prices, and resolution status.
    """
    print(f"[*] Resolving market via Gamma API ({id_type}: {id_val})...")
    
    market_obj = None
    event_obj = None

    if id_type == "condition_id":
        resp = requests.get(f"{GAMMA_API_BASE}/markets", params={"condition_id": id_val}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                market_obj = data[0]
            elif isinstance(data, dict) and "conditionId" in data:
                market_obj = data
    else:
        # Try event slug first
        resp = requests.get(f"{GAMMA_API_BASE}/events", params={"slug": id_val}, timeout=15)
        if resp.status_code == 200 and resp.json():
            events = resp.json()
            if len(events) > 0:
                event_obj = events[0]
                markets = event_obj.get("markets", [])
                if len(markets) == 1:
                    market_obj = markets[0]
                elif len(markets) > 1:
                    print(f"[*] Found {len(markets)} sub-markets for event '{event_obj.get('title')}':")
                    for i, m in enumerate(markets):
                        vol = float(m.get("volume", 0) or 0)
                        print(f"    [{i+1}] {m.get('question')} (Vol: ${vol:,.0f})")
                    
                    if submarket_idx is not None and 1 <= submarket_idx <= len(markets):
                        choice = submarket_idx - 1
                    elif not sys.stdin.isatty():
                        # Pick highest volume market in non-interactive mode
                        choice = 0
                    else:
                        try:
                            val = input(f"Select market [1-{len(markets)}] (default 1): ").strip()
                            if val.isdigit() and 1 <= int(val) <= len(markets):
                                choice = int(val) - 1
                            else:
                                choice = 0
                        except (EOFError, KeyboardInterrupt):
                            choice = 0
                    market_obj = markets[choice]

        if not market_obj:
            # Fallback to direct market slug
            resp = requests.get(f"{GAMMA_API_BASE}/markets", params={"slug": id_val}, timeout=15)
            if resp.status_code == 200 and resp.json():
                res = resp.json()
                if isinstance(res, list) and len(res) > 0:
                    market_obj = res[0]
                elif isinstance(res, dict) and "conditionId" in res:
                    market_obj = res

    if not market_obj:
        raise ValueError(f"Could not find market for {id_type}='{id_val}' on Polymarket.")

    # Parse outcomes and prices
    outcomes = market_obj.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = ["Yes", "No"]
    elif not outcomes:
        outcomes = ["Yes", "No"]

    outcome_prices_raw = market_obj.get("outcomePrices")
    prices = []
    if isinstance(outcome_prices_raw, str):
        try:
            prices = [float(p) for p in json.loads(outcome_prices_raw)]
        except Exception:
            prices = [0.5, 0.5]
    elif isinstance(outcome_prices_raw, list):
        prices = [float(p) for p in outcome_prices_raw]
    else:
        prices = [0.5] * len(outcomes)

    is_closed = market_obj.get("closed", False)

    return {
        "conditionId": market_obj["conditionId"],
        "question": market_obj.get("question") or (event_obj.get("title") if event_obj else "Unknown Market"),
        "slug": market_obj.get("slug") or id_val,
        "is_closed": is_closed,
        "outcomes": outcomes,
        "outcome_prices": prices,
        "clob_token_ids": market_obj.get("clobTokenIds"),
        "volume": float(market_obj.get("volume", 0) or 0)
    }


def fetch_all_trades(condition_id: str) -> List[Dict[str, Any]]:
    """
    Paginates through all trades recorded on the market.
    """
    trades = []
    offset = 0
    limit = 1000

    print(f"[*] Fetching trade records from Polymarket Data API...")
    while True:
        url = f"{DATA_API_BASE}/trades"
        params = {"market": condition_id, "limit": limit, "offset": offset}
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code != 200:
                print(f"[!] Warning: Data API returned HTTP {r.status_code} at offset {offset}")
                break
            batch = r.json()
            if not batch or not isinstance(batch, list):
                break
            trades.extend(batch)
            print(f"    -> Pulled {len(trades):,} trades so far...", end="\r", flush=True)
            if len(batch) < limit:
                break
            offset += len(batch)
        except Exception as e:
            print(f"\n[!] Error during trade fetching: {e}")
            break

    print(f"\n[+] Total trades retrieved: {len(trades):,}")
    return trades


def fetch_holders(condition_id: str) -> List[Dict[str, Any]]:
    """
    Queries current position holders for the market.
    """
    url = f"{DATA_API_BASE}/holders"
    try:
        r = requests.get(url, params={"market": condition_id}, timeout=15)
        if r.status_code == 200 and isinstance(r.json(), list):
            return r.json()
    except Exception:
        pass
    return []


def analyze_wallets(market_meta: Dict[str, Any], trades: List[Dict[str, Any]], holders_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregates cash inflows, outflows, share balances, and computes PnL and ROI per wallet.
    """
    prices = market_meta["outcome_prices"]
    outcomes = market_meta["outcomes"]
    is_closed = market_meta["is_closed"]

    wallets: Dict[str, Dict[str, Any]] = {}

    def get_wallet_entry(addr: str) -> Dict[str, Any]:
        if addr not in wallets:
            wallets[addr] = {
                "wallet": addr,
                "name": "",
                "buys_usd": 0.0,
                "sells_usd": 0.0,
                "shares": {i: 0.0 for i in range(len(outcomes))},
                "trades_count": 0,
                "trades_list": [],
            }
        return wallets[addr]

    # Process all trade executions
    for t in trades:
        addr = t.get("proxyWallet")
        if not addr:
            continue
        entry = get_wallet_entry(addr)
        entry["trades_count"] += 1
        
        # Capture username or pseudonym if available
        if not entry["name"]:
            name = t.get("name") or t.get("pseudonym") or ""
            if name:
                entry["name"] = name

        side = t.get("side", "").upper()
        size = float(t.get("size", 0) or 0)
        price = float(t.get("price", 0) or 0)
        outcome_idx = int(t.get("outcomeIndex", 0) or 0)
        cost = size * price

        entry["trades_list"].append({
            "side": side,
            "size": size,
            "price": price,
            "outcome_idx": outcome_idx,
            "cost": cost
        })

        if side == "BUY":
            entry["buys_usd"] += cost
            entry["shares"][outcome_idx] = entry["shares"].get(outcome_idx, 0.0) + size
        elif side == "SELL":
            entry["sells_usd"] += cost
            entry["shares"][outcome_idx] = entry["shares"].get(outcome_idx, 0.0) - size

    # Supplement profile names and balances from holders endpoint if any
    for group in holders_data:
        for h in group.get("holders", []):
            addr = h.get("proxyWallet")
            if not addr:
                continue
            entry = get_wallet_entry(addr)
            if not entry["name"]:
                entry["name"] = h.get("name") or h.get("pseudonym") or ""

    # Determine winning outcome if market is closed
    winning_idx = None
    if is_closed and len(prices) > 0:
        winning_idx = prices.index(max(prices))

    # Compute PnL, ROI, Win Rate, and Category
    results = []
    for addr, data in wallets.items():
        # Current or payout value of remaining shares
        current_value = 0.0
        for idx, count in data["shares"].items():
            if idx < len(prices):
                outcome_price = prices[idx]
                current_value += max(0.0, count) * outcome_price

        total_value = data["sells_usd"] + current_value
        net_pnl = total_value - data["buys_usd"]
        invested = data["buys_usd"]

        if invested > 0:
            roi = (net_pnl / invested) * 100.0
        else:
            roi = 0.0

        if net_pnl > 0.01:
            category = "PROFITABLE"
        elif net_pnl < -0.01:
            category = "UNPROFITABLE"
        else:
            category = "BREAKEVEN"

        # Win Rate calculation
        wins = 0
        tr_list = data["trades_list"]
        total_tr = len(tr_list)
        total_vol = data["buys_usd"] + data["sells_usd"]
        avg_trade_size = (total_vol / total_tr) if total_tr > 0 else 0.0

        for tr in tr_list:
            idx = tr["outcome_idx"]
            if is_closed and winning_idx is not None:
                if idx == winning_idx:
                    wins += 1
            else:
                # Active market: winning if buy is in profit or sell had price >= 0.5
                cur_p = prices[idx] if idx < len(prices) else 0.5
                if tr["side"] == "BUY" and cur_p >= tr["price"]:
                    wins += 1
                elif tr["side"] == "SELL" and tr["price"] >= 0.5:
                    wins += 1

        win_rate = (wins / total_tr * 100.0) if total_tr > 0 else 0.0

        results.append({
            "wallet": addr,
            "name": data["name"],
            "pnl": net_pnl,
            "roi": roi,
            "invested": invested,
            "sells": data["sells_usd"],
            "current_value": current_value,
            "trades": total_tr,
            "wins": wins,
            "win_rate": win_rate,
            "avg_trade_size": avg_trade_size,
            "category": category,
        })

    return results


def fetch_lifetime_profiles(
    wallets_data: List[Dict[str, Any]], 
    limit: Optional[int] = None, 
    max_workers: int = 30
) -> List[Dict[str, Any]]:
    """
    Fetches each wallet's overall lifetime Polymarket history (both closed and open positions),
    calculating their true Lifetime P&L across all markets, lifetime win rate,
    lifetime total bets, and average bet size.
    """
    candidates = wallets_data
    if limit is not None and limit > 0 and len(candidates) > limit:
        # Prioritize by match activity/volume if limit is set
        candidates = sorted(candidates, key=lambda x: (x["trades"], abs(x["pnl"])), reverse=True)[:limit]

    total_count = len(candidates)
    print(f"\n[*] Scanning lifetime Polymarket track records across all markets for {total_count:,} wallets...")

    completed = 0

    def get_wallet_lifetime(wallet_info):
        nonlocal completed
        w = wallet_info["wallet"]
        try:
            # 1. Query authoritative Polymarket v2 user stats
            r_stats = requests.get(
                f"{DATA_API_BASE}/v2/user-stats",
                params={"user": w},
                timeout=8
            )
            v2_data = r_stats.json().get("data") if r_stats.status_code == 200 and isinstance(r_stats.json(), dict) else None
            pnl_data = v2_data.get("all_time_pnl") if v2_data else None

            # 2. Query closed positions (up to 100) for win rate & avg bet size
            r_closed = requests.get(
                f"{DATA_API_BASE}/closed-positions",
                params={"user": w, "limit": 100},
                timeout=8
            )
            closed_pos = r_closed.json() if r_closed.status_code == 200 and isinstance(r_closed.json(), list) else []

            # 3. Query active open positions
            r_open = requests.get(
                f"{DATA_API_BASE}/positions",
                params={"user": w, "sizeThreshold": 0, "limit": 50},
                timeout=8
            )
            open_pos = r_open.json() if r_open.status_code == 200 and isinstance(r_open.json(), list) else []

            # Determine Lifetime P&L using Polymarket's official headline PnL metric
            if pnl_data and ("trade_pnl" in pnl_data or "realized_pnl" in pnl_data):
                trade_pnl = pnl_data.get("trade_pnl")
                if trade_pnl is not None:
                    lifetime_pnl = float(trade_pnl)
                else:
                    realized = float(pnl_data.get("realized_pnl", 0) or 0)
                    unrealized = float(pnl_data.get("unrealized_pnl", 0) or 0)
                    lifetime_pnl = realized + unrealized
                total_positions = int(v2_data.get("trades", 0) or (len(closed_pos) + len(open_pos)))
            else:
                # Fallback for new unindexed wallets: calculate from positions
                closed_pnl = sum(float(p.get("realizedPnl", 0) or 0) for p in closed_pos)
                open_pnl = sum(float(p.get("cashPnl", 0) or 0) for p in open_pos)
                total_positions = len(closed_pos) + len(open_pos)
                if total_positions > 0:
                    lifetime_pnl = closed_pnl + open_pnl
                else:
                    lifetime_pnl = wallet_info.get("pnl", 0.0)
                    total_positions = 1

            # Compute win rate and average bet size from closed positions sample
            all_sample = closed_pos + open_pos
            if all_sample:
                closed_wins = sum(1 for p in closed_pos if float(p.get("realizedPnl", 0) or 0) > 0.01)
                open_wins = sum(1 for p in open_pos if float(p.get("cashPnl", 0) or 0) > 0.01)
                sample_wins = closed_wins + open_wins
                sample_count = len(all_sample)
                lifetime_win_rate = (sample_wins / sample_count * 100.0) if sample_count > 0 else 0.0

                sample_invested = sum((float(p.get("totalBought", 0) or 0) * float(p.get("avgPrice", 0) or 0)) for p in closed_pos) + sum(float(p.get("initialValue", 0) or 0) for p in open_pos)
                avg_bet_size = (sample_invested / sample_count) if sample_count > 0 else 0.0

                lifetime_wins = int(round((lifetime_win_rate / 100.0) * total_positions))
                lifetime_losses = max(0, total_positions - lifetime_wins)
            else:
                match_pnl = wallet_info.get("pnl", 0.0)
                lifetime_win_rate = 100.0 if match_pnl > 0.01 else 0.0
                avg_bet_size = wallet_info.get("invested", 0.0)
                lifetime_wins = 1 if match_pnl > 0.01 else 0
                lifetime_losses = 1 if match_pnl < -0.01 else 0

            # Detect if wallet is a Bot vs No Bot (Human)
            maker_rebate = float(pnl_data.get("maker_rebate", 0) or 0) if pnl_data else 0.0
            taker_rebate = float(pnl_data.get("taker_rebate", 0) or 0) if pnl_data else 0.0
            lifetime_trades_count = int(pnl_data.get("trade_count", 0) or 0) if pnl_data else 0
            match_trades = wallet_info.get("trades", 0)

            bot_signals = []
            if maker_rebate > 5.0 or taker_rebate > 20.0:
                bot_signals.append(f"Market Maker Rebates (${maker_rebate:.0f})")
            if lifetime_trades_count >= 2500:
                bot_signals.append(f"High Trades ({lifetime_trades_count:,})")
            elif total_positions >= 800:
                bot_signals.append(f"High Markets ({total_positions:,})")
            elif lifetime_trades_count >= 500 and (lifetime_trades_count / max(1, total_positions)) > 15:
                bot_signals.append(f"Automated Laddering ({lifetime_trades_count/total_positions:.1f} tr/mkt)")
            elif match_trades >= 30:
                bot_signals.append(f"Rapid Match Trades ({match_trades})")

            is_bot = len(bot_signals) > 0
            bot_label = "BOT" if is_bot else "NO BOT"
            bot_detail = "; ".join(bot_signals) if is_bot else "Manual Human Trader"

            if lifetime_pnl > 0.01:
                lifetime_category = "PROFITABLE"
            elif lifetime_pnl < -0.01:
                lifetime_category = "UNPROFITABLE"
            else:
                lifetime_category = "BREAKEVEN"

            return {
                "wallet": w,
                "name": wallet_info.get("name", "") or (v2_data.get("user_name") if v2_data else ""),
                "is_bot": is_bot,
                "bot_label": bot_label,
                "bot_detail": bot_detail,
                "lifetime_pnl": lifetime_pnl,
                "lifetime_category": lifetime_category,
                "lifetime_positions": total_positions,
                "lifetime_wins": lifetime_wins,
                "lifetime_losses": lifetime_losses,
                "lifetime_win_rate": lifetime_win_rate,
                "avg_bet_size": avg_bet_size,
                "match_pnl": wallet_info.get("pnl", 0.0),
                "match_roi": wallet_info.get("roi", 0.0),
                "match_invested": wallet_info.get("invested", 0.0),
                "match_sells": wallet_info.get("sells", 0.0),
                "match_trades": match_trades,
                "match_category": wallet_info.get("category", "BREAKEVEN"),
            }
        except Exception:
            match_pnl = wallet_info.get("pnl", 0.0)
            return {
                "wallet": w,
                "name": wallet_info.get("name", ""),
                "is_bot": False,
                "bot_label": "NO BOT",
                "bot_detail": "Manual Human Trader",
                "lifetime_pnl": match_pnl,
                "lifetime_category": "PROFITABLE" if match_pnl > 0.01 else ("UNPROFITABLE" if match_pnl < -0.01 else "BREAKEVEN"),
                "lifetime_positions": 1,
                "lifetime_wins": 1 if match_pnl > 0.01 else 0,
                "lifetime_losses": 1 if match_pnl < -0.01 else 0,
                "lifetime_win_rate": 100.0 if match_pnl > 0.01 else 0.0,
                "avg_bet_size": wallet_info.get("invested", 0.0),
                "match_pnl": match_pnl,
                "match_roi": wallet_info.get("roi", 0.0),
                "match_invested": wallet_info.get("invested", 0.0),
                "match_sells": wallet_info.get("sells", 0.0),
                "match_trades": wallet_info.get("trades", 0),
                "match_category": wallet_info.get("category", "BREAKEVEN"),
            }
        finally:
            completed += 1
            if completed % 10 == 0 or completed == total_count:
                print(f"    -> Scanned {completed:,} / {total_count:,} wallets ({(completed/total_count*100):.1f}%)...", end="\r", flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = [p for p in executor.map(get_wallet_lifetime, candidates) if p]

    print(f"\n[+] Successfully scanned lifetime track records for {len(results):,} wallets.")
    return results


def print_dashboard(market_meta: Dict[str, Any], results: List[Dict[str, Any]], args: argparse.Namespace):
    """
    Renders the market summary box and top ranked wallets table sorted by Lifetime P&L.
    """
    total_wallets = len(results)
    if total_wallets == 0:
        print("[!] No wallet activity found for this market.")
        return

    # Fetch lifetime profiles for all wallets (or top scan_limit if specified)
    scan_limit = getattr(args, "scan_limit", None)
    lifetime_profiles = fetch_lifetime_profiles(results, limit=scan_limit)

    profitable_list = [r for r in lifetime_profiles if r["lifetime_category"] == "PROFITABLE"]
    unprofitable_list = [r for r in lifetime_profiles if r["lifetime_category"] == "UNPROFITABLE"]
    breakeven_list = [r for r in lifetime_profiles if r["lifetime_category"] == "BREAKEVEN"]

    total_profit = sum(r["lifetime_pnl"] for r in profitable_list)
    total_loss = sum(r["lifetime_pnl"] for r in unprofitable_list)
    total_volume = sum(r["invested"] + r["sells"] for r in results)

    status_str = "RESOLVED / CLOSED" if market_meta["is_closed"] else "ACTIVE / OPEN"

    print("\n" + "=" * 90)
    print(f" POLYMARKET WALLET SCANNER & LIFETIME P&L REPORT")
    print("=" * 90)
    print(f" Market:    {market_meta['question']}")
    print(f" Status:    {status_str}")
    outcomes_info = ", ".join(f"{o}: {p*100:.1f}¢" for o, p in zip(market_meta["outcomes"], market_meta["outcome_prices"]))
    print(f" Outcomes:  {outcomes_info}")
    print("-" * 90)
    print(f" Total Unique Wallets That Took This Bet:  {total_wallets:,}")
    print(f"   [+] Lifetime Profitable:  {len(profitable_list):,} ({len(profitable_list)/len(lifetime_profiles)*100:.1f}%) | Total Net Gain: +${total_profit:,.2f}")
    print(f"   [-] Lifetime Unprofitable: {len(unprofitable_list):,} ({len(unprofitable_list)/len(lifetime_profiles)*100:.1f}%) | Total Net Loss: -${abs(total_loss):,.2f}")
    print(f"   [=] Lifetime Breakeven:   {len(breakeven_list):,} ({len(breakeven_list)/len(lifetime_profiles)*100:.1f}%)")
    print(f" This Match Traded Volume:                 ${total_volume:,.2f}")
    print("=" * 90)

    # Filter
    filtered = lifetime_profiles
    if args.min_invested > 0:
        filtered = [r for r in filtered if r["match_invested"] >= args.min_invested]

    if args.category == "profitable":
        filtered = [r for r in filtered if r["lifetime_category"] == "PROFITABLE"]
    elif args.category == "unprofitable":
        filtered = [r for r in filtered if r["lifetime_category"] == "UNPROFITABLE"]
    elif args.category == "breakeven":
        filtered = [r for r in filtered if r["lifetime_category"] == "BREAKEVEN"]

    # Filter by bot type
    bot_filter = getattr(args, "bot", "all")
    if bot_filter == "bot":
        filtered = [r for r in filtered if r["is_bot"]]
    elif bot_filter == "nobot":
        filtered = [r for r in filtered if not r["is_bot"]]

    # Sort primarily by Lifetime P&L
    if args.sort == "pnl" or args.sort == "lifetime_pnl":
        filtered.sort(key=lambda x: x["lifetime_pnl"], reverse=True)
    elif args.sort == "losers":
        filtered.sort(key=lambda x: x["lifetime_pnl"], reverse=False)
    elif args.sort == "winrate":
        filtered.sort(key=lambda x: (x["lifetime_win_rate"], x["lifetime_positions"]), reverse=True)
    elif args.sort == "match_pnl":
        filtered.sort(key=lambda x: x["match_pnl"], reverse=True)
    elif args.sort == "avg_bet":
        filtered.sort(key=lambda x: x["avg_bet_size"], reverse=True)
    elif args.sort == "bets":
        filtered.sort(key=lambda x: x["lifetime_positions"], reverse=True)

    top_n = filtered[:args.top]

    print(f"\n 🏆 RANKED WALLETS BY LIFETIME P&L (Showing top {len(top_n)} of {len(filtered)} filtered):")
    header = f"{'Rank':<5} | {'Type':<8} | {'Lifetime Cat':<13} | {'Wallet / Pseudonym':<22} | {'Lifetime P&L':>14} | {'Win %':>8} | {'Lifetime Record':<18} | {'Avg Bet':>10} | {'This Bet P&L':>13}"
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for rank, r in enumerate(top_n, 1):
        name_display = r["name"] if r["name"] else f"{r['wallet'][:6]}...{r['wallet'][-4:]}"
        if len(name_display) > 20:
            name_display = name_display[:17] + "..."

        bot_type = "[BOT]" if r.get("is_bot") else "[NO BOT]"
        lt_sign = '+' if r['lifetime_pnl'] > 0 else ('-' if r['lifetime_pnl'] < 0 else '')
        lt_pnl_str = f"{lt_sign}${abs(r['lifetime_pnl']):,.2f}"
        win_str = f"{r['lifetime_win_rate']:.1f}%"
        rec_str = f"{r['lifetime_wins']}w / {r['lifetime_positions']} mkts"
        avg_bet_str = f"${r['avg_bet_size']:,.2f}"
        m_sign = '+' if r['match_pnl'] > 0 else ('-' if r['match_pnl'] < 0 else '')
        m_pnl_str = f"{m_sign}${abs(r['match_pnl']):,.2f}"
        cat_badge = r["lifetime_category"]

        print(f"{rank:<5} | {bot_type:<8} | {cat_badge:<13} | {name_display:<22} | {lt_pnl_str:>14} | {win_str:>8} | {rec_str:<18} | {avg_bet_str:>10} | {m_pnl_str:>13}")

    print("-" * len(header))

    # Dedicated Section: Lifetime High Win Rate Sharps (Across ALL Polymarket history)
    min_pos = getattr(args, "min_trades", 10) or 10
    max_avg_bet = getattr(args, "max_avg_bet", 0.0) or 0.0

    snipers = [p for p in lifetime_profiles if p["lifetime_positions"] >= min_pos]
    if max_avg_bet > 0:
        snipers = [p for p in snipers if p["avg_bet_size"] <= max_avg_bet]
    if bot_filter == "bot":
        snipers = [p for p in snipers if p["is_bot"]]
    elif bot_filter == "nobot":
        snipers = [p for p in snipers if not p["is_bot"]]

    # Sort primarily by lifetime win rate, then lowest average bet size
    snipers.sort(key=lambda x: (x["lifetime_win_rate"], -x["avg_bet_size"]), reverse=True)

    print(f"\n" + "=" * 115)
    print(f" 🎯 LIFETIME HIGH WIN % SHARPS (Across ALL Polymarket History, Min {min_pos} Markets)")
    print(f" Evaluates their overall track record across all bets, highlighting low-amount sharps")
    print("=" * 115)

    if not snipers:
        print(f" No wallets found matching criteria.")
    else:
        s_header = f"{'Rank':<5} | {'Type':<8} | {'Lifetime Win %':<16} | {'Lifetime Record':<20} | {'Avg Bet Size':>13} | {'Lifetime P&L':>14} | {'This Bet P&L':>13} | {'Wallet / Pseudonym':<20}"
        print("-" * len(s_header))
        print(s_header)
        print("-" * len(s_header))

        for s_rank, s in enumerate(snipers[:args.top], 1):
            s_name = s["name"] if s["name"] else f"{s['wallet'][:6]}...{s['wallet'][-4:]}"
            if len(s_name) > 18:
                s_name = s_name[:15] + "..."
            s_bot_type = "[BOT]" if s.get("is_bot") else "[NO BOT]"
            s_lt_sign = '+' if s['lifetime_pnl'] > 0 else ('-' if s['lifetime_pnl'] < 0 else '')
            lt_pnl_fmt = f"{s_lt_sign}${abs(s['lifetime_pnl']):,.2f}"
            s_m_sign = '+' if s['match_pnl'] > 0 else ('-' if s['match_pnl'] < 0 else '')
            m_pnl_fmt = f"{s_m_sign}${abs(s['match_pnl']):,.2f}"
            win_pct_fmt = f"{s['lifetime_win_rate']:.1f}%"
            wins_fmt = f"{s['lifetime_wins']} wins / {s['lifetime_positions']} mkts"
            avg_bet_fmt = f"${s['avg_bet_size']:,.2f}"

            print(f"{s_rank:<5} | {s_bot_type:<8} | {win_pct_fmt:<16} | {wins_fmt:<20} | {avg_bet_fmt:>13} | {lt_pnl_fmt:>14} | {m_pnl_fmt:>13} | {s_name:<20}")

        print("-" * len(s_header))

    # Export CSV if requested
    if args.csv:
        export_csv(args.csv, filtered)

    # Export JSON if requested
    if args.json:
        export_json(args.json, filtered)


def export_csv(filepath: str, rows: List[Dict[str, Any]]):
    import csv
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Rank", 
            "Trader_Type",
            "Bot_Signals",
            "Lifetime_Category", 
            "Wallet", 
            "Pseudonym", 
            "Lifetime_PnL_USD", 
            "Lifetime_WinRate_Pct", 
            "Lifetime_Wins", 
            "Lifetime_Losses", 
            "Lifetime_Total_Bets", 
            "Avg_Bet_Size_USD", 
            "This_Bet_PnL_USD", 
            "This_Bet_ROI_Pct", 
            "This_Bet_Invested_USD", 
            "This_Bet_Trades"
        ])
        for idx, r in enumerate(rows, 1):
            writer.writerow([
                idx,
                r.get("bot_label", "NO BOT"),
                r.get("bot_detail", ""),
                r.get("lifetime_category", ""),
                r["wallet"],
                r.get("name", ""),
                f"{r.get('lifetime_pnl', 0.0):.4f}",
                f"{r.get('lifetime_win_rate', 0.0):.2f}",
                r.get("lifetime_wins", 0),
                r.get("lifetime_losses", 0),
                r.get("lifetime_positions", 0),
                f"{r.get('avg_bet_size', 0.0):.2f}",
                f"{r.get('match_pnl', 0.0):.4f}",
                f"{r.get('match_roi', 0.0):.2f}",
                f"{r.get('match_invested', 0.0):.4f}",
                r.get("match_trades", 0)
            ])
    print(f"\n[+] Successfully exported {len(rows)} records to CSV: {filepath}")


def export_json(filepath: str, rows: List[Dict[str, Any]]):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    print(f"\n[+] Successfully exported {len(rows)} records to JSON: {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Scan all wallets that took a Polymarket bet, compute their Lifetime P&L, categorize into Profitable/Unprofitable, and rank them."
    )
    parser.add_argument("market", help="Polymarket URL, slug (e.g. 'kraken-ipo-in-2025'), or condition ID (0x...)")
    parser.add_argument("--submarket", type=int, default=None, help="1-indexed submarket number if the event has multiple questions")
    parser.add_argument("--top", type=int, default=25, help="Number of wallets to display in the table (default: 25)")
    parser.add_argument("--sort", choices=["pnl", "lifetime_pnl", "losers", "winrate", "match_pnl", "avg_bet", "bets"], default="lifetime_pnl", help="Ranking criteria (default: lifetime_pnl)")
    parser.add_argument("--category", choices=["all", "profitable", "unprofitable", "breakeven"], default="all", help="Filter by lifetime wallet category (default: all)")
    parser.add_argument("--bot", choices=["all", "bot", "nobot"], default="all", help="Filter by trader type: all, bot, or nobot (default: all)")
    parser.add_argument("--min-invested", type=float, default=0.0, help="Minimum invested USD threshold in this match (default: 0)")
    parser.add_argument("--min-trades", type=int, default=10, help="Minimum lifetime positions/markets on Polymarket for high-accuracy sharps (default: 10)")
    parser.add_argument("--max-avg-bet", type=float, default=0.0, help="Filter for low bet sizes, e.g. 100 for avg bet under $100")
    parser.add_argument("--scan-limit", type=int, default=None, help="Number of wallets to scan for lifetime profiles (default: all wallets)")
    parser.add_argument("--csv", type=str, default=None, help="Save the full ranked analysis to a CSV file")
    parser.add_argument("--json", type=str, default=None, help="Save the full ranked analysis to a JSON file")

    args = parser.parse_args()

    id_type, id_val = extract_identifier(args.market)
    try:
        market_meta = resolve_market(id_type, id_val, submarket_idx=args.submarket)
    except Exception as e:
        print(f"[!] Error resolving market: {e}")
        sys.exit(1)

    trades = fetch_all_trades(market_meta["conditionId"])
    holders = fetch_holders(market_meta["conditionId"])
    results = analyze_wallets(market_meta, trades, holders)

    print_dashboard(market_meta, results, args)


if __name__ == "__main__":
    main()
