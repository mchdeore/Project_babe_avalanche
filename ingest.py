"""Unified ingestion: All sources, all bookmakers."""
from __future__ import annotations
import os, re, time
from collections import defaultdict
import requests
from dotenv import load_dotenv
from utils import (canonical_game_id, devig, devig_market, init_db, insert_history, load_config,
                   normalize_team, odds_to_prob, safe_json, upsert_rows, utc_now_iso, within_window)


def fetch_odds_api_games(session, api_key, config):
    """Fetch game odds from ALL configured bookmakers."""
    games, groups = {}, defaultdict(list)
    now = utc_now_iso()
    
    for sport in config["sports"]:
        for mkt in config["markets"]:
            url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
            params = {"apiKey": api_key, "regions": ",".join(config["regions"]),
                      "markets": mkt, "oddsFormat": "decimal", "dateFormat": "iso"}
            resp = session.get(url, params=params, timeout=20)
            data = resp.json() if resp.status_code == 200 else []
            print(f"  {sport}/{mkt}: {len(data)} games")
            time.sleep(config.get("request_delay_seconds", 0))
            
            for game in data:
                home, away, commence = game.get("home_team"), game.get("away_team"), game.get("commence_time")
                if not all([home, away, commence]) or not within_window(commence, config.get("bettable_window_days", 30)):
                    continue
                
                game_id = canonical_game_id(game["sport_key"], home, away, commence[:10])
                games[game_id] = {"game_id": game_id, "league": game["sport_key"], "commence_time": commence,
                                  "home_team": home, "away_team": away, "last_refreshed": now}
                
                for book in game.get("bookmakers", []):
                    if book["key"] not in config["books"]:
                        continue
                    for m in book.get("markets", []):
                        if m["key"] not in config["markets"]:
                            continue
                        for out in m.get("outcomes", []):
                            name, price = out.get("name"), out.get("price")
                            if name is None or price is None:
                                continue
                            
                            if m["key"] == "totals":
                                side, line = name.strip().lower(), out.get("point")
                                if side not in {"over", "under"}:
                                    continue
                            else:
                                o, h, a = normalize_team(name), normalize_team(home), normalize_team(away)
                                if o == h or h in o:
                                    side = "home"
                                elif o == a or a in o:
                                    side = "away"
                                elif o in {"draw", "tie", "x"}:
                                    side = "draw"
                                else:
                                    continue
                                line = out.get("point", 0.0) if m["key"] == "spreads" else 0.0
                            
                            if line is None:
                                continue
                            
                            groups[(game_id, m["key"], float(line), book["key"])].append({
                                "game_id": game_id, "market": m["key"], "side": side, "line": float(line),
                                "source": "odds_api", "provider": book["key"], "price": price,
                                "implied_prob": odds_to_prob(price), "provider_updated_at": book.get("last_update", now),
                                "last_refreshed": now, "snapshot_time": now, "source_event_id": game.get("id"),
                                "source_market_id": None, "outcome": name,
                            })
    
    rows = [r for g in groups.values() for r in devig_market(g)]
    return games, rows


def fetch_odds_api_futures(session, api_key, config):
    """Fetch futures from ALL configured bookmakers."""
    games, rows = {}, []
    now = utc_now_iso()
    
    FUTURES = {
        "basketball_nba_championship_winner": "NBA Championship",
        "icehockey_nhl_championship_winner": "NHL Stanley Cup",
    }
    
    for sport_key, name in FUTURES.items():
        print(f"  {name}...", end=" ")
        resp = session.get(f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds",
                           params={"apiKey": api_key, "regions": "us", "oddsFormat": "decimal"}, timeout=15)
        if resp.status_code != 200:
            print("failed")
            continue
        
        futures_id = f"futures_{sport_key}"
        games[futures_id] = {"game_id": futures_id, "league": sport_key, "commence_time": "",
                            "home_team": name, "away_team": "", "last_refreshed": now}
        
        book_count = 0
        for event in resp.json():
            for book in event.get("bookmakers", []):
                if book["key"] not in config["books"]:
                    continue
                book_count += 1
                for mkt in book.get("markets", []):
                    outcomes = mkt.get("outcomes", [])
                    probs = [odds_to_prob(o.get("price", 0)) for o in outcomes]
                    devigged = devig(probs)
                    for i, out in enumerate(outcomes):
                        team = normalize_team(out.get("name", ""))
                        rows.append({
                            "game_id": futures_id, "market": "futures", "side": team, "line": 0.0,
                            "source": "odds_api", "provider": book["key"], "price": out.get("price"),
                            "implied_prob": probs[i], "devigged_prob": devigged[i],
                            "provider_updated_at": book.get("last_update", now), "last_refreshed": now,
                            "snapshot_time": now, "source_event_id": None, "source_market_id": None, "outcome": team,
                        })
        print(f"{book_count} books")
    
    return games, rows


def fetch_polymarket(session):
    """Fetch Polymarket futures (no vig, prices = probabilities)."""
    games, rows = {}, []
    now = utc_now_iso()
    
    all_markets = []
    for offset in range(0, 1000, 100):
        resp = session.get("https://gamma-api.polymarket.com/markets",
                           params={"closed": "false", "limit": 100, "offset": offset}, timeout=15)
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        all_markets.extend(data)
    
    print(f"  Fetched {len(all_markets)} markets")
    
    FUTURES = {
        "futures_basketball_nba_championship_winner": ("nba", "NBA Championship"),
        "futures_icehockey_nhl_championship_winner": ("stanley cup", "NHL Stanley Cup"),
    }
    
    for futures_id, (phrase, name) in FUTURES.items():
        games[futures_id] = {"game_id": futures_id, "league": futures_id.replace("futures_", ""),
                            "commence_time": "", "home_team": name, "away_team": "", "last_refreshed": now}
        teams = []
        for m in all_markets:
            q = (m.get("question") or "").lower()
            if phrase in q and "win" in q:
                match = re.search(r"will (?:the )?(.+?) win", q)
                if not match:
                    continue
                team = normalize_team(match.group(1))
                outcomes = safe_json(m.get("outcomes"))
                prices = safe_json(m.get("outcomePrices"))
                for i, out in enumerate(outcomes):
                    if str(out).lower() == "yes" and i < len(prices):
                        try:
                            price = float(prices[i])
                            teams.append(team)
                            rows.append({
                                "game_id": futures_id, "market": "futures", "side": team, "line": 0.0,
                                "source": "polymarket", "provider": "polymarket", "price": price,
                                "implied_prob": price, "devigged_prob": price,
                                "provider_updated_at": now, "last_refreshed": now, "snapshot_time": now,
                                "source_event_id": m.get("id"), "source_market_id": None, "outcome": team,
                            })
                        except (ValueError, TypeError):
                            pass
                        break
        print(f"  {phrase}: {len(teams)} teams")
    
    return games, rows


def fetch_kalshi(session):
    """Extract game references from Kalshi parlay markets."""
    games, rows = {}, []
    now = utc_now_iso()
    
    all_markets = []
    cursor = None
    for _ in range(20):
        params = {"limit": 100, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        resp = session.get("https://api.elections.kalshi.com/trade-api/v2/markets", params=params, timeout=15)
        if resp.status_code != 200:
            break
        data = resp.json()
        all_markets.extend(data.get("markets", []))
        cursor = data.get("cursor")
        if not cursor or len(data.get("markets", [])) < 100:
            break
    
    print(f"  Fetched {len(all_markets)} markets")
    
    seen = set()
    for m in all_markets:
        for leg in m.get("mve_selected_legs", []):
            ticker = leg.get("market_ticker", "")
            parts = ticker.split("-")
            if len(parts) < 2:
                continue
            match = re.match(r"(\d{2})([A-Z]{3})(\d{2})([A-Z]+)", parts[1])
            if not match or "GAME" not in parts[0]:
                continue
            
            y, mon, d, teams = match.groups()
            months = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
                      "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}
            date = f"20{y}-{months.get(mon, '01')}-{d}"
            t1, t2 = (teams[:3], teams[3:6]) if len(teams) >= 6 else (teams, "")
            selection = parts[2] if len(parts) > 2 else ""
            
            key = (date, t1, t2)
            if key in seen:
                continue
            seen.add(key)
            
            league = "NBA" if "NBA" in parts[0] else "NFL" if "NFL" in parts[0] else "SPORT"
            game_id = f"kalshi_{date}_{t1}_{t2}"
            
            games[game_id] = {"game_id": game_id, "league": league, "commence_time": date,
                             "home_team": t1, "away_team": t2, "last_refreshed": now}
            rows.append({
                "game_id": game_id, "market": "h2h", "side": normalize_team(selection), "line": 0.0,
                "source": "kalshi", "provider": "kalshi", "price": None,
                "implied_prob": None, "devigged_prob": None, "provider_updated_at": now,
                "last_refreshed": now, "snapshot_time": now, "source_event_id": ticker,
                "source_market_id": m.get("ticker"), "outcome": selection,
            })
    
    print(f"  Extracted {len(games)} games")
    return games, rows


def ingest():
    """Run full ingestion from all sources."""
    load_dotenv()
    api_key = os.getenv("ODDS_API_KEY")
    config = load_config()
    conn = init_db(config["storage"]["database"])
    
    all_games, all_rows = {}, []
    
    try:
        with requests.Session() as session:
            print("\n[ODDS API - Games]")
            if api_key:
                g, r = fetch_odds_api_games(session, api_key, config)
                all_games.update(g)
                all_rows.extend(r)
                print(f"  → {len(g)} games, {len(r)} rows")
            else:
                print("  ⚠️ Missing ODDS_API_KEY")
            
            print("\n[ODDS API - Futures]")
            if api_key:
                g, r = fetch_odds_api_futures(session, api_key, config)
                all_games.update(g)
                all_rows.extend(r)
                print(f"  → {len(g)} futures, {len(r)} rows")
            
            print("\n[POLYMARKET]")
            g, r = fetch_polymarket(session)
            all_games.update(g)
            all_rows.extend(r)
            print(f"  → {len(r)} rows")
            
            print("\n[KALSHI]")
            g, r = fetch_kalshi(session)
            all_games.update(g)
            all_rows.extend(r)
            print(f"  → {len(g)} games, {len(r)} rows")
        
        upsert_rows(conn, "games", ["game_id"],
                    ["league", "commence_time", "home_team", "away_team", "last_refreshed"], all_games.values())
        upsert_rows(conn, "market_latest", ["game_id", "market", "side", "line", "source", "provider"],
                    ["price", "implied_prob", "devigged_prob", "provider_updated_at", "last_refreshed",
                     "source_event_id", "source_market_id", "outcome"], all_rows)
        insert_history(conn, all_rows)
        conn.commit()
        
    finally:
        conn.close()
    
    print(f"\n✅ {len(all_games)} markets, {len(all_rows)} rows")


if __name__ == "__main__":
    ingest()
