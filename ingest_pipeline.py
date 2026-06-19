import os
import json
import sqlite3
import time
from datetime import datetime
from typing import Optional
import requests
import pandas as pd
import soccerdata as sd

# #region agent log
_DEBUG_LOG_PATH = "/Users/lucasolmeta/Desktop/Projects/prediction-cup/.cursor/debug-3b5f00.log"
def _debug_log(location, message, data=None, hypothesis_id=None):
    try:
        os.makedirs(os.path.dirname(_DEBUG_LOG_PATH), exist_ok=True)
        with open(_DEBUG_LOG_PATH, "a") as _f:
            _f.write(json.dumps({
                "sessionId": "3b5f00",
                "timestamp": int(time.time() * 1000),
                "location": location,
                "message": message,
                "data": data or {},
                "hypothesisId": hypothesis_id,
            }) + "\n")
    except Exception:
        pass
# #endregion

def _load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            key, _, value = line.partition("=")
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key.strip(), value)

_load_env()

DB_PATH = "prediction_cup.db"
SCHEMA_PATH = "schema.sql"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "YOUR_API_KEY")

SOCCERDATA_LEAGUE = "INT-World Cup"

# ---------------------------------------------------------------------------
# Shared FBref instance (one per season, avoids re-scraping the schedule)
# ---------------------------------------------------------------------------

_fbref_cache: dict = {}

def _get_fbref(season: int) -> sd.FBref:
    if season not in _fbref_cache:
        _fbref_cache[season] = sd.FBref(leagues=SOCCERDATA_LEAGUE, seasons=str(season))
    return _fbref_cache[season]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_db():
    """Initialises the database using the schema file."""
    _debug_log("ingest_pipeline.py:init_db", "init_db entry", {
        "schema_path": SCHEMA_PATH,
        "schema_exists": os.path.exists(SCHEMA_PATH),
        "cwd": os.getcwd(),
    }, "A")
    with sqlite3.connect(DB_PATH) as conn:
        with open(SCHEMA_PATH, "r") as f:
            conn.executescript(f.read())
        # Migrate existing raw_weather rows that pre-date the data_type column
        try:
            conn.execute(
                "ALTER TABLE raw_weather ADD COLUMN data_type TEXT DEFAULT 'historical'"
            )
        except Exception:
            pass  # column already exists
        # Remove previously stored rows with corrupted IDs (pandas Series.to_string artefacts)
        conn.execute("DELETE FROM raw_team_stats WHERE team_id LIKE '%dtype: object%'")
        conn.execute("DELETE FROM raw_player_stats WHERE player_id LIKE '%dtype: object%'")
        conn.commit()
    print("Database initialised successfully.")


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flattens MultiIndex columns (FBref style) to single-level underscore-joined strings."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(part for part in (str(c).strip() for c in col) if part).strip("_")
            if isinstance(col, tuple) else str(col)
            for col in df.columns
        ]
    return df


def _row_to_dict(row) -> dict:
    """Converts a DataFrame row to a JSON-serialisable dict, handling NaT/NaN."""
    result = {}
    for k, v in row.items():
        key = str(k)
        if hasattr(v, "isoformat"):
            result[key] = v.isoformat()
        elif isinstance(v, float) and pd.isna(v):
            result[key] = None
        elif isinstance(v, (str, int, float, bool, type(None))):
            result[key] = v
        else:
            try:
                if pd.isna(v):
                    result[key] = None
                    continue
            except (TypeError, ValueError):
                pass
            result[key] = str(v)
    return result


# ---------------------------------------------------------------------------
# Step 1 – Discover today's fixtures
# ---------------------------------------------------------------------------

def fetch_today_fixtures(date_str: str, season: int) -> list:
    """Loads the full WC schedule from FBref and filters for today's matches.

    Returns a list of dicts with keys:
        fixture_id, home_team_name, away_team_name, venue, match_date
    """
    _debug_log("fetch_today_fixtures", "start", {"date": date_str, "season": season})
    try:
        fbref = _get_fbref(season)
        sched = fbref.read_schedule().reset_index()
        sched["_date_str"] = pd.to_datetime(sched["date"]).dt.strftime("%Y-%m-%d")
        today_rows = sched[sched["_date_str"] == date_str]
    except Exception as exc:
        print(f"[fetch_today_fixtures] Schedule fetch failed: {exc}")
        return []

    results = []
    with sqlite3.connect(DB_PATH) as conn:
        for _, row in today_rows.iterrows():
            raw_gid = row.get("game_id")
            # FBref only assigns game_id after a match report exists (post kick-off).
            # For upcoming fixtures use a stable composite key instead.
            f_id = (
                str(raw_gid)
                if pd.notna(raw_gid) and str(raw_gid) not in ("nan", "None", "")
                else f"{row.get('home_team', '')}_{row.get('away_team', '')}_{date_str}"
            )
            h_name = str(row["home_team"])
            a_name = str(row["away_team"])
            venue = str(row.get("venue", ""))
            payload = _row_to_dict(row.drop("_date_str", errors="ignore"))

            conn.execute(
                """
                INSERT INTO raw_fixtures (fixture_id, match_date, home_team, away_team, raw_payload, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    match_date=excluded.match_date,
                    raw_payload=excluded.raw_payload,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (f_id, date_str, h_name, a_name, json.dumps(payload)),
            )
            results.append({
                "fixture_id": f_id,
                "home_team_name": h_name,
                "away_team_name": a_name,
                "venue": venue,
                "match_date": date_str,
            })
        conn.commit()

    print(f"[fetch_today_fixtures] Found {len(results)} fixture(s) for {date_str}.")
    return results


# ---------------------------------------------------------------------------
# Step 2 – Last-5 historical backfill for a specific team
# ---------------------------------------------------------------------------

def fetch_last5_fixtures(team_name: str, season: int) -> list:
    """Returns the last 5 completed WC fixtures for a team from FBref schedule.

    Returns a list of dicts with keys:
        fixture_id, venue_city, match_date, home_team, away_team
    """
    _debug_log("fetch_last5_fixtures", "start", {"team": team_name, "season": season})
    try:
        fbref = _get_fbref(season)
        sched = fbref.read_schedule().reset_index()
    except Exception as exc:
        print(f"[fetch_last5_fixtures] Schedule fetch failed: {exc}")
        return []

    team_games = sched[
        ((sched["home_team"] == team_name) | (sched["away_team"] == team_name))
        & sched["score"].notna()
        & (sched["score"] != "")
    ].copy()

    team_games["_date"] = pd.to_datetime(team_games["date"])
    team_games = team_games.sort_values("_date", ascending=False).head(5)

    results = []
    with sqlite3.connect(DB_PATH) as conn:
        for _, row in team_games.iterrows():
            f_id = str(row["game_id"])
            m_date = row["_date"].strftime("%Y-%m-%d")
            h_name = str(row["home_team"])
            a_name = str(row["away_team"])
            venue = str(row.get("venue", ""))
            payload = _row_to_dict(row.drop(["_date"], errors="ignore"))

            conn.execute(
                """
                INSERT INTO raw_fixtures (fixture_id, match_date, home_team, away_team, raw_payload, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    match_date=excluded.match_date,
                    raw_payload=excluded.raw_payload,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (f_id, m_date, h_name, a_name, json.dumps(payload)),
            )
            results.append({
                "fixture_id": f_id,
                "venue_city": venue,
                "match_date": m_date,
                "home_team": h_name,
                "away_team": a_name,
            })
        conn.commit()

    print(f"[fetch_last5_fixtures] {team_name}: {len(results)} historical fixture(s).")
    return results


# ---------------------------------------------------------------------------
# Step 2b – Team & player stats for a historical fixture
# ---------------------------------------------------------------------------

def ingest_match_performance(
    fixture_id: str,
    home_team: str,
    away_team: str,
    match_date: str,
    season: int,
):
    """Harvests team discipline stats (fouls, cards) and per-player stats for a fixture.

    Team stats use read_team_match_stats(stat_type='misc') filtered by date.
    Player stats use read_player_match_stats(match_id=fixture_id) for a targeted
    single-game fetch — only downloads one match page instead of the full season.
    """
    _debug_log(
        "ingest_match_performance",
        "start",
        {"fixture_id": fixture_id, "home": home_team, "away": away_team, "date": match_date},
    )
    fbref = _get_fbref(season)

    with sqlite3.connect(DB_PATH) as conn:
        # -- Team discipline stats (fouls, cards, offsides, etc.) --
        try:
            ts = fbref.read_team_match_stats(
                stat_type="misc",
                team=[home_team, away_team],
            )
            ts_reset = _flatten_columns(ts.reset_index())
            ts_reset["_date_str"] = pd.to_datetime(ts_reset["date"]).dt.strftime("%Y-%m-%d")
            game_ts = ts_reset[ts_reset["_date_str"] == match_date].drop(
                columns=["_date_str"], errors="ignore"
            )

            for _, row in game_ts.iterrows():
                t_id = str(row.get("team", f"{fixture_id}_unknown"))
                payload = _row_to_dict(row)
                conn.execute(
                    """
                    INSERT INTO raw_team_stats (fixture_id, team_id, raw_payload, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(fixture_id, team_id) DO UPDATE SET
                        raw_payload=excluded.raw_payload,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (fixture_id, t_id, json.dumps(payload)),
                )
            print(f"[ingest_match_performance] Fixture {fixture_id}: {len(game_ts)} team misc rows stored.")
        except Exception as exc:
            print(f"[ingest_match_performance] Team misc stats error for {fixture_id}: {exc}")

        # -- Team shooting stats (shots, SoT, xG, etc.) --
        try:
            ts_sh = fbref.read_team_match_stats(
                stat_type="shooting",
                team=[home_team, away_team],
            )
            ts_sh_reset = _flatten_columns(ts_sh.reset_index())
            ts_sh_reset["_date_str"] = pd.to_datetime(ts_sh_reset["date"]).dt.strftime("%Y-%m-%d")
            game_ts_sh = ts_sh_reset[ts_sh_reset["_date_str"] == match_date].drop(
                columns=["_date_str"], errors="ignore"
            )

            for _, row in game_ts_sh.iterrows():
                team_name_val = str(row.get("team", f"{fixture_id}_unknown"))
                t_id = f"{team_name_val}__shooting"
                payload = _row_to_dict(row)
                conn.execute(
                    """
                    INSERT INTO raw_team_stats (fixture_id, team_id, raw_payload, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(fixture_id, team_id) DO UPDATE SET
                        raw_payload=excluded.raw_payload,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (fixture_id, t_id, json.dumps(payload)),
                )
            print(f"[ingest_match_performance] Fixture {fixture_id}: {len(game_ts_sh)} team shooting rows stored.")
        except Exception as exc:
            print(f"[ingest_match_performance] Team shooting stats error for {fixture_id}: {exc}")

        # -- Per-player stats (targeted single-game fetch via match_id) --
        try:
            ps = fbref.read_player_match_stats(match_id=fixture_id, stat_type="summary")
            ps_reset = _flatten_columns(ps.reset_index())

            for _, row in ps_reset.iterrows():
                p_name = str(row.get("player", "unknown"))
                p_team = str(row.get("team", "unknown"))
                p_id = f"{p_name}__{p_team}"
                payload = _row_to_dict(row)
                conn.execute(
                    """
                    INSERT INTO raw_player_stats (fixture_id, player_id, raw_payload, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(fixture_id, player_id) DO UPDATE SET
                        raw_payload=excluded.raw_payload,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (fixture_id, p_id, json.dumps(payload)),
                )
            print(f"[ingest_match_performance] Fixture {fixture_id}: {len(ps_reset)} player rows stored.")
        except Exception as exc:
            print(f"[ingest_match_performance] Player stats error for {fixture_id}: {exc}")

        conn.commit()


# ---------------------------------------------------------------------------
# Step 3 – Weather harvesting via Open-Meteo (no auth required)
# ---------------------------------------------------------------------------

# Static lookup for 2026 FIFA World Cup venues whose stadium names are not
# resolvable by the Open-Meteo geocoding API. Values are the host city name
# used as the geocoding query instead.
_WC2026_VENUE_CITIES: dict = {
    "MetLife Stadium": "New York",
    "AT&T Stadium": "Dallas",
    "SoFi Stadium": "Los Angeles",
    "Levi's Stadium": "San Jose",
    "Rose Bowl Stadium": "Pasadena",
    "Gillette Stadium": "Boston",
    "Hard Rock Stadium": "Miami",
    "NRG Stadium": "Houston",
    "BC Place Stadium": "Vancouver",
    "BMO Field": "Toronto",
    "Estadio Akron": "Guadalajara",
    "Estadio Banorte": "Culiacán",
    "Estadio BBVA": "Monterrey",
    "Estadio Azteca": "Mexico City",
    "Q2 Stadium": "Austin",
    "Arrowhead Stadium": "Kansas City",
    "Lincoln Financial Field": "Philadelphia",
    "Empower Field at Mile High": "Denver",
    "Camping World Stadium": "Orlando",
    "Snapdragon Stadium": "San Diego",
    "Lumen Field": "Seattle",
}


def _geocode_city(city: str) -> tuple:
    """Returns (latitude, longitude) for a city/venue name via Open-Meteo geocoding.

    Falls back to a static WC-2026 venue→city map for stadium names that the
    geocoding API cannot resolve.
    """
    if not city:
        return None, None

    # Resolve known WC stadium names to their host city first
    query = _WC2026_VENUE_CITIES.get(city, city)

    url = (
        f"https://geocoding-api.open-meteo.com/v1/search"
        f"?name={requests.utils.quote(query)}&count=1&language=en&format=json"
    )
    try:
        resp = requests.get(url, timeout=10)
        results = resp.json().get("results", [])
        if results:
            return results[0]["latitude"], results[0]["longitude"]
    except Exception as exc:
        _debug_log("_geocode_city", "geocoding failed", {"city": city, "query": query, "error": str(exc)})
    return None, None


def ingest_weather_for_fixture(fixture_id: str, venue_city: str, match_date: str):
    """Fetches historical weather for a fixture's venue via Open-Meteo archive.

    No API key required. Stores temperature, precipitation, and wind speed.
    `venue_city` can be a stadium name — Open-Meteo geocoding resolves most major venues.
    """
    if not venue_city or not match_date:
        print(f"[ingest_weather] Fixture {fixture_id}: missing city or date, skipping.")
        return

    # Strip the "(Neutral Site)" suffix that FBref appends to WC venue names
    clean_venue = venue_city.replace("(Neutral Site)", "").strip()

    lat, lon = _geocode_city(clean_venue)
    if lat is None:
        print(f"[ingest_weather] Could not geocode '{clean_venue}' for fixture {fixture_id}.")
        return

    # Use the standard Open-Meteo forecast API with explicit date range — it
    # supports recent historical dates (up to ~3 months back) on the free tier.
    # archive.open-meteo.com is a paid-tier service.
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={match_date}&end_date={match_date}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
        f"&timezone=UTC"
    )
    _debug_log(
        "ingest_weather_for_fixture",
        "open-meteo request",
        {"fixture_id": fixture_id, "venue": clean_venue, "date": match_date},
    )
    try:
        resp = requests.get(url, timeout=15)
        payload = resp.json()
    except Exception as exc:
        print(f"[ingest_weather] Request failed for fixture {fixture_id}: {exc}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO raw_weather (fixture_id, venue_city, match_date, data_type, raw_payload, updated_at)
            VALUES (?, ?, ?, 'historical', ?, CURRENT_TIMESTAMP)
            """,
            (fixture_id, clean_venue, match_date, json.dumps(payload)),
        )
        conn.commit()
    print(f"[ingest_weather] Fixture {fixture_id}: historical weather stored for '{clean_venue}' on {match_date}.")


def ingest_weather_forecast(fixture_id: str, venue: str, match_date: str):
    """Fetches the day-of weather forecast for a today's fixture venue.

    Requests both a daily summary and hourly breakdown so match-time conditions
    (kick-off temperature, precipitation probability, wind) can be read directly.
    Uses timezone=auto so hourly times are expressed in the venue's local timezone.
    Stored in raw_weather with data_type='forecast'.
    """
    if not venue or not match_date:
        print(f"[ingest_forecast] Fixture {fixture_id}: missing venue or date, skipping.")
        return

    clean_venue = venue.replace("(Neutral Site)", "").strip()

    lat, lon = _geocode_city(clean_venue)
    if lat is None:
        print(f"[ingest_forecast] Could not geocode '{clean_venue}' for fixture {fixture_id}.")
        return

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={match_date}&end_date={match_date}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
        f"windspeed_10m_max,precipitation_probability_max,weathercode"
        f"&hourly=temperature_2m,precipitation,windspeed_10m,"
        f"precipitation_probability,weathercode"
        f"&timezone=auto"
    )
    _debug_log(
        "ingest_weather_forecast",
        "open-meteo forecast request",
        {"fixture_id": fixture_id, "venue": clean_venue, "date": match_date},
    )
    try:
        resp = requests.get(url, timeout=15)
        payload = resp.json()
    except Exception as exc:
        print(f"[ingest_forecast] Request failed for fixture {fixture_id}: {exc}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO raw_weather (fixture_id, venue_city, match_date, data_type, raw_payload, updated_at)
            VALUES (?, ?, ?, 'forecast', ?, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING
            """,
            (fixture_id, clean_venue, match_date, json.dumps(payload)),
        )
        conn.commit()

    daily = payload.get("daily", {})
    tmax   = (daily.get("temperature_2m_max")   or [None])[0]
    precip = (daily.get("precipitation_sum")     or [None])[0]
    wind   = (daily.get("windspeed_10m_max")     or [None])[0]
    pop    = (daily.get("precipitation_probability_max") or [None])[0]
    print(
        f"[ingest_forecast] Fixture {fixture_id} ({clean_venue}): "
        f"{tmax}°C  {precip}mm rain  {wind}km/h wind  {pop}% precip chance"
    )


# ---------------------------------------------------------------------------
# Step 4 – Market & Elo stubs
# ---------------------------------------------------------------------------

def fetch_kalshi_odds(home_team: str, away_team: str) -> dict:
    """Stub: fetch current win/tie market prices from Kalshi for a matchup.

    TODO: Implement real Kalshi API call.
    Markets follow a naming convention such as FIFAWC-{HOME}-{AWAY}.
    Authenticate with: Authorization: Bearer {KALSHI_API_KEY}
    Base URL: https://trading-api.kalshi.com/trade-api/v2
    """
    _debug_log(
        "fetch_kalshi_odds",
        "stub called",
        {"home_team": home_team, "away_team": away_team},
    )
    return {
        "home_win": None,
        "away_win": None,
        "tie": None,
        "source": "kalshi_stub",
        "note": "Not yet implemented – wire KALSHI_API_KEY and market ticker lookup.",
    }


def fetch_team_elo(team_name: str) -> dict:
    """Stub: fetch current Elo rating for a national team.

    TODO: Integrate with World Football Elo Ratings (eloratings.net).
    No public REST API exists; options are:
      - Scrape https://www.eloratings.net/{team_name}
      - Download the community CSV dataset from GitHub:
        https://github.com/martj42/international_results
    Note: ClubElo (soccerdata) covers club teams only, not national teams.
    """
    _debug_log("fetch_team_elo", "stub called", {"team_name": team_name})
    return {
        "team": team_name,
        "elo": None,
        "source": "elo_stub",
        "note": "Not yet implemented – wire eloratings.net or the Martj42 international dataset.",
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_daily_sync():
    """Main execution sequence. Designed for daily cron invocation.

    Flow:
      1. Discover today's World Cup fixtures from FBref (uses cached schedule after first run).
      2. Fetch a day-of weather FORECAST for every today's fixture venue (hourly + daily).
      3. Collect unique team names from today's fixtures.
      4. For each team, backfill the last 5 completed WC matches from the same schedule.
      5. For each historical fixture: harvest team stats, player stats, and historical weather.
      6. Log Kalshi odds and Elo ratings for today's matchups (stubs).

    On first run, FBref uses a headless Chrome scraper (~1-2 min).
    Subsequent same-day runs read from the local cache (~seconds).
    """
    _debug_log("run_daily_sync", "entry", {
        "db_path": DB_PATH,
        "schema_path": SCHEMA_PATH,
        "schema_exists": os.path.exists(SCHEMA_PATH),
    }, "A")

    init_db()

    today = datetime.utcnow().strftime("%Y-%m-%d")
    CURRENT_SEASON = int(os.getenv("FOOTBALL_SEASON", "2026"))

    print(f"\n{'='*60}")
    print(f"  Daily sync starting — {today}")
    print(f"  League: {SOCCERDATA_LEAGUE} | Season: {CURRENT_SEASON}")
    print(f"{'='*60}\n")

    # ------------------------------------------------------------------
    # 1. Today's fixtures
    # ------------------------------------------------------------------
    today_fixtures = fetch_today_fixtures(today, CURRENT_SEASON)

    if not today_fixtures:
        print("No World Cup fixtures scheduled for today. Exiting.")
        return

    # ------------------------------------------------------------------
    # 2. Day-of weather forecast for every today's match venue
    # ------------------------------------------------------------------
    print("Fetching match-day weather forecasts...")
    for fx in today_fixtures:
        ingest_weather_forecast(fx["fixture_id"], fx["venue"], today)

    # ------------------------------------------------------------------
    # 3. Unique team names playing today
    # ------------------------------------------------------------------
    team_names: set = set()
    for fx in today_fixtures:
        team_names.add(fx["home_team_name"])
        team_names.add(fx["away_team_name"])

    print(f"Teams active today: {sorted(team_names)}")

    # ------------------------------------------------------------------
    # 4. Last-5 historical backfill per team
    # ------------------------------------------------------------------
    historical: list = []
    seen_fixture_ids: set = set()

    for team_name in sorted(team_names):
        for fx_info in fetch_last5_fixtures(team_name, CURRENT_SEASON):
            if fx_info["fixture_id"] not in seen_fixture_ids:
                seen_fixture_ids.add(fx_info["fixture_id"])
                historical.append(fx_info)

    print(f"\nHistorical fixtures to backfill: {len(historical)}")

    # ------------------------------------------------------------------
    # 5. Per-fixture: team stats + player stats + historical weather
    # ------------------------------------------------------------------
    for fx_info in historical:
        fid = fx_info["fixture_id"]
        print(f"\n  → Fixture {fid} ({fx_info['venue_city']} | {fx_info['match_date']})")
        ingest_match_performance(
            fixture_id=fid,
            home_team=fx_info["home_team"],
            away_team=fx_info["away_team"],
            match_date=fx_info["match_date"],
            season=CURRENT_SEASON,
        )
        ingest_weather_for_fixture(fid, fx_info["venue_city"], fx_info["match_date"])

    # ------------------------------------------------------------------
    # 6. Kalshi odds + Elo for today's matches
    # ------------------------------------------------------------------
    print("\nFetching market & Elo data for today's fixtures...")
    for fx in today_fixtures:
        odds = fetch_kalshi_odds(fx["home_team_name"], fx["away_team_name"])
        elo_home = fetch_team_elo(fx["home_team_name"])
        elo_away = fetch_team_elo(fx["away_team_name"])
        print(
            f"  {fx['home_team_name']} vs {fx['away_team_name']} — "
            f"Kalshi: {odds['source']} | "
            f"Elo H/A: {elo_home['elo']}/{elo_away['elo']}"
        )

    print(f"\n{'='*60}")
    print(f"  Daily sync complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_daily_sync()
