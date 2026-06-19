CREATE TABLE IF NOT EXISTS raw_fixtures (
    fixture_id TEXT PRIMARY KEY,
    match_date TEXT,
    home_team TEXT,
    away_team TEXT,
    raw_payload TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_team_stats (
    fixture_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    raw_payload TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fixture_id, team_id)
);

CREATE TABLE IF NOT EXISTS raw_player_stats (
    fixture_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    raw_payload TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fixture_id, player_id)
);

CREATE TABLE IF NOT EXISTS raw_market_odds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id TEXT NOT NULL,
    market_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    raw_payload TEXT
);

CREATE TABLE IF NOT EXISTS raw_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id TEXT NOT NULL,
    venue_city TEXT,
    match_date TEXT,
    data_type TEXT DEFAULT 'historical',
    raw_payload TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
