"""
Quick tests of the Roblox and Steam APIs used in the GameTrend Radar project.

Every test function makes one (or a few) calls and saves the raw response
to ./responses/<name>_response.json.

Usage:
  python api_test.py                 # run all tests
  python api_test.py roblox_games    # run selected tests
  python api_test.py --list          # list available tests

Only the Python standard library is used.
"""

import json
import sys
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

OUT_DIR = Path(__file__).parent / "responses"
HEADERS = {"User-Agent": "GameTrendRadar-student-project/0.1 (API test)"}
SESSION_ID = str(uuid.uuid4())  # Roblox explore/search endpoints need any UUID

# example IDs used by the tests
ROBLOX_UNIVERSE_IDS = [1686885941, 383310974, 994732206, 7436755782]  # Brookhaven, Adopt Me!, Blox Fruits, Grow a Garden
STEAM_APP_IDS = [730, 570, 3241660]  # Counter-Strike 2, Dota 2, R.E.P.O.


# ---------------------------------------------------------------- helpers

def get_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code} for {url}")
        raise
    elapsed_ms = (time.time() - started) * 1000
    print(f"  GET {url[:110]}  ->  {status}, {len(body) / 1024:.1f} kB, {elapsed_ms:.0f} ms")
    return json.loads(body)


def save_json(name, data):
    OUT_DIR.mkdir(exist_ok=True)
    path = OUT_DIR / f"{name}_response.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  saved {path.name}")


def ids_param(ids):
    return ",".join(str(i) for i in ids)


# ---------------------------------------------------------------- Roblox

def roblox_sorts(country="pl"):
    """Discover page: ranking lists (top trending, up-and-coming, ...) for one country, first page."""
    url = (f"https://apis.roblox.com/explore-api/v1/get-sorts"
           f"?sessionId={SESSION_ID}&device=computer&country={country}")
    data = get_json(url)
    save_json("roblox_sorts", data)
    print(f"  next page token present: {bool(data.get('nextSortsPageToken'))}")


def roblox_search(query="grow a"):
    """Search games by text (used to find Roblox clones of Steam games)."""
    url = (f"https://apis.roblox.com/search-api/omni-search"
           f"?searchQuery={urllib.parse.quote(query)}&sessionId={SESSION_ID}&pageType=all")
    save_json("roblox_search", get_json(url))


def roblox_games(universe_ids=ROBLOX_UNIVERSE_IDS):
    """Core game snapshot: players now, visits, favourites, genre, created/updated (max 50 IDs per call)."""
    url = f"https://games.roblox.com/v1/games?universeIds={ids_param(universe_ids)}"
    save_json("roblox_games", get_json(url))


def roblox_votes(universe_ids=ROBLOX_UNIVERSE_IDS):
    """Up/down votes (max 50 IDs per call)."""
    url = f"https://games.roblox.com/v1/games/votes?universeIds={ids_param(universe_ids)}"
    save_json("roblox_votes", get_json(url))


def roblox_badges(universe_id=ROBLOX_UNIVERSE_IDS[3]):
    """Badges of one game; pastDayAwardedCount of the 'join' badge ~ new players per day."""
    url = f"https://badges.roblox.com/v1/universes/{universe_id}/badges?limit=100&sortOrder=Asc"
    save_json("roblox_badges", get_json(url))


def roblox_recommendations(universe_id=ROBLOX_UNIVERSE_IDS[3]):
    """'Players also play' list for one game."""
    url = f"https://games.roblox.com/v1/games/recommendations/game/{universe_id}?maxRows=20"
    save_json("roblox_recommendations", get_json(url))


# ---------------------------------------------------------------- Steam

def steam_most_played():
    """Top 100 games by daily peak players (no key)."""
    url = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
    save_json("steam_most_played", get_json(url))


def steam_current_players(app_ids=STEAM_APP_IDS):
    """Concurrent players, one call per game (no key)."""
    results = {}
    for app_id in app_ids:
        url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
        results[app_id] = get_json(url)
        time.sleep(0.5)
    save_json("steam_current_players", results)


def steam_store_search(store_filter="popularnew", count=100):
    """Store search list (popularnew / topsellers). The game list is HTML inside the JSON (results_html)."""
    url = (f"https://store.steampowered.com/search/results/"
           f"?filter={store_filter}&start=0&count={count}&infinite=1&cc=us&l=english")
    data = get_json(url)
    save_json("steam_store_search", data)
    print(f"  total_count in store: {data.get('total_count')}")


def steam_appdetails(app_id=STEAM_APP_IDS[2]):
    """Store metadata of one game: release date, genres, categories, developers..."""
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc=us&l=english"
    save_json("steam_appdetails", get_json(url))


def steamspy_appdetails(app_id=STEAM_APP_IDS[2]):
    """SteamSpy: user tags with vote counts, previous-day peak ccu, owners range."""
    url = f"https://steamspy.com/api.php?request=appdetails&appid={app_id}"
    save_json("steamspy_appdetails", get_json(url))


# ---------------------------------------------------------------- runner

TESTS = {
    "roblox_sorts": roblox_sorts,
    "roblox_search": roblox_search,
    "roblox_games": roblox_games,
    "roblox_votes": roblox_votes,
    "roblox_badges": roblox_badges,
    "roblox_recommendations": roblox_recommendations,
    "steam_most_played": steam_most_played,
    "steam_current_players": steam_current_players,
    "steam_store_search": steam_store_search,
    "steam_appdetails": steam_appdetails,
    "steamspy_appdetails": steamspy_appdetails,
}


def main(argv):
    if "--list" in argv:
        print("\n".join(TESTS))
        return
    selected = argv or list(TESTS)
    failed = []
    for name in selected:
        if name not in TESTS:
            print(f"unknown test: {name} (use --list)")
            continue
        print(f"\n[{name}]")
        try:
            TESTS[name]()
        except Exception as e:
            print(f"  FAILED: {e}")
            failed.append(name)
        time.sleep(1)  # be gentle with rate limits
    print(f"\nDone. Output in {OUT_DIR}")
    if failed:
        print("Failed:", ", ".join(failed))


if __name__ == "__main__":
    main(sys.argv[1:])
