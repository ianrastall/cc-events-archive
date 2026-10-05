#!/usr/bin/env python3
r"""Chess.com events pipeline: raw event PGNs -> catalog -> CTML -> PGN -> published ZIPs.

Lives in pipeline/ of the cc-events-archive repository; README.md beside this
file says what each stage decides. The inbox is only ever READ. Working state
goes to work/ (not committed), and `publish` writes the year folders and
cc_events_manifest.json. Nothing here commits or pushes.

    scan       read new/changed PGNs: dates, rounds, players, ratings, format
    enrich     Elysium lookups by FIDE id: titles and the standard rating for
               the event month; flags Elo tags and dates that cannot be right
    match      find the event among Elysium's event records: place
    tables     find the event's own crosstable: name, dates, place, rounds,
               category, and each player's title, federation, rating and score
    classify   tournament average, FIDE category, the 2200 floor, keep / cull
    ctml       write and validate a CTML document for every kept event
    pgn        write PGN back out of the CTML
    export     work/catalog.csv
    status     print a summary
    run        all of the above, in order

    verify     replay source and regenerated PGN side by side
    bundles    write the three prepared databases (all, 2600+, 2700+); publish does this too
    publish    write <year>/<slug>.zip and the manifest into the working tree

Run with the CTML project's interpreter, which has python-chess and lxml:

    D:\dev\proj\ctml\.venv\Scripts\python.exe pipeline\cc_events.py run
"""
from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import sqlite3
import statistics
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from xml.sax.saxutils import escape as _xml_escape

REPO = Path(__file__).resolve().parent.parent      # the cc-events-archive checkout
HOME = REPO / "work"                                # catalog and generated CTML/PGN; not committed
# Where the downloaded PGNs are. All of these are read, and only read: the
# finished files were moved to the first, the downloader is still writing to
# the second, and the third is the folder named for them beside this repository.
# A file present in two of them is taken from whichever copy is newer.
# Historical tournaments (1834-1989), one PGN per event, cut from a historical
# database with PgnTools' Tour Breaker. They are not Chess.com files: their Elo
# tags are Edo or Chessmetrics ratings and are trusted as they stand, nothing
# is looked up for them, and their dates may be a bare year. Events scanned
# from this folder carry origin = 'historical'.
HISTORICAL_DIR = Path(r"D:\pgn\historical-tours")
INBOXES = [Path(r"D:\dev\proj\cc-events-download"), Path(r"D:\all\cc\events"), REPO.parent / "cc-events",
           HISTORICAL_DIR]
OPENINGS_TSV = Path(r"D:\dev\proj\ctml\assets\all.tsv")   # ECO code and opening name by position
ELYSIUM_DB = Path(r"D:\elysium\db\elysium.db")
CTML_ROOT = Path(r"D:\dev\proj\ctml")
# Parsed TWIC / OlimpBase / chess-results / NWChess crosstables: headers (dates,
# category, average, rounds) and event-time player rows. This file is what the
# source-evidence archive's `ctml-crosstables` observations were made from. It is
# read directly because archive.sqlite is rewritten in place while that archive
# is being deduplicated; a run on 2026-10-01 caught it at a quarter of its rows.
CROSSTABLES_JSON = Path(r"D:\dev\proj\ctml\crosstables\crosstables.json")
CATALOG = HOME / "catalog.sqlite"
CTML_OUT = HOME / "ctml"
PGN_OUT = HOME / "pgn"

VERSION = "cc-events-pipeline/5"
CTML_NS = "urn:ctml:2.0"
KEEP_AVERAGE = 2300          # ChessBase's "strong tournament" line
RATING_FLOOR = 2200          # and no participant below this ...
FLOOR_EXEMPT_PLAYERS = 200   # ... unless the field is larger than this (Olympiads, World Cups, big opens)
ELYSIUM_LAST_MONTH = "2025-12"   # last monthly list in elysium.db; later events lean on their Elo tags
MIN_RATED_SHARE = 0.5        # below this the average is not trusted -> review
SETTLE_SECONDS = 120         # ignore files the downloader touched this recently
# Chess.com backfilled its older events around early 2021 and stamped them with
# the ratings of that moment. Before this month an Elo tag is believed only if
# the rating lists confirm it. PROVISIONAL: set from the 2009-2016 files; check
# it again once 2017-2022 events have been downloaded and enriched.
IMPORT_ERA_END = "2020-10"
SAMPLE = 60                  # players sampled per event for rating-list checks

SERIES_PATTERNS = {
    "Titled Tuesday": re.compile(r"titled\s*tuesday"),
    "Bullet Brawl": re.compile(r"bullet\s*brawl"),
    "3-0 Thursday": re.compile(r"3\s*0\s*thursday"),
    "CCC": re.compile(r"computer\s*chess\s*championship|\bccc\b|\bcccc\b"),
}
TEST_PATTERN = re.compile(r"\bcbtest|\bteste?\b|\btrial knockout\b|\bcb ugly\b")   # Chess.com's broadcast rehearsals
# The Olympiads (open and women's) are kept whatever their average: the user's
# call, 2026-10-05. Not the youth, disabled or online Olympiads, which are
# smaller events and are judged like any other.
OLYMPIAD_PATTERN = re.compile(r"\bolympiad\b")
OLYMPIAD_EXCLUDED = re.compile(r"\byouth\b|\bu\d\d\b|\bdisabilit|\bschool|\bjunior|\bonline\b")
# Chess.com often carries one event under two or three ids: the event, and a
# "-live", "-secret" or "-broadcast" copy of it. Only one is kept.
TWIN_SUFFIX = re.compile(r"-(live|secret|broadcast|tv\d?)$", re.I)
ENGINE_PATTERN = re.compile(r"\btcec|\bkomodo\b|\bstockfish\b|\bleela\b|\blc0\b")
TIME_CONTROL_RE = re.compile(r"\d{2,}(\+\d+)?(:\d+(\+\d+)?)*|\d+/\d+.*")  # "1" is not a time control
NAME_PRIORITY = {"twic": 0, "olimpbase": 1, "chess-results": 2, "mega26": 3, "gigaking": 4}
PLACE_PRIORITY = {"mega26": 0, "olimpbase": 1, "gigaking": 2}
TITLES = {"GM", "IM", "FM", "CM", "WGM", "WIM", "WFM", "WCM", "NM"}
RESULTS = {"1-0", "0-1", "1/2-1/2", "0-0", "*"}
POINTS = {"1-0": (1.0, 0.0), "0-1": (0.0, 1.0), "1/2-1/2": (0.5, 0.5)}

SCHEMA = """
create table if not exists file(
    name text primary key, size integer, mtime integer, status text, scanned_at text);
create table if not exists event(
    slug text primary key, file text, name text, site_slug text,
    games integer, players integer, rounds integer,
    tag_start text, tag_end text, slug_years text,
    unfinished integer, fen_games integer, team_games integer, board_games integer,
    time_control text, clock_games integer, clk_initial integer, cadence_guess text,
    format text, format_note text,
    fide_id_players integer, header_rated_players integer, avg_game_weighted integer,
    enriched integer default 0,
    rating_agree real, rating_agree_n integer, date_suspect integer, date_reason text,
    elo_suspect integer, elo_reason text, rating_month text,
    est_month_first text, est_month_last text, est_agree real,
    matched integer default 0,
    ely_event_id integer, ely_name text, ely_place text, ely_source text,
    ely_start text, ely_end text, ely_format text, ely_score real, ely_matches text,
    ely_avg integer, ely_avg_n integer,
    start text, end text, date_basis text,
    rated_players integer, backfilled_players integer, avg_rating integer, avg_basis text,
    min_rating integer, max_rating integer, below_floor integer,
    category integer, decision text, reason text,
    ctml_path text, ctml_sig text, ctml_valid integer, ctml_games integer,
    ctml_bytes integer, parse_errors integer, pgn_path text, pgn_sig text);
create table if not exists player(
    slug text, key text, name text, fide_id text, title text, team text,
    elo_first integer, elo_min integer, elo_max integer, games integer, points real,
    fed text, birth_year integer, ely_title text, ely_name text,
    elo_ely integer, elo_ely_period text,
    primary key(slug, key));
create index if not exists ix_player_fide on player(fide_id);
create table if not exists ely_player(
    fide_id text primary key, player_ids text, name text, fed text, sex text,
    birth text, titles text);
create table if not exists xtab(
    id integer primary key, obs_ref text unique, source text, ref text, issue integer,
    title text, name text, place text, country text, start text, end text, format text,
    category integer, ave integer, round_now integer, round_total integer, final integer,
    n_players integer, header text, players_json text);
create index if not exists ix_xtab_start on xtab(start);
create table if not exists meta(key text primary key, value text);
"""


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


# Columns added after the first catalogs were built; applied to existing ones.
MIGRATIONS = (("file", "dir", "text"), ("event", "site_tag", "text"), ("player", "ely_by_event", "integer"), ("player", "ely_by_name", "integer"), ("event", "origin", "text"), ("event", "pub_name", "text"), ("event", "byes", "integer"), ("event", "date_strays", "integer"), ("event", "ely_kind", "text"),
              ("event", "identity_doubt", "text"), ("player", "doubt", "text"),
              ("player", "in_roster", "integer"),
              # crosstable stage
              ("event", "xt_done", "integer default 0"), ("event", "xt_id", "integer"),
              ("event", "xt_source", "text"), ("event", "xt_ref", "text"), ("event", "xt_obs", "text"),
              ("event", "xt_title", "text"), ("event", "xt_name", "text"), ("event", "xt_name_ok", "integer"),
              ("event", "xt_place", "text"), ("event", "xt_country", "text"),
              ("event", "xt_start", "text"), ("event", "xt_end", "text"), ("event", "xt_format", "text"),
              ("event", "xt_category", "integer"), ("event", "xt_avg", "integer"),
              ("event", "xt_rounds", "integer"), ("event", "xt_final", "integer"),
              ("event", "xt_kind", "text"), ("event", "xt_score", "real"), ("event", "xt_rows", "integer"),
              ("event", "avg_computed", "integer"), ("event", "display_name", "text"),
              ("event", "xt_pool", "text"), ("event", "tags_standard", "integer"), ("event", "xt_fit", "real"),
              ("event", "unrated_then", "integer"), ("event", "unknown_rating", "integer"),
              ("player", "xt_name", "text"), ("player", "xt_title", "text"), ("player", "xt_fed", "text"),
              ("player", "xt_rating", "integer"), ("player", "xt_score", "real"),
              ("player", "xt_rank", "integer"), ("player", "xt_id_ok", "integer"))


def open_catalog() -> sqlite3.Connection:
    con = sqlite3.connect(CATALOG, timeout=60)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    if "below_2000" in {r["name"] for r in con.execute("pragma table_info(event)")}:
        con.execute("alter table event rename column below_2000 to below_floor")  # the floor is no longer 2000
    for table, column, kind in MIGRATIONS:
        if column not in {r["name"] for r in con.execute(f"pragma table_info({table})")}:
            con.execute(f"alter table {table} add column {column} {kind}")
    con.commit()
    return con


def open_elysium() -> sqlite3.Connection:
    # Read-only and immutable: this tool never writes to the Elysium workbench.
    return sqlite3.connect(f"file:{ELYSIUM_DB.as_posix()}?mode=ro&immutable=1", uri=True)


def norm_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", s.casefold()).strip()


def decode(b: bytes) -> str:
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("cp1252", errors="replace")


def valid_date(raw: str) -> str | None:
    """'2010.01.26' -> '2010-01-26', or None when not a real calendar day."""
    m = re.fullmatch(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", raw or "")
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    if y < 1800:
        return None
    try:
        return dt.date(y, mo, d).isoformat()
    except ValueError:
        return None


def to_int(raw: str | None) -> int | None:
    return int(raw) if raw and raw.isdigit() and int(raw) > 0 else None


def fide_category(avg: int | None) -> int | None:
    return (avg - 2251) // 25 + 1 if avg is not None and avg >= 2251 else None


def mean_int(values) -> int | None:
    values = list(values)
    return int(sum(values) / len(values) + 0.5) if values else None


def month_add(ym: str, n: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    k = y * 12 + (m - 1) + n
    return f"{k // 12:04d}-{k % 12 + 1:02d}"


# --------------------------------------------------------------------------
# scan
# --------------------------------------------------------------------------

GAME_SPLIT = re.compile(rb'(?m)^\[Event "')
TAG_RE = re.compile(rb'^\[(\w+)\s+"(.*)"\]\s*$', re.M)
CLK_RE = re.compile(rb"\[%clk (\d+):(\d\d):(\d\d)")
SITE_SLUG_RE = re.compile(r"chess\.com/events/([^/\"]+)")
SITE_TAG_RE = re.compile(rb'^\[Site "(.*)"\]\s*$', re.M)


def site_place(sites: Counter) -> str:
    """The place a file's Site tags name; '' when they hold only Chess.com URLs
    or nothing. Files from 2024 on state the venue there ("Napoli, IT")."""
    for value, _ in sites.most_common():
        value = value.strip()
        if value and value != "?" and "://" not in value and "chess.com" not in value.lower():
            return value
    return ""


def split_games(raw: bytes) -> list[bytes]:
    starts = [m.start() for m in GAME_SPLIT.finditer(raw)]
    return [raw[a:b] for a, b in zip(starts, starts[1:] + [len(raw)])]


def split_header(chunk: bytes) -> tuple[bytes, bytes]:
    cut = chunk.find(b"\n\n")
    if cut > 0 and chunk.count(b"\n[", 0, cut) == chunk.count(b"\n", 0, cut):
        return chunk[:cut + 1], chunk[cut + 1:]  # the usual shape: tags, blank line, moves
    pos = 0
    for line in chunk.splitlines(keepends=True):
        if line[:1] != b"[":
            break
        pos += len(line)
    return chunk[:pos], chunk[pos:]


def parse_tags(header: bytes) -> dict[str, str]:
    tags = {}
    for m in TAG_RE.finditer(header):
        value = decode(m.group(2)).replace('\\"', '"').replace("\\\\", "\\").strip()
        tags[m.group(1).decode("ascii", "replace")] = value
    return tags


class _Player:
    __slots__ = ("names", "fide", "titles", "teams", "elos", "games", "points")

    def __init__(self):
        self.names, self.titles, self.teams = Counter(), Counter(), Counter()
        self.fide, self.elos, self.games, self.points = None, [], 0, 0.0


def is_bye(name: str) -> bool:
    """Chess.com records a bye as a game against a player called 'bye'."""
    return norm_name(name) == "bye"


def side_points(result: str) -> tuple[float | None, float | None]:
    """Points to White and Black for any result token, including the lopsided
    ones a bye or forfeit produces ('1/2-0', '1-0', '0-1/2'). None when unknown."""
    worth = {"1": 1.0, "1/2": 0.5, "0": 0.0}
    left, _, right = result.partition("-")
    if "-" in result and left in worth and right in worth:
        return worth[left], worth[right]
    return None, None


def cadence_from_seconds(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    if seconds >= 3600:
        return "classical"
    if seconds >= 600:
        return "rapid"
    if seconds >= 180:
        return "blitz"
    return "bullet"


def scan_file(path: Path) -> dict | None:
    """Everything the headers of one event PGN state, without replaying moves."""
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    chunks = split_games(raw)
    if not chunks:
        return None
    players: dict[str, _Player] = {}
    name_to_fide: dict[str, set[str]] = defaultdict(set)
    names, site_slugs, tcs, sites = Counter(), Counter(), Counter(), Counter()
    partial_dates = set()
    event_rounds, event_types = Counter(), Counter()
    game_dates, rounds, url_rounds = [], set(), set()
    pairs: Counter = Counter()
    unfinished = fen_games = team_games = board_games = clock_games = byes = 0
    clk_max, all_elos, game_rows = [], [], []

    for chunk in chunks:
        header, moves = split_header(chunk)
        t = parse_tags(header)
        names[t.get("Event", "")] += 1
        sites[t.get("Site", "")] += 1
        m = SITE_SLUG_RE.search(t.get("Site", ""))
        if m:
            site_slugs[m.group(1)] += 1
            # .../events/2010-amber/01a-blind/...: the round, for files whose Round tags are "?"
            # ("Basque01" in a match played in several formats is its own round).
            m = re.search(r"/events/[^/]+/(?:0*(\d+)[^/]*|([^/]+))/", t.get("Site", ""))
            if m:
                url_rounds.add(m.group(1) or m.group(2))
        d = valid_date(t.get("Date", ""))
        m = re.fullmatch(r"(\d{4})\.(\d\d|\?\?)\.(?:\d\d|\?\?)", t.get("Date", ""))
        if m and 1000 < int(m.group(1)) < 2100:
            partial_dates.add(m.group(1) + ("" if m.group(2) == "??" else "-" + m.group(2)))
        if d:
            # EndDate counts only when it is the same game finishing: a stray
            # EndDate years later is the day Chess.com touched the record.
            e = valid_date(t.get("EndDate", ""))
            late = (dt.date.fromisoformat(e) - dt.date.fromisoformat(d)).days if e else -1
            game_dates.append((d, e if 0 <= late <= 3 else d))
        if t.get("EventRounds", "").isdigit() and int(t["EventRounds"]) > 0:
            event_rounds[int(t["EventRounds"])] += 1
        if t.get("EventType"):
            event_types[t["EventType"].split("(")[0].strip().lower()] += 1
        rnd = t.get("Round", "")
        if rnd and rnd not in ("?", "-"):
            rounds.add(rnd.split(".")[0])
        result = t.get("Result", "*")
        bye = is_bye(t.get("White", "")) or is_bye(t.get("Black", ""))
        byes += bye
        if result not in POINTS and not bye:
            unfinished += 1
        fen_games += "FEN" in t and not bye
        board_games += "Board" in t
        team_games += bool(t.get("WhiteTeam") or t.get("BlackTeam"))
        if TIME_CONTROL_RE.fullmatch(t.get("TimeControl", "")):
            tcs[t["TimeControl"]] += 1
        clks = [int(h) * 3600 + int(mi) * 60 + int(s) for h, mi, s in CLK_RE.findall(moves)]
        if len(set(clks)) > 4:  # a clock that actually ran, not one value repeated
            clock_games += 1
            clk_max.append(max(clks))

        keys = []
        for side, pts in zip(("White", "Black"), side_points(result)):
            name = t.get(side, "").strip()
            if is_bye(name):
                continue  # not a person; the opponent keeps the points the bye gave
            fid = t.get(f"{side}FideId", "").strip()
            fid = fid if fid.isdigit() and int(fid) > 0 else None
            key = f"f:{fid}" if fid else f"n:{norm_name(name)}"
            keys.append(key)
            p = players.setdefault(key, _Player())
            p.names[name] += 1
            p.fide = fid
            if fid:
                name_to_fide[norm_name(name)].add(key)
            title = t.get(f"{side}Title", "").strip().upper()
            if title in TITLES:
                p.titles[title] += 1
            team = t.get(f"{side}Team", "").strip()
            if team:
                p.teams[team] += 1
            elo = to_int(t.get(f"{side}Elo"))
            if elo:
                p.elos.append(elo)
                all_elos.append(elo)
            p.games += not bye
            if pts is not None:
                p.points += pts
        if not bye:
            game_rows.append(keys)

    # A player tagged with a FIDE id in some games and not in others is one person.
    alias = {}
    for key in list(players):
        if key.startswith("n:"):
            owners = name_to_fide.get(key[2:], set())
            if len(owners) == 1:
                target = players[next(iter(owners))]
                src = players.pop(key)
                target.names.update(src.names)
                target.titles.update(src.titles)
                target.teams.update(src.teams)
                target.elos.extend(src.elos)
                target.games += src.games
                target.points += src.points
                alias[key] = next(iter(owners))
    for keys in game_rows:
        a, b = (alias.get(k, k) for k in keys)
        if a != b:
            pairs[frozenset((a, b))] += 1

    n = len(players)
    fmt = note = None
    if team_games * 2 > len(chunks):
        fmt = "team"
    elif n == 2:
        fmt = "match"
    elif n >= 3:
        full = n * (n - 1) // 2
        if len(pairs) == full and len(set(pairs.values())) == 1:
            fmt = "round-robin"
            k = next(iter(pairs.values()))
            note = "single" if k == 1 else f"{k} games per pairing"
        else:
            note = f"{len(pairs)} of {full} possible pairings played"

    # The event's dates are the busiest run of game dates; a game or two dated
    # months away from everything else does not stretch the event.
    tag_start = tag_end = None
    strays = 0
    if game_dates:
        per_day = Counter(d for d, _ in game_dates)
        runs, days = [], sorted(per_day)
        for day in days:
            if runs and (dt.date.fromisoformat(day) - dt.date.fromisoformat(runs[-1][-1])).days <= 45:
                runs[-1].append(day)
            else:
                runs.append([day])
        main = max(runs, key=lambda run: sum(per_day[day] for day in run))
        tag_start = main[0]
        tag_end = max(e for d, e in game_dates if main[0] <= d <= main[-1])
        strays = sum(per_day[day] for run in runs if run is not main for day in run)
    elif partial_dates:
        # No game has a full date ("1977.??.??", "1988.06.??"): the event is
        # dated to the month if every game names the same one, else to the year(s).
        if len(partial_dates) == 1 and len(next(iter(partial_dates))) == 7:
            tag_start = tag_end = next(iter(partial_dates))
        else:
            years = sorted({value[:4] for value in partial_dates})
            tag_start, tag_end = years[0], years[-1]

    if path.parent == HISTORICAL_DIR:
        # A historical file holds only the games between its strongest players
        # (the collection it was cut from keeps games where both sides are
        # rated 2400 or more), so the pairings present say nothing about the
        # event's format. Its EventType tag does.
        kind = event_types.most_common(1)[0][0] if event_types else ""
        fmt = ("team" if kind.startswith("team") else
               {"tourn": "round-robin", "match": "match", "swiss": "swiss", "k.o.": "knockout",
                "ko": "knockout"}.get(kind))
        note = f"EventType tag: {kind}" if kind else None

    stem = path.stem
    slug_years = sorted(set(re.findall(r"(?<!\d)(1[89]\d\d|20[0-3]\d)(?!\d)", stem)))
    clk_initial = int(statistics.median(clk_max)) if clk_max else None
    tc = tcs.most_common(1)[0][0] if tcs else None
    tc_seconds = int(tc.split("+")[0]) if tc and re.fullmatch(r"\d{3,}(\+\d+)?", tc) else None
    return {
        "event": {
            "slug": stem, "file": path.name, "name": names.most_common(1)[0][0],
            "site_slug": site_slugs.most_common(1)[0][0] if site_slugs else None,
            "site_tag": site_place(sites),
            "games": len(chunks) - byes, "byes": byes, "players": n,
            "rounds": (len(rounds) or len(url_rounds)
                       or (event_rounds.most_common(1)[0][0] if event_rounds else None)),
            "tag_start": tag_start, "tag_end": tag_end, "date_strays": strays,
            "slug_years": ",".join(slug_years) or None,
            "unfinished": unfinished, "fen_games": fen_games, "team_games": team_games,
            "board_games": board_games, "time_control": tc, "clock_games": clock_games,
            "clk_initial": clk_initial,
            "cadence_guess": cadence_from_seconds(tc_seconds or clk_initial),
            "format": fmt, "format_note": note,
            "fide_id_players": sum(1 for p in players.values() if p.fide),
            "header_rated_players": sum(1 for p in players.values() if p.elos),
            "avg_game_weighted": mean_int(all_elos),
        },
        "players": [
            {
                "slug": stem, "key": key, "name": p.names.most_common(1)[0][0], "fide_id": p.fide,
                "title": p.titles.most_common(1)[0][0] if p.titles else None,
                "team": p.teams.most_common(1)[0][0] if p.teams else None,
                "elo_first": p.elos[0] if p.elos else None,
                "elo_min": min(p.elos) if p.elos else None,
                "elo_max": max(p.elos) if p.elos else None,
                "games": p.games, "points": p.points,
            }
            for key, p in players.items()
        ],
    }


def insert_row(con: sqlite3.Connection, table: str, row: dict) -> None:
    cols = ", ".join(row)
    con.execute(f"insert into {table}({cols}) values ({', '.join('?' * len(row))})", list(row.values()))


def source_path(folder: str | None, name: str) -> Path | None:
    """Where a scanned PGN is now: the folder it was scanned in, else any inbox."""
    for base in ([Path(folder)] if folder else []) + INBOXES:
        if (base / name).is_file():
            return base / name
    return None


def stage_scan(con: sqlite3.Connection, inboxes: list[Path], limit: int | None = None,
               rescan: bool = False) -> None:
    if rescan:
        con.execute("delete from file")  # every file is read again; Elysium player lookups stay cached
    known = {r["name"]: (r["size"], r["mtime"], r["dir"])
             for r in con.execute("select name, size, mtime, dir from file")}
    now = time.time()
    new = empty = skipped_fresh = failed = moved = partial = 0
    found: dict[str, tuple[Path, object]] = {}
    for inbox in inboxes:
        if not inbox.is_dir():
            continue
        partial += len(list(inbox.glob("*.crdownload")))
        for path in inbox.glob("*.pgn"):
            st = path.stat()
            if path.name not in found or st.st_mtime > found[path.name][1].st_mtime:
                found[path.name] = (path, st)
    for name in sorted(found):
        path, st = found[name]
        sig = (st.st_size, int(st.st_mtime))
        if name in known and known[name][:2] == sig:
            if known[name][2] != str(path.parent):  # same file, new folder
                con.execute("update file set dir = ? where name = ?", (str(path.parent), name))
                moved += 1
            continue
        if now - st.st_mtime < SETTLE_SECONDS:
            skipped_fresh += 1
            continue
        status = "ok"
        result = None
        if st.st_size == 0:
            status = "empty"
            empty += 1
        else:
            try:
                result = scan_file(path)
                if result and path.parent == HISTORICAL_DIR:
                    result["event"]["origin"] = "historical"
                if result is None:
                    status = "no-games"
            except Exception as exc:  # keep going; the file is reported, not lost
                status = f"error: {exc}"
                failed += 1
        con.execute("delete from event where file=?", (path.name,))
        con.execute("delete from player where slug=?", (path.stem,))
        if result:
            insert_row(con, "event", result["event"])
            for p in result["players"]:
                insert_row(con, "player", p)
            new += 1
        con.execute(
            "insert or replace into file(name, size, mtime, status, scanned_at, dir) values (?,?,?,?,?,?)",
            (path.name, sig[0], sig[1], status, dt.datetime.now().isoformat(timespec="seconds"),
             str(path.parent)),
        )
        if new % 200 == 0:
            con.commit()
        if limit and new >= limit:
            break
    # Events scanned before site_tag was recorded: read just that tag.
    for r in con.execute("select e.slug, e.file, f.dir from event e join file f on f.name = e.file "
                         "where e.site_tag is null").fetchall():
        path = source_path(r["dir"], r["file"])
        if path:
            found_sites = Counter(decode(v) for v in SITE_TAG_RE.findall(path.read_bytes()))
            con.execute("update event set site_tag = ? where slug = ?", (site_place(found_sites), r["slug"]))
    con.commit()
    log(f"scan: {new} events read, {empty} empty files, {failed} failed, {moved} found in a new folder, "
        f"{skipped_fresh} too fresh to touch, {partial} still downloading")


# --------------------------------------------------------------------------
# enrich (Elysium players and ratings)
# --------------------------------------------------------------------------

class Elysium:
    def __init__(self):
        self.con = open_elysium()
        self.cur = self.con.cursor()

    def player_by_fide(self, fide_id: str) -> dict | None:
        row = self.cur.execute(
            "select p.id, p.display_name, p.federation, p.sex, p.birth_date, p.merged_into "
            "from player_external_id x join player p on p.id = x.player_id "
            "where x.system = 'fide' and x.external_id = ?", (fide_id,)).fetchone()
        if not row:
            return None
        ids, (pid, name, fed, sex, birth, merged) = [row[0]], row
        hops = 0
        while merged and hops < 6:  # follow the soft-merge chain to the live identity
            nxt = self.cur.execute(
                "select id, display_name, federation, sex, birth_date, merged_into from player where id = ?",
                (merged,)).fetchone()
            if not nxt:
                break
            ids.append(nxt[0])
            name, fed, sex, birth, merged = nxt[1], nxt[2] or fed, nxt[3] or sex, nxt[4] or birth, nxt[5]
            hops += 1
        marks = ",".join("?" * len(ids))
        titles = [r[0] for r in self.cur.execute(
            f"select distinct title from player_title where player_id in ({marks})", ids)]
        return {"player_ids": ",".join(map(str, ids)), "name": name, "fed": fed, "sex": sex,
                "birth": birth, "titles": ",".join(sorted(titles))}

    def rating_at(self, player_ids: str, ym: str, back_months: int | None = None) -> tuple[int, str] | None:
        """The rating in force in month ym: that month's, else the most recent
        earlier one however far back. A rating carries forward across months
        with none recorded; it never reaches back before the player's first."""
        best = None
        earliest = month_add(ym, -back_months) if back_months else "0000-00"
        for pid in player_ids.split(","):
            row = self.cur.execute(
                "select period, value from rating where player_id = ? and system = 'combined' "
                "and scope = 'standard' and period <= ? and period >= ? and value is not null "
                "order by period desc limit 1", (int(pid), ym, earliest)).fetchone()
            if row and (best is None or row[0] > best[1]):
                best = (row[1], row[0])
        return best

    def series(self, player_ids: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for pid in player_ids.split(","):
            for period, value in self.cur.execute(
                    "select period, value from rating where player_id = ? and system = 'combined' "
                    "and scope = 'standard' and period_prec = 'month' and value is not null", (int(pid),)):
                out[period] = value
        return out


def ely_player_cached(con: sqlite3.Connection, ely: Elysium, fide_id: str) -> sqlite3.Row | None:
    row = con.execute("select * from ely_player where fide_id = ?", (fide_id,)).fetchone()
    if row is None:
        found = ely.player_by_fide(fide_id) or {}
        con.execute(
            "insert into ely_player(fide_id, player_ids, name, fed, sex, birth, titles) values (?,?,?,?,?,?,?)",
            (fide_id, found.get("player_ids"), found.get("name"), found.get("fed"), found.get("sex"),
             found.get("birth"), found.get("titles")))
        row = con.execute("select * from ely_player where fide_id = ?", (fide_id,)).fetchone()
    return row if row["player_ids"] else None


def rating_for(ev, p, ignore_doubt: bool = False) -> tuple[int | None, str | None]:
    """The player's standard FIDE rating at the time of the event, and its source.

    In order: the rating printed in the event's own crosstable (when that table
    prints standard ratings); Elysium's rating for the event month; the PGN's
    Elo tag, but only where the event's tags have been shown to be standard
    ratings (ev["tags_standard"]). For events later than Elysium's last list
    the tag is preferred when it sits within 100 points of that last list,
    which marks it as the same pool, only newer. A player whose FIDE id is not
    believed has no rating unless the crosstable gives one.
    """
    if p["xt_rating"] and ev["xt_pool"] != "other":
        return p["xt_rating"], "table"
    if p["doubt"] and not ignore_doubt:
        return None, None
    if p["xt_name"] and p["xt_id_ok"] == 0:
        return None, None  # the FIDE id is someone else's and the table gave no rating
    tag = None if (ev["elo_suspect"] and p["fide_id"]) else p["elo_first"]
    covered = bool(ev["rating_month"]) and ev["rating_month"] <= ELYSIUM_LAST_MONTH
    if p["elo_ely"]:
        if not covered and tag and abs(tag - p["elo_ely"]) <= 100:
            return tag, "pgn"
        return p["elo_ely"], "elysium"
    if tag and ev["tags_standard"]:
        return tag, "pgn"
    return None, None


def slug_year_conflict(slug_years: str | None, tag_start: str | None, tag_end: str | None) -> bool:
    if not slug_years or not tag_start:
        return False
    lo, hi = int(tag_start[:4]) - 1, int((tag_end or tag_start)[:4]) + 1
    return not any(lo <= int(y) <= hi for y in slug_years.split(","))


def matching_list_period(ely: Elysium, sample) -> tuple[str, str, float, bool] | None:
    """Which rating lists do these Elo tags come from?

    Returns (first month, last month, share of the sample matching, at_data_end)
    for the run of months in which the most sampled players' Elo tag equals their
    published rating, or None when no single period explains half the sample.
    at_data_end marks a run that reaches the last month Elysium holds, where
    "matches the newest list" is not evidence of anything.
    """
    hits: Counter = Counter()
    data_end = ""
    for p, ep in sample:
        series = ely.series(ep["player_ids"])
        if series:
            data_end = max(data_end, max(series))
        for period, value in series.items():
            if value == p["elo_first"]:
                hits[period] += 1
    if not hits:
        return None
    top = max(hits.values())
    if top < max(2, 0.5 * len(sample)):
        return None
    first = last = max(hits, key=lambda k: (hits[k], k))
    while hits.get(month_add(first, -1), 0) >= 0.9 * top:
        first = month_add(first, -1)
    while hits.get(month_add(last, 1), 0) >= 0.9 * top:
        last = month_add(last, 1)
    return first, last, top / len(sample), last >= data_end


def stage_enrich(con: sqlite3.Connection, limit: int | None = None) -> None:
    ely = Elysium()
    events = con.execute("select * from event where enriched = 0 order by slug").fetchall()
    if limit:
        events = events[:limit]
    started = time.time()
    for i, ev in enumerate(events, 1):
        hay = norm_name(f"{ev['slug']} {ev['name']}")
        if (ev["origin"] == "historical" or ENGINE_PATTERN.search(hay)
                or any(pat.search(hay) for pat in SERIES_PATTERNS.values())):
            # Titled Tuesday and the like are set aside by name; their hundreds
            # of players per file are not worth looking up. Historical events
            # are taken as their own PGN states them.
            con.execute("update event set enriched = 1, date_suspect = 0, elo_suspect = 0 where slug = ?",
                        (ev["slug"],))
            continue
        players = con.execute("select * from player where slug = ?", (ev["slug"],)).fetchall()
        tag_month = ev["tag_start"][:7] if ev["tag_start"] else None
        linked = []
        for p in players:
            if not p["fide_id"]:
                continue
            ep = ely_player_cached(con, ely, p["fide_id"])
            if ep is None:
                continue
            linked.append((p, ep))
            fed = ep["fed"] if ep["fed"] and re.fullmatch(r"[A-Z]{3}", ep["fed"]) else None
            birth = int(ep["birth"][:4]) if ep["birth"] and ep["birth"][:4].isdigit() else None
            con.execute(
                "update player set fed = ?, birth_year = ?, ely_title = ?, ely_name = ? where slug = ? and key = ?",
                (fed, birth, ep["titles"] or None, ep["name"], p["slug"], p["key"]))

        # Do the Elo tags agree with the rating list in force when the Date
        # tags say the event was played?
        checked = agreed = 0
        sample = [(p, ep) for p, ep in linked if p["elo_first"]][:SAMPLE]
        if tag_month:
            for p, ep in sample:
                got = ely.rating_at(ep["player_ids"], tag_month, back_months=6)
                if got:
                    checked += 1
                    agreed += got[0] == p["elo_first"]
        agree = agreed / checked if checked else None

        conflict = slug_year_conflict(ev["slug_years"], ev["tag_start"], ev["tag_end"])
        est = None
        if len(sample) >= 2 and (conflict or not tag_month or checked == 0 or agree < 0.5):
            est = matching_list_period(ely, sample)
        slug_ys = [int(y) for y in (ev["slug_years"] or "").split(",") if y]

        # Chess.com gets one of two things wrong on older events. Either it
        # attached ratings from the list current when it imported the event
        # (Elo tags from a LATER list than the dates), or it stamped the import
        # date on a historical event (Elo tags from an EARLIER list than the dates).
        date_suspect = elo_suspect = 0
        date_reason = elo_reason = None
        if conflict or not tag_month:
            date_suspect = 1
            date_reason = ("no valid Date tag" if not tag_month
                           else f"name says {ev['slug_years']}, Date tags say {ev['tag_start'][:4]}")
            if est and slug_ys and not any(
                    abs(int(m[:4]) - y) <= 1 for m in est[:2] for y in slug_ys):
                elo_suspect = 1
                elo_reason = f"Elo tags match the {est[0]}..{est[1]} rating lists, not {ev['slug_years']}"
        elif est and est[2] >= 0.6 and (checked == 0 or (checked >= 2 and agree < 0.5)):
            if est[0] > tag_month:
                elo_suspect = 1
                elo_reason = (f"Elo tags match the {est[0]}..{est[1]} rating lists, not {tag_month}'s "
                              f"({agreed} of {checked} agree)")
            elif est[1] < month_add(tag_month, -3) and not est[3]:
                date_suspect = 1
                date_reason = f"Elo tags match the {est[0]}..{est[1]} rating lists, but Date tags say {tag_month}"
        if (not elo_suspect and not date_suspect and tag_month < IMPORT_ERA_END
                and any(p["elo_first"] for p in players) and not (checked >= 4 and agree >= 0.8)):
            # Too few players to prove it either way, or a mix of event-time
            # and import-time ratings. Before the import nothing is taken on trust.
            elo_suspect = 1
            elo_reason = (f"event predates Chess.com's import and its Elo tags are not confirmed by the "
                          f"{tag_month} rating list ({agreed} of {checked} agree)")

        # The month whose rating list applies to this event.
        if not date_suspect:
            rating_month = tag_month
        elif est and not elo_suspect:
            rating_month = est[0]
        elif slug_ys:
            rating_month = f"{slug_ys[0]}-07"  # only the year is known: mid-year list
        else:
            rating_month = None

        # Elysium's rating for the event month, for every player it knows.
        # rating_for() decides when it is used in place of the Elo tag.
        con.execute("update player set elo_ely = null, elo_ely_period = null where slug = ?", (ev["slug"],))
        if rating_month:
            for p, ep in linked:
                got = ely.rating_at(ep["player_ids"], rating_month)
                if got:
                    con.execute("update player set elo_ely = ?, elo_ely_period = ? where slug = ? and key = ?",
                                (got[0], got[1], p["slug"], p["key"]))

        con.execute(
            "update event set enriched = 1, rating_agree = ?, rating_agree_n = ?, date_suspect = ?, "
            "date_reason = ?, elo_suspect = ?, elo_reason = ?, rating_month = ?, est_month_first = ?, "
            "est_month_last = ?, est_agree = ? where slug = ?",
            (agree, checked, date_suspect, date_reason, elo_suspect, elo_reason, rating_month,
             est[0] if est else None, est[1] if est else None, est[2] if est else None, ev["slug"]))
        if i % 50 == 0:
            con.commit()
            log(f"  enrich {i}/{len(events)}  ({time.time() - started:.0f}s)")
    con.commit()
    log(f"enrich: {len(events)} events in {time.time() - started:.0f}s")


# --------------------------------------------------------------------------
# match (Elysium events)
# --------------------------------------------------------------------------

def name_keys(name: str) -> tuple[set[str], set[str]]:
    """(family + first initial keys, family-only keys) for roster comparison."""
    name = re.sub(r"\([^)]*\)", " ", name or "").strip()  # 'H. Nakamura (USA)'
    if "," in name:
        fam, giv = name.split(",", 1)
        fam, giv = norm_name(fam), norm_name(giv)
        if not fam:
            return set(), set()
        return ({f"{fam} {giv[0]}"} if giv else set()), {fam}
    toks = norm_name(name).split()
    if not toks:
        return set(), set()
    if len(toks) == 1:
        return set(), {toks[0]}
    # Order unknown. Read it both ways: 'Davidsson Stefan Orri' (family first,
    # then the given name) and 'Pablo Glavina' (given name first, family last).
    return ({f"{toks[0]} {toks[1][0]}", f"{toks[-1]} {toks[0][0]}"},
            {t for t in toks if len(t) > 1})


def name_words(text: str) -> set[str]:
    """Distinguishing words of an event name: no years, numbers or ordinals."""
    return {w for w in norm_name(text).split() if not re.fullmatch(r"\d+(st|nd|rd|th)?", w) and len(w) > 1}


class Roster:
    """Names reduced to comparison keys. Chess.com rosters are often mixed:
    'Van Wely, Loek' beside a bare 'Anand'."""

    def __init__(self, names):
        self.n = 0
        self.full: set[str] = set()       # family + initial, where a given name exists
        self.fam: set[str] = set()        # every family key
        self.bare: set[str] = set()       # family keys of names given as a surname only
        self.items = []
        self.words = []                   # per item: the name's longer words, for loose pairing
        for name in names:
            full, fam = name_keys(name)
            if not fam:
                continue
            # A bare 'Dominguez' should meet 'Dominguez Perez, Leinier'.
            fam = fam | {tok for key in fam for tok in key.split() if len(tok) >= 5 and " " in key}
            self.n += 1
            self.full |= full
            self.fam |= fam
            if not full:
                self.bare |= fam
            self.items.append((full, fam))
            self.words.append({t for t in norm_name(re.sub(r"\([^)]*\)", " ", name)).split() if len(t) >= 4})

    def lists(self, full: set[str], fam: set[str]) -> bool:
        """Is a person with these keys on this roster?"""
        if not full:
            return bool(fam & self.fam)
        return bool(full & self.full) or bool(fam & self.bare)


def roster_overlap(ours: Roster, theirs: Roster) -> int:
    # Counted from both sides so several people cannot all match one name.
    return min(sum(theirs.lists(full, fam) for full, fam in ours.items),
               sum(ours.lists(full, fam) for full, fam in theirs.items))


def family_overlap(ours: Roster, theirs: Roster) -> int:
    """Overlap by family name alone: 'Kramnik,W' is 'Kramnik, Vladimir', and a
    wrongly identified 'Adams, David M' still sits where 'Adams, Michael' does."""
    return min(sum(bool(fam & theirs.fam) for _, fam in ours.items),
               sum(bool(fam & ours.fam) for _, fam in theirs.items))


def stage_match(con: sqlite3.Connection, limit: int | None = None) -> None:
    ely = Elysium()
    cur = ely.cur
    roster_cache: dict[int, tuple[Roster, list[int]]] = {}
    player_names: dict[int, str] = {}

    def ely_roster(event_id: int) -> tuple[Roster, list[int]]:
        if event_id in roster_cache:
            return roster_cache[event_id]
        names, ratings = [], []
        for raw_name, rating in cur.execute(
                "select raw_name, rating_snapshot from participant where event_id = ?", (event_id,)):
            if raw_name:
                names.append(raw_name)
            if rating:
                ratings.append(rating)
        if not names:
            # No participant list: the roster is whoever plays the event's
            # games, by the names as printed. Elysium has not resolved every
            # side to a player (half the field of a 2023 round robin can be
            # unresolved), so a roster of resolved players alone is too thin
            # to match. game_observation has no index on game_id, but its ids
            # are the games' ids; game_id is checked all the same.
            game_ids = [r[0] for r in cur.execute("select id from game where event_id = ?", (event_id,))]
            wanted, raw_names = set(game_ids), set()
            for j in range(0, len(game_ids), 500):
                part = game_ids[j:j + 500]
                for game_id, white, black in cur.execute(
                        "select game_id, raw_white, raw_black from game_observation "
                        f"where id in ({','.join('?' * len(part))})", part):
                    if game_id in wanted:
                        raw_names.update(x for x in (white, black) if x)
            names = sorted(raw_names)
        if not names:
            ids = set()
            for w, b in cur.execute("select white_player_id, black_player_id from game where event_id = ?",
                                    (event_id,)):
                ids.update(x for x in (w, b) if x)
            missing = [x for x in ids if x not in player_names]
            for j in range(0, len(missing), 500):
                part = missing[j:j + 500]
                for pid, display in cur.execute(
                        f"select id, display_name from player where id in ({','.join('?' * len(part))})", part):
                    player_names[pid] = display
            names = [player_names[x] for x in ids if x in player_names]
        roster_cache[event_id] = (Roster(names), ratings)
        return roster_cache[event_id]

    source_cache: dict[int, str] = {}

    def ely_source(event_id: int, ref: str) -> str:
        """The dataset an Elysium event came from: twic, mega26, olimpbase, ..."""
        parts = ref.split(":")
        if len(parts) >= 3 and parts[1] in NAME_PRIORITY:
            return parts[1]
        if event_id not in source_cache:
            row = cur.execute(
                "select s.label from event_alias a join source_document d on d.id = a.source_doc_id "
                "join source s on s.id = d.source_id where a.event_id = ? limit 1", (event_id,)).fetchone()
            source_cache[event_id] = row[0] if row else (parts[1] if len(parts) >= 3 else "elysium")
        return source_cache[event_id]

    def day_gap(a: str | None, b: str | None) -> int:
        try:
            return abs((dt.date.fromisoformat(a) - dt.date.fromisoformat(b)).days)
        except (TypeError, ValueError):
            return 0

    con.execute("update event set matched = 1 where matched = 0 and origin = 'historical'")
    events = con.execute(
        "select * from event where matched = 0 and enriched = 1 order by coalesce(tag_start, ''), slug").fetchall()
    if limit:
        events = events[:limit]
    started = time.time()
    found = 0
    for i, ev in enumerate(events, 1):
        if ev["date_suspect"]:
            years = (ev["slug_years"] or "").split(",")
            if ev["est_month_first"] and not ev["elo_suspect"]:
                lo = (dt.date.fromisoformat(ev["est_month_first"] + "-01") - dt.timedelta(days=5)).isoformat()
                hi = month_add(ev["est_month_last"], 1) + "-05"
            elif years[0]:
                lo, hi = f"{years[0]}-01-01", f"{years[-1]}-12-31"  # only the year is known
            else:
                lo = hi = None
        else:
            day = dt.date.fromisoformat(ev["tag_start"])
            lo, hi = (day - dt.timedelta(days=4)).isoformat(), (day + dt.timedelta(days=4)).isoformat()
        matches = []
        our_words = name_words(f"{ev['slug']} {ev['name']}")
        if lo:
            ours = Roster(r["name"] for r in con.execute("select name from player where slug = ?", (ev["slug"],)))
            candidates = cur.execute(
                "select id, ref, name, format, cadence, date_start, date_end, raw_place from event "
                "where date_start between ? and ? and length(date_start) = 10 and merged_into is null",
                (lo, hi)).fetchall()
            for eid, ref, name, fmt, cadence, d0, d1, place in candidates:
                theirs, ratings = ely_roster(eid)
                if not theirs.n or not ours.n:
                    continue
                common = roster_overlap(ours, theirs)
                if common < 2:
                    continue
                jaccard = common / (ours.n + theirs.n - common)
                contained = common / min(ours.n, theirs.n)
                if jaccard >= 0.6:
                    kind, score = "same", jaccard
                elif contained >= 0.8 and common >= 8:
                    # One roster sits inside the other: Elysium has a section of
                    # this file ("part"), or this file is the broadcast boards of
                    # a larger event ("whole").
                    kind, score = ("part" if theirs.n < ours.n else "whole"), contained
                else:
                    continue
                distance = 0 if ev["date_suspect"] else day_gap(d0, ev["tag_start"]) + day_gap(d1 or d0, ev["tag_end"])
                matches.append({
                    "id": eid, "source": ely_source(eid, ref), "kind": kind, "name": name, "place": place,
                    "format": fmt, "cadence": cadence, "start": d0, "end": d1, "common": common,
                    "ours": ours.n, "theirs": theirs.n, "score": round(score, 3), "days_off": distance,
                    "words": len(our_words & name_words(name or "")),
                    "avg": mean_int(ratings) if len(ratings) >= 0.5 * theirs.n else None,
                    "avg_n": len(ratings),
                })
        fields = dict.fromkeys(
            ("ely_event_id", "ely_name", "ely_place", "ely_source", "ely_start", "ely_end", "ely_format",
             "ely_score", "ely_kind", "ely_matches", "ely_avg", "ely_avg_n"))
        if matches:
            found += 1
            # Best first: the same roster before a partial one, then the roster
            # score, then the name that shares more words with ours (the rapid
            # and the blitz of one festival have one roster and often one date
            # range), then the closest dates, then the source; among TWIC's
            # weekly snapshots of one event the latest (highest id) is final.
            rank = {"same": 0, "whole": 1, "part": 2}
            matches.sort(key=lambda m: (rank[m["kind"]], -round(m["score"], 1), -m["words"], m["days_off"],
                                        NAME_PRIORITY.get(m["source"], 9), -m["id"]))
            best = matches[0]
            same = [m for m in matches if m["kind"] == "same"]
            pick = lambda key: next((m[key] for m in matches if m[key]), None)
            with_avg = next((m for m in same if m["avg"]), None)
            placed = sorted((m for m in matches if m["place"]),
                            key=lambda m: (m["days_off"] > 3, PLACE_PRIORITY.get(m["source"], 9)))
            fields.update(
                ely_event_id=best["id"], ely_name=best["name"] if best["kind"] != "part" else None,
                ely_source=best["source"], ely_kind=best["kind"],
                ely_start=best["start"] if best["kind"] != "part" else None,
                ely_end=best["end"] if best["kind"] != "part" else None, ely_score=best["score"],
                ely_place=placed[0]["place"] if placed else None, ely_format=pick("format") if same else None,
                ely_matches=json.dumps(matches, ensure_ascii=False),
                ely_avg=with_avg["avg"] if with_avg else None,
                ely_avg_n=with_avg["avg_n"] if with_avg else None)
        # Which of our players does the matched roster actually list? A name
        # Elysium's table lacks is the first sign of a wrong identity.
        con.execute("update player set in_roster = null where slug = ?", (ev["slug"],))
        if matches and matches[0]["kind"] in ("same", "whole"):
            theirs = roster_cache[matches[0]["id"]][0]
            for p in con.execute("select key, name from player where slug = ?", (ev["slug"],)).fetchall():
                full, fam = name_keys(p["name"])
                con.execute("update player set in_roster = ? where slug = ? and key = ?",
                            (int(theirs.lists(full, fam)), ev["slug"], p["key"]))
        sets = ", ".join(f"{k} = ?" for k in fields)
        con.execute(f"update event set matched = 1, {sets} where slug = ?", [*fields.values(), ev["slug"]])
        if i % 100 == 0:
            con.commit()
            log(f"  match {i}/{len(events)}  found {found}  ({time.time() - started:.0f}s)")
    con.commit()
    log(f"match: {found} of {len(events)} events found in Elysium ({time.time() - started:.0f}s)")


# --------------------------------------------------------------------------
# tables (crosstables from the source-evidence archive)
# --------------------------------------------------------------------------

OLD_TITLES = {"g": "GM", "m": "IM", "f": "FM", "c": "CM", "wg": "WGM", "wm": "WIM", "wf": "WFM", "wc": "WCM"}
XT_SOURCE_PRIORITY = {"twic": 0, "olimpbase": 1, "chess-results": 2, "nwchess-minev": 3}
XT_FIELDS = ("xt_id", "xt_source", "xt_ref", "xt_obs", "xt_title", "xt_name", "xt_name_ok", "xt_place",
             "xt_country", "xt_start", "xt_end", "xt_format", "xt_category", "xt_avg", "xt_rounds",
             "xt_final", "xt_kind", "xt_score", "xt_rows", "xt_pool", "xt_fit")
XT_PLAYER_FIELDS = ("xt_name", "xt_title", "xt_fed", "xt_rating", "xt_score", "xt_rank", "xt_id_ok")


def roman_to_int(text: str) -> int | None:
    values = {"I": 1, "V": 5, "X": 10}
    total = prev = 0
    for ch in reversed(text.upper()):
        v = values.get(ch)
        if v is None:
            return None
        total += v if v >= prev else -v
        prev = max(prev, v)
    return total or None


def parse_table_header(header: str) -> dict:
    """What a TWIC table header states beyond name and dates.

    Two generations of header exist:
      'Tal Memorial Moscow (RUS), 5-14 xi 2010 cat. XXI (2757)'
      '69th ch-GRE 2019 Thessaloniki GRE Wed 20th Nov 2019 - Thu 28th Nov 2019. Category: 5. Ave: (2374)'
    and standings carry 'Leading Round 6 (of 9) Standings:' or 'Leading Final Round 9 Standings:'.
    """
    out = dict.fromkeys(("category", "ave", "round_now", "round_total", "final", "country"))
    header = header.strip()
    m = re.search(r"Category:\s*(\d+)", header)
    if m:
        out["category"] = int(m.group(1))
    else:
        m = re.search(r"\bcat\.?\s*([IVX]+)\b", header)
        if m:
            out["category"] = roman_to_int(m.group(1))
    m = re.search(r"\((\d{4})\)$", header) or re.search(r"Ave(?:rage rating)?:?\s*\(?(\d{4})\)?", header)
    if m and 1000 <= int(m.group(1)) <= 2900:
        out["ave"] = int(m.group(1))
    m = re.search(r"Round\s+(\d+)\s*\(?of\s+(\d+)\)?", header)
    if m:
        out["round_now"], out["round_total"] = int(m.group(1)), int(m.group(2))
    m = re.search(r"Final Round\s+(\d+)", header)
    if m:
        out["final"] = 1
        out["round_now"] = int(m.group(1))
        out["round_total"] = out["round_total"] or int(m.group(1))
    m = (re.search(r"\(([A-Z]{3})\),", header)
         or re.search(r"\b([A-Z]{3})\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\d", header))
    if m:
        out["country"] = m.group(1)
    return out


def build_table_index(con: sqlite3.Connection) -> bool:
    """Copy the dated crosstable records into the catalog, once per version of the file."""
    if not CROSSTABLES_JSON.exists():
        log(f"tables: {CROSSTABLES_JSON} not found; stage skipped")
        return False
    stamp = f"{file_stamp(CROSSTABLES_JSON)}:index-4"
    row = con.execute("select value from meta where key = 'xtab_stamp'").fetchone()
    if row and row[0] == stamp:
        return True
    records = json.loads(CROSSTABLES_JSON.read_text(encoding="utf-8"))
    if isinstance(records, dict):  # tolerate {"crosstables": [...]}
        records = next(v for v in records.values() if isinstance(v, list))
    con.execute("delete from xtab")
    count = 0
    for number, p in enumerate(records):
        if not (isinstance(p.get("start"), str) and len(p["start"]) == 10):
            continue
        players = [x for x in (p.get("players") or []) if (x.get("name") or "").strip()]
        if len(players) < 2 or p.get("format") == "team":
            continue
        # A few old tables were parsed with the year in the rating column.
        year = int(p["start"][:4])
        rated = [x["rating"] for x in players if isinstance(x.get("rating"), int)]
        if rated and sum(year - 2 <= r <= year + 1 for r in rated) >= 0.5 * len(rated):
            for x in players:
                x["rating"] = None
        header = p.get("header") or ""
        stated = parse_table_header(header)
        title = " ".join((p.get("event") or "").split())
        place = (p.get("place") or "").strip() or None
        country = p.get("country") or stated["country"]
        # 'title' is the name with its city, and sometimes the country code, on the end.
        name = title
        if country and name.endswith(" " + country):
            name = name[: -len(country)].strip()
        if place and name.lower().endswith(" " + place.lower()):
            name = name[: -len(place)].strip()
        m = re.search(r"(\d+)$", p.get("ref") or "")
        issue = int(m.group(1)) if p.get("source") == "twic" and m else 0
        final = stated["final"]
        scores = [x.get("score") for x in players]
        if not final and p.get("format") == "round-robin" and all(s is not None for s in scores):
            # An all-play-all is complete when the points add up to a whole
            # number of cycles.
            cycle = len(players) * (len(players) - 1) / 2
            total = sum(scores)
            final = int(total > 0 and abs(total / cycle - round(total / cycle)) < 1e-6)
        con.execute(
            "insert or ignore into xtab(obs_ref, source, ref, issue, title, name, place, country, start, end, "
            "format, category, ave, round_now, round_total, final, n_players, header, players_json) "
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"crosstables.json#{number}", p.get("source"), p.get("ref"), issue, title, name, place,
             country, p.get("start"), p.get("end") or p.get("start"),
             p.get("format"), stated["category"], stated["ave"], stated["round_now"], stated["round_total"],
             final or 0, len(players), header, json.dumps(players, ensure_ascii=False)))
        count += 1
    con.execute("insert or replace into meta(key, value) values ('xtab_stamp', ?)", (stamp,))
    con.execute("update event set xt_done = 0")
    con.commit()
    log(f"tables: indexed {count} dated crosstables from {CROSSTABLES_JSON.name}")
    return True


def clean_title(raw: str | None) -> str | None:
    raw = (raw or "").strip()
    return raw.upper() if raw.upper() in TITLES else OLD_TITLES.get(raw.lower())


def same_person_name(a: str, b: str) -> bool:
    """Could these two spellings name one person?

    'Wallace, John Paul' and 'Paul Wallace' can; so can 'Pavlov, Sergei' and
    'Pavlov Sergey', or 'Carlsen' and 'Carlsen, Magnus'. 'Gashimov, Sarkhan'
    and 'Gashimov, Vugar' cannot.
    """
    import difflib

    def words(text):
        return norm_name(re.sub(r"\([^)]*\)", " ", text)).split()

    def alike(x, y):
        if len(x) == 1 or len(y) == 1:  # an initial
            return x[0] == y[0]
        return x == y or difflib.SequenceMatcher(None, x, y).ratio() >= 0.75  # Aleksei / Alexei

    def within(small, large):
        return bool(small) and all(any(alike(tok, t) for t in large) for tok in small)

    if "," in a and "," in b:
        # Both are 'Family, Given': families must agree, and so must the first
        # given names ('Adams, David M' is not 'Adams, Michael').
        (fam_a, giv_a), (fam_b, giv_b) = ([words(part) for part in name.split(",", 1)] for name in (a, b))
        small, large = (fam_a, fam_b) if len(fam_a) <= len(fam_b) else (fam_b, fam_a)
        if not within(small, large):
            return False
        return not giv_a or not giv_b or alike(giv_a[0], giv_b[0])
    wa, wb = words(a), words(b)
    return within(*((wa, wb) if len(wa) <= len(wb) else (wb, wa)))


def assign_rows(ours: list[sqlite3.Row], rows: list[dict], closed: bool) -> dict[str, tuple[int, str]]:
    """Pair each of our players with a row of the table: {player key: (row index, how)}.

    'name' is a name match; 'paired' means the names disagree but the row can
    only be this player (same family name with another given name, or the one
    player and the one row left over in a closed field).
    """
    keys = [name_keys(r.get("name") or "") for r in rows]
    used: set[int] = set()
    out: dict[str, tuple[int, str]] = {}

    def claim(match, how):
        for p in ours:
            if p["key"] in out:
                continue
            full, fam = name_keys(p["name"])
            hits = [i for i, (rfull, rfam) in enumerate(keys) if i not in used and match(full, fam, rfull, rfam)]
            if len(hits) == 1:
                out[p["key"]] = (hits[0], how)
                used.add(hits[0])

    def wide(fam: set[str]) -> set[str]:
        return fam | {tok for key in fam for tok in key.split() if len(tok) >= 5}

    claim(lambda full, fam, rfull, rfam: bool(full and rfull and full & rfull), "name")
    claim(lambda full, fam, rfull, rfam: (not full or not rfull) and bool(wide(fam) & wide(rfam)), "name")
    if closed:  # only where the two fields are the same people
        claim(lambda full, fam, rfull, rfam: bool(fam & rfam), "paired")  # same family, different given name
    left = [p for p in ours if p["key"] not in out]
    free = [i for i in range(len(rows)) if i not in used]
    if closed and len(left) == 1 and len(free) == 1 and len(rows) >= 4:
        out[left[0]["key"]] = (free[0], "paired")
    return out


def stage_tables(con: sqlite3.Connection, limit: int | None = None) -> None:
    if not build_table_index(con):
        return
    rosters: dict[int, Roster] = {}
    con.execute("update event set xt_done = 1 where xt_done = 0 and origin = 'historical'")
    events = con.execute(
        "select * from event where xt_done = 0 and enriched = 1 order by coalesce(tag_start, ''), slug").fetchall()
    if limit:
        events = events[:limit]
    started = time.time()
    found = 0
    for i, ev in enumerate(events, 1):
        ours_rows = con.execute("select * from player where slug = ?", (ev["slug"],)).fetchall()
        ours = Roster(p["name"] for p in ours_rows)
        our_words = name_words(f"{ev['slug']} {ev['name']}")
        years = [y for y in (ev["slug_years"] or "").split(",") if y]
        if ev["date_suspect"]:
            lo, hi = (f"{years[0]}-01-01", f"{years[-1]}-12-31") if years else (None, None)
            earliest_end = None
        else:
            first, last = dt.date.fromisoformat(ev["tag_start"]), dt.date.fromisoformat(ev["tag_end"])
            lo = (first - dt.timedelta(days=40)).isoformat()   # the table's event may have begun earlier
            hi = (last + dt.timedelta(days=4)).isoformat()
            earliest_end = (first - dt.timedelta(days=4)).isoformat()
        accepted = []
        if lo and ours.n:
            for c in con.execute("select * from xtab where start between ? and ?", (lo, hi)).fetchall():
                if earliest_end and c["end"] < earliest_end:
                    continue
                if c["id"] not in rosters:
                    rosters[c["id"]] = Roster(r.get("name") for r in json.loads(c["players_json"]))
                theirs = rosters[c["id"]]
                common = roster_overlap(ours, theirs)
                if max(ours.n, theirs.n) <= 3:
                    # A match or a three-player event: everyone must be accounted
                    # for, allowing one renamed player who shares a name token.
                    if ours.n != theirs.n:
                        continue
                    if common < ours.n:
                        mine = [w for (f, m), w in zip(ours.items, ours.words) if not theirs.lists(f, m)]
                        other = [w for (f, m), w in zip(theirs.items, theirs.words) if not ours.lists(f, m)]
                        if len(mine) == 1 and len(other) == 1 and mine[0] & other[0]:
                            common += 1
                    if common < ours.n:
                        continue
                    kind, score = "same", 1.0
                else:
                    if common < 2:
                        continue
                    jaccard = common / (ours.n + theirs.n - common)
                    contained = common / min(ours.n, theirs.n)
                    by_family = family_overlap(ours, theirs)
                    family_jaccard = by_family / (ours.n + theirs.n - by_family)
                    if jaccard >= 0.6:
                        kind, score = "same", jaccard
                    elif family_jaccard >= 0.8 and common >= 0.5 * min(ours.n, theirs.n):
                        # Half the field matches exactly and nearly all of it
                        # by family name: initials and transliterations differ.
                        kind, score = "same", family_jaccard * 0.9
                    elif contained >= 0.8 and common >= 8:
                        kind, score = ("part" if theirs.n < ours.n else "whole"), contained
                    else:
                        continue
                days_off = 0
                if not ev["date_suspect"]:
                    days_off = (abs((dt.date.fromisoformat(c["start"]) - first).days)
                                + abs((dt.date.fromisoformat(c["end"]) - last).days))
                accepted.append({"row": c, "kind": kind, "score": score, "days_off": days_off,
                                 "words": len(our_words & name_words(c["title"]))})

        # One event appears in several weekly issues; the latest table stands
        # for the group.
        groups: dict[tuple, dict] = {}
        for a in accepted:
            c = a["row"]
            key = (c["source"], norm_name(c["title"]), c["start"], c["end"])
            if key not in groups or (c["issue"], c["final"]) > (groups[key]["row"]["issue"], groups[key]["row"]["final"]):
                groups[key] = a
        # Do the table's scores equal the points our games give each player?
        # That is what tells a festival's classical event from its blitz when
        # both have one roster: 'fit' is the share of listed scores that agree.
        for a in groups.values():
            rows = json.loads(a["row"]["players_json"])
            a["pairs"] = assign_rows(ours_rows, rows, closed=a["kind"] == "same")
            scored = [(p["points"], rows[a["pairs"][p["key"]][0]].get("score")) for p in ours_rows
                      if p["key"] in a["pairs"] and rows[a["pairs"][p["key"]][0]].get("score") is not None]
            a["fit"] = (sum(abs(mine - theirs) < 0.01 for mine, theirs in scored) / len(scored)
                        if len(scored) >= 2 else None)
            # Short of an exact fit, the scores can still be consistent with one
            # event. A finished table can only show a player MORE points than
            # our file does (Chess.com broadcast part of the event); a table
            # caught mid-event can only show FEWER. The blitz table set against
            # the classical games breaks both ways.
            if a["fit"] is None:
                a["consistent"] = True
            elif a["row"]["final"]:
                a["consistent"] = sum(mine > theirs + 0.01 for mine, theirs in scored) <= 0.1 * len(scored)
            else:
                a["consistent"] = sum(mine < theirs - 0.01 for mine, theirs in scored) <= 0.1 * len(scored)
        fit_rank = lambda a: -1.0 if a["fit"] is None else -round(a["fit"], 1) - 1
        ranked = sorted(groups.values(), key=lambda a: (
            {"same": 0, "whole": 1, "part": 2}[a["kind"]], -round(a["score"], 1), not a["consistent"],
            fit_rank(a), -a["words"], a["days_off"], XT_SOURCE_PRIORITY.get(a["row"]["source"], 9),
            -a["row"]["issue"]))

        fields = dict.fromkeys(XT_FIELDS)
        con.execute(f"update player set {', '.join(f'{k} = null' for k in XT_PLAYER_FIELDS)} where slug = ?",
                    (ev["slug"],))
        if ranked:
            found += 1
            best = ranked[0]
            c = best["row"]
            # A sibling event (the blitz beside the rapid) has the same roster
            # and source; the name is trusted only when ours favours one of them.
            rivals = [a for a in ranked[1:] if a["kind"] == best["kind"] and a["row"]["source"] == c["source"]
                      and round(a["score"], 1) == round(best["score"], 1)
                      and norm_name(a["row"]["title"]) != norm_name(c["title"])]
            fit = best["fit"]
            if fit is not None and fit >= 0.8:
                # The standings are ours. A sibling whose scores fit as well
                # would leave it open; one that does not is ruled out.
                name_ok = not any(a["fit"] is not None and a["fit"] >= 0.8 for a in rivals)
            elif not best["consistent"]:
                name_ok = False  # scores that cannot belong to these games: another event
            else:
                name_ok = not any(a["words"] >= best["words"] for a in rivals)
            if best["kind"] != "same" and (fit is None or fit < 0.8):
                name_ok = name_ok and best["words"] >= 1
            rows = json.loads(c["players_json"])
            pairs = best["pairs"]
            # Which rating pool does the table print? A rapid or blitz event's
            # table carries those ratings, which are not comparable with the
            # standard list the 2300 line is drawn on.
            both = [(p["elo_ely"], rows[pairs[p["key"]][0]].get("rating")) for p in ours_rows
                    if p["key"] in pairs and p["elo_ely"] and isinstance(rows[pairs[p["key"]][0]].get("rating"), int)]
            pool = None
            if len(both) >= 4:
                pool = "standard" if sum(abs(a - b) <= 5 for a, b in both) >= 0.5 * len(both) else "other"
            for p in ours_rows:
                if p["key"] not in pairs:
                    continue
                index, how = pairs[p["key"]]
                r = rows[index]
                rating = r.get("rating") if isinstance(r.get("rating"), int) and r["rating"] > 0 else None
                id_ok = None
                if p["fide_id"]:
                    # The id is discarded only on evidence, and the evidence is
                    # the rating the table prints (when it is from the standard
                    # list). Same name: the table rates the player far above
                    # that id's rating, or at master level when Elysium knows
                    # the id and has no rating for it then (a namesake).
                    # Different given name: the ratings differ (Adams, David M
                    # is not Adams, Michael; Kramnik,W is Kramnik, Vladimir).
                    comparable = rating if pool != "other" else None
                    gap = comparable - p["elo_ely"] if p["elo_ely"] and comparable else None
                    unrated_then = bool(p["ely_name"]) and not p["elo_ely"] and (comparable or 0) >= 2200
                    if same_person_name(p["name"], r.get("name") or ""):
                        id_ok = int(not (unrated_then or (gap is not None and gap > 300)))
                    else:
                        id_ok = int(not (unrated_then or (gap is not None and abs(gap) > 100)))
                fed = r.get("fed") if re.fullmatch(r"[A-Z]{3}", r.get("fed") or "") else None
                con.execute(
                    "update player set xt_name = ?, xt_title = ?, xt_fed = ?, xt_rating = ?, xt_score = ?, "
                    "xt_rank = ?, xt_id_ok = ? where slug = ? and key = ?",
                    (" ".join((r.get("name") or "").split()), clean_title(r.get("title")), fed, rating,
                     r.get("score") if c["final"] else None,
                     r.get("rank") if c["final"] and isinstance(r.get("rank"), int) else None,
                     id_ok, ev["slug"], p["key"]))
            # Another source's table of the same event may know the place or
            # country this one omits; siblings share both, so that is safe.
            def also(column):
                near = lambda a: (abs((dt.date.fromisoformat(a["row"]["start"]) - dt.date.fromisoformat(c["start"])).days) <= 2)
                return c[column] or next((a["row"][column] for a in ranked
                                          if a["kind"] == "same" and a["row"][column] and near(a)), None)

            fields.update(
                xt_id=c["id"], xt_source=c["source"], xt_ref=c["ref"], xt_obs=c["obs_ref"], xt_title=c["title"],
                xt_name=c["name"], xt_name_ok=int(name_ok), xt_place=also("place"), xt_country=also("country"),
                xt_start=c["start"], xt_end=c["end"], xt_format=c["format"], xt_category=c["category"],
                xt_avg=c["ave"], xt_rounds=c["round_total"], xt_final=c["final"], xt_kind=best["kind"],
                xt_score=round(best["score"], 3), xt_rows=len(pairs), xt_pool=pool,
                xt_fit=None if fit is None else round(fit, 3))
        sets = ", ".join(f"{k} = ?" for k in fields)
        con.execute(f"update event set xt_done = 1, {sets} where slug = ?", [*fields.values(), ev["slug"]])
        if i % 200 == 0:
            con.commit()
            log(f"  tables {i}/{len(events)}  found {found}  ({time.time() - started:.0f}s)")
    con.commit()
    log(f"tables: {found} of {len(events)} events have a crosstable in the archive ({time.time() - started:.0f}s)")


# --------------------------------------------------------------------------
# classify
# --------------------------------------------------------------------------

def resolve_by_event(con: sqlite3.Connection) -> None:
    """Rate players who have no FIDE id through the event's Elysium record.

    Chess.com's older files name many players by surname only ("Carlsen
    (NOR)") or without an id ("Garry Kasparov"), so nothing could be looked up
    for them and strong events were left unjudged. Where the event itself has
    been matched in Elysium, its games say who those players are: a name of
    ours that fits exactly one player of that event is that player, and gets
    that player's standard rating for the month. player.ely_by_event records
    the attempt (1 found, 0 not)."""
    todo = con.execute(
        "select distinct e.slug from event e join player p on p.slug = e.slug "
        "where e.ely_event_id is not null and e.origin is null and p.fide_id is null "
        "and p.elo_ely is null and p.ely_by_event is null").fetchall()
    if not todo:
        return
    ely = Elysium()
    cur = ely.cur
    found = 0
    for row in todo:
        ev = con.execute("select * from event where slug = ?", (row["slug"],)).fetchone()
        start = ev["ely_start"] if ev["date_suspect"] and ev["ely_start"] else (ev["tag_start"] or ev["ely_start"])
        month = start[:7] if start and len(start) >= 7 else (f"{start}-12" if start else None)
        games = cur.execute("select id, white_player_id, black_player_id from game where event_id = ?",
                            (ev["ely_event_id"],)).fetchall()
        sides = {g[0]: (g[1], g[2]) for g in games}
        known: dict[int, set[str]] = defaultdict(set)   # Elysium player -> names printed for them here
        ids = list(sides)
        for j in range(0, len(ids), 500):
            part = ids[j:j + 500]
            for game_id, white, black in cur.execute(
                    "select game_id, raw_white, raw_black from game_observation "
                    f"where id in ({','.join('?' * len(part))})", part):
                if game_id in sides:
                    for player_id, name in zip(sides[game_id], (white, black)):
                        if player_id and name:
                            known[player_id].add(name)
        keyed = {pid: [name_keys(n) for n in names] for pid, names in known.items()}
        for p in con.execute("select * from player where slug = ? and fide_id is null and elo_ely is null "
                             "and ely_by_event is null", (ev["slug"],)).fetchall():
            full, fam = name_keys(p["name"])
            hits = {pid for pid, keys in keyed.items()
                    if any((full & their_full) if (full and their_full) else (fam & their_fam)
                           for their_full, their_fam in keys)}
            rating = None
            if len(hits) == 1 and month:
                chain, pid = [], next(iter(hits))
                while pid and len(chain) < 6:   # follow soft merges to the live identity
                    chain.append(pid)
                    merged = cur.execute("select merged_into from player where id = ?", (pid,)).fetchone()
                    pid = merged[0] if merged else None
                rating = ely.rating_at(",".join(map(str, chain)), month)
            if rating:
                found += 1
                con.execute("update player set elo_ely = ?, elo_ely_period = ?, ely_by_event = 1 "
                            "where slug = ? and key = ?", (rating[0], rating[1], ev["slug"], p["key"]))
            else:
                con.execute("update player set ely_by_event = 0 where slug = ? and key = ?", (ev["slug"], p["key"]))
        if month and not ev["rating_month"]:
            con.execute("update event set rating_month = ? where slug = ?", (month, ev["slug"]))
    con.commit()
    log(f"classify: {found} players without a FIDE id rated through their event's Elysium record ({len(todo)} events)")


def resolve_by_name(con: sqlite3.Connection) -> None:
    """Last resort for events still unjudged for lack of ratings: find their
    id-less players among Elysium's rated players by name.

    Only players who have ever been rated 2200 or more are considered, and a
    name is accepted only when it cannot be anyone else:
      - a full name ("Garry Kasparov") that fits one such player with a rating
        at the time; or several, of whom exactly one is within 60 points of
        the file's Elo tag;
      - a bare surname ("Carlsen (NOR)") that fits exactly one player rated
        2500 or more at the time. Title matches and elite events are what
        Chess.com left without ids, and at that level a surname is one person.
    player.ely_by_name records the attempt (1 found, 0 not)."""
    todo = con.execute(
        "select distinct e.slug from event e join player p on p.slug = e.slug "
        "where e.decision = 'review' and e.origin is null and p.fide_id is null and p.elo_ely is null "
        "and p.ely_by_name is null").fetchall()
    if not todo:
        return
    ely = Elysium()
    cur = ely.cur
    strong = [r[0] for r in cur.execute(
        "select distinct player_id from rating where system = 'combined' and scope = 'standard' and value >= 2200")]
    by_full: dict[str, set[int]] = defaultdict(set)
    by_family: dict[str, set[int]] = defaultdict(set)
    person: dict[int, tuple[str, bool]] = {}   # id -> (name as displayed, has a birth date)
    by_words: dict[frozenset, set[int]] = defaultdict(set)   # the name's words, in any order
    for j in range(0, len(strong), 900):
        part = strong[j:j + 900]
        for pid, display, birth in cur.execute(
                f"select id, display_name, birth_date from player where id in ({','.join('?' * len(part))})", part):
            person[pid] = (norm_name(display or ""), bool(birth))
            by_words[frozenset(person[pid][0].split())].add(pid)
            full, fam = name_keys(display or "")
            for key in full:
                by_full[key].add(pid)
            if "," in (display or ""):
                by_family[norm_name(display.split(",")[0])].add(pid)
    found = 0
    for row in todo:
        ev = con.execute("select * from event where slug = ?", (row["slug"],)).fetchone()
        start = ev["ely_start"] if ev["date_suspect"] and ev["ely_start"] else (ev["start"] or ev["tag_start"])
        if ev["date_suspect"] and not ev["ely_start"] and ev["slug_years"]:
            start = ev["slug_years"].split(",")[0]   # the PGN's dates are import dates; the name gives the year
        month = ev["rating_month"] if ev["rating_month"] and not ev["date_suspect"] else None
        month = month or (start[:7] if start and len(start) >= 7 else (f"{start}-07" if start else None))
        for p in con.execute("select * from player where slug = ? and fide_id is null and elo_ely is null "
                             "and ely_by_name is null", (ev["slug"],)).fetchall():
            full, fam = name_keys(p["name"])
            rating = None
            if month:
                words = frozenset(norm_name(re.sub(r"\([^)]*\)", " ", p["name"])).split())
                if full and by_words.get(words):
                    # "Viswanathan Anand" is "Anand, Viswanathan" and nobody else.
                    candidates = set(by_words[words])
                elif full:
                    candidates = set().union(*(by_full.get(key, set()) for key in full))
                else:
                    candidates = set().union(*(by_family.get(key, set()) for key in fam))
                # A rating more than five years old at the time is not a rating then:
                # a "classics" page dated to its 2020 upload must not rate Morphy.
                rated = [(pid, r) for pid in candidates
                         if (r := ely.rating_at(str(pid), month)) and int(r[1][:4]) >= int(month[:4]) - 5]
                # Elysium holds some players twice under one name (the FIDE
                # record and an older import). Records with the same displayed
                # name are one person: the record with a birth date, then the
                # one with the more recent rating, speaks for them.
                one_each: dict[str, tuple] = {}
                for pid, r in rated:
                    rank = (person[pid][1], r[1])
                    if person[pid][0] not in one_each or rank > one_each[person[pid][0]][0]:
                        one_each[person[pid][0]] = (rank, pid, r)
                rated = [(pid, r) for _, pid, r in one_each.values()]
                if full:
                    if len(rated) > 1 and p["elo_first"]:
                        rated = [(pid, r) for pid, r in rated if abs(r[0] - p["elo_first"]) <= 60]
                else:
                    rated = [(pid, r) for pid, r in rated if r[0] >= 2500]
                if len(rated) == 1:
                    rating = rated[0][1]
            if rating:
                found += 1
                con.execute("update player set elo_ely = ?, elo_ely_period = ?, ely_by_name = 1 "
                            "where slug = ? and key = ?", (rating[0], rating[1], ev["slug"], p["key"]))
            else:
                con.execute("update player set ely_by_name = 0 where slug = ? and key = ?", (ev["slug"], p["key"]))
        if month and (not ev["rating_month"] or ev["date_suspect"]):
            con.execute("update event set rating_month = ? where slug = ?", (month, ev["slug"]))
    con.commit()
    log(f"classify: {found} players without a FIDE id rated by name among Elysium's rated players ({len(todo)} events)")


def stage_classify(con: sqlite3.Connection) -> None:
    resolve_by_event(con)
    resolve_by_name(con)
    events = con.execute("select * from event").fetchall()
    for ev in events:
        # Dates: the matched crosstable states the event's dates; otherwise the
        # PGN's own tags, unless they are contradicted.
        years = [y for y in (ev["slug_years"] or "").split(",") if y]
        table_ok = bool(ev["xt_id"] and ev["xt_name_ok"])
        if table_ok and ev["xt_start"]:
            start, end, date_basis = ev["xt_start"], ev["xt_end"], f"table:{ev['xt_source']}"
        elif not ev["date_suspect"]:
            start, end, date_basis = ev["tag_start"], ev["tag_end"], "pgn-tags"
        elif ev["ely_start"]:
            start, end, date_basis = ev["ely_start"], ev["ely_end"] or ev["ely_start"], f"elysium:{ev['ely_source']}"
        elif ev["est_month_first"] and not ev["elo_suspect"]:
            start, end, date_basis = ev["est_month_first"], ev["est_month_last"], "rating-list-month"
        elif years:
            start, end, date_basis = years[0], years[-1], "name-year"
        else:
            start = end = None
            date_basis = "unknown"

        # Chess.com sometimes attached the wrong person's FIDE id to a name
        # (Anand became a 2000-born namesake, Gashimov a child). In a small
        # strong field such a player stands out. Their id, rating and title
        # are not used when the evidence is one of:
        #   - 800+ points below the field;
        #   - 400+ below it and absent from the matched Elysium roster;
        #   - under twelve at the time, with no rating near the field's.
        # A weak local entrant who IS in the roster stays as stated.
        players = con.execute("select * from player where slug = ?", (ev["slug"],)).fetchall()
        # Are this event's Elo tags standard ratings? Compare them with the
        # standard list wherever a player has both: exact agreement where
        # Elysium covers the month, within 100 points where it stops short.
        both = [abs(p["elo_first"] - p["elo_ely"]) for p in players if p["elo_first"] and p["elo_ely"]]
        covered = bool(ev["rating_month"]) and ev["rating_month"] <= ELYSIUM_LAST_MONTH
        tags_standard = int(not ev["elo_suspect"] and len(both) >= 4
                            and sum(d <= (5 if covered else 100) for d in both) >= 0.8 * len(both))
        historical = ev["origin"] == "historical"
        if historical:
            tags_standard = 1  # Edo / Chessmetrics ratings, trusted as tagged
        ev = {**dict(ev), "tags_standard": tags_standard}
        rated = [(p, *rating_for(ev, p, ignore_doubt=True)) for p in players]
        values = [v for _, v, _ in rated if v]
        doubts: dict[str, str] = {}
        if values and len(players) <= 20 and not historical:
            field = max(values) if len(values) <= 3 else statistics.median(values)
            if field >= 2400:
                for p, value, _ in rated:
                    if not p["fide_id"] or p["xt_name"]:
                        continue  # a player the crosstable lists is settled there
                    age = int(start[:4]) - p["birth_year"] if start and p["birth_year"] else None
                    gap = field - value if value is not None else None
                    if gap is not None and gap > 800:
                        doubts[p["key"]] = f"rated {value} in a field around {int(field)}"
                    elif gap is not None and gap > 400 and p["in_roster"] == 0:
                        doubts[p["key"]] = (f"rated {value} in a field around {int(field)}, and not in the "
                                            f"matched {ev['ely_source']} roster")
                    elif age is not None and age < 12 and (gap is None or gap > 200):
                        doubts[p["key"]] = f"would have been {age} years old"
        con.execute("update player set doubt = null where slug = ?", (ev["slug"],))
        for key, why in doubts.items():
            con.execute("update player set doubt = ? where slug = ? and key = ?", (why, ev["slug"], key))
        identity_doubt = "; ".join(f"{p['name']} ({p['fide_id']}): {doubts[p['key']]}"
                                   for p in players if p["key"] in doubts) or None

        ratings, backfilled, sources = [], 0, set()
        for p, value, source in rated:
            if value and p["key"] not in doubts:
                ratings.append(value)
                sources.add(source)
                backfilled += source == "elysium"
        avg = computed = mean_int(ratings)
        share = len(ratings) / ev["players"] if ev["players"] else 0
        basis = "+".join(sorted(sources)) or None
        category = fide_category(avg)
        stated = False
        if table_ok and ev["xt_kind"] == "same" and ev["xt_avg"] and ev["xt_pool"] != "other":
            # The crosstable prints the field's average and category itself.
            avg, basis, stated = ev["xt_avg"], f"stated:{ev['xt_source']}", True
            category = ev["xt_category"] if ev["xt_category"] is not None else fide_category(avg)
        elif share < MIN_RATED_SHARE and ev["ely_avg"]:
            # The PGN names players without ratings, but the matched Elysium
            # crosstable states them.
            avg, basis = ev["ely_avg"], f"elysium:{ev['ely_source']}"
            category = fide_category(avg)

        # The floor. A player Elysium knows by a believed FIDE id but has no
        # rating for was unrated at the time; anyone else without a rating is
        # simply unknown to us.
        below = sorted(r for r in ratings if r < RATING_FLOOR)
        unrated_then = unknown = 0
        for p, value, _ in rated:
            if value and p["key"] not in doubts:
                continue
            believed = p["fide_id"] and p["key"] not in doubts and p["xt_id_ok"] != 0 and p["ely_name"]
            if believed and covered:
                unrated_then += 1
            else:
                unknown += 1

        hay = norm_name(ev["name"] if historical else f"{ev['slug']} {ev['name']}")
        series = None if historical else next(
            (label for label, pat in SERIES_PATTERNS.items() if pat.search(hay)), None)
        if series:
            decision, reason = "series", f"{series} has its own archive"
        elif not historical and ENGINE_PATTERN.search(hay):
            decision, reason = "engine", "engine event"
        elif not historical and TEST_PATTERN.search(hay):
            decision, reason = "test", "a broadcast test, not an event"
        elif not historical and ratings and max(ratings) > 2900:
            decision, reason = "engine", f"a participant is rated {max(ratings)}: an engine"
        elif avg is None:
            decision, reason = "review", "no ratings in the PGN or in Elysium"
        elif stated:
            decision = "keep" if avg >= KEEP_AVERAGE else "cull"
            reason = f"average {avg} as stated in the {ev['xt_source']} crosstable ({ev['xt_ref']})"
        elif basis and basis.startswith("elysium:"):
            decision = "keep" if avg >= KEEP_AVERAGE else "cull"
            reason = f"average {avg} from the matched {ev['ely_source']} crosstable"
        elif share < MIN_RATED_SHARE:
            decision, reason = "review", f"only {len(ratings)} of {ev['players']} players have a rating"
        else:
            decision = "keep" if avg >= KEEP_AVERAGE else "cull"
            reason = f"average {avg} over {len(ratings)} of {ev['players']} players"
        if doubts and decision in ("keep", "cull"):
            reason += f", leaving out {len(doubts)} doubted identit{'y' if len(doubts) == 1 else 'ies'}"
        if decision == "keep" and below and ev["players"] <= FLOOR_EXEMPT_PLAYERS:
            decision = "cull"
            reason = (f"{len(below)} player{'s' if len(below) != 1 else ''} under {RATING_FLOOR} "
                      f"(lowest {below[0]}); average {avg}")
        if (decision in ("keep", "cull") and ev["ely_avg"] and ev["ely_kind"] == "same"
                and (avg >= KEEP_AVERAGE) != (ev["ely_avg"] >= KEEP_AVERAGE)):
            # Two independent event-time figures on opposite sides of the line.
            decision = "review"
            reason = f"average {avg} here but {ev['ely_avg']} in the matched {ev['ely_source']} crosstable"
        if (decision == "cull" and ev["players"] > FLOOR_EXEMPT_PLAYERS and OLYMPIAD_PATTERN.search(hay)
                and not OLYMPIAD_EXCLUDED.search(hay)):
            decision, reason = "keep", f"an Olympiad, kept by name ({reason})"
        display = ev["xt_name"] if table_ok and ev["xt_name"] else ev["name"]
        # "... Live", "... Secret": Chess.com's label for the copy, not part of the event's name.
        display = re.sub(r"(\s+(Live|Secret|Broadcast))+$", "", display, flags=re.I) or display
        if historical and start:
            # Some files carry another event's name: "31st Swiss Chess
            # Association Championship - Biel 1927" on a Biel event of 1977.
            # A name whose every year is off by more than one from the games'
            # is not this event's; the town and year stand in for it.
            named = [int(y) for y in re.findall(r"(?<!\d)(1[89]\d\d)(?!\d)", display)]
            if named and all(abs(y - int(start[:4])) > 1 for y in named):
                town = re.sub(r"\s+[A-Z]{3}$", "", re.sub(r"\([^)]*\)", "", ev["site_tag"] or "").strip())
                display = f"{town or 'Tournament'} {start[:4]}"
        con.execute(
            "update event set rated_players = ?, backfilled_players = ?, avg_rating = ?, avg_basis = ?, "
            "avg_computed = ?, min_rating = ?, max_rating = ?, below_floor = ?, category = ?, decision = ?, "
            "reason = ?, start = ?, end = ?, date_basis = ?, identity_doubt = ?, display_name = ?, "
            "tags_standard = ?, unrated_then = ?, unknown_rating = ? where slug = ?",
            (len(ratings), backfilled, avg, basis, computed, min(ratings) if ratings else None,
             max(ratings) if ratings else None, len(below),
             category, decision, reason, start, end, date_basis, identity_doubt, display,
             tags_standard, unrated_then, unknown, ev["slug"]))
    con.commit()
    drop_twins(con)
    assign_publish_names(con)
    counts = Counter(r["decision"] for r in con.execute("select decision from event"))
    log("classify: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))


# --------------------------------------------------------------------------
# published file names: YYYY-MM-DD-site-event
# --------------------------------------------------------------------------

ONLINE_HOSTS = (("chess.com", "chess-com"), ("chess24", "chess24-com"), ("lichess", "lichess-org"),
                ("chessbase", "chessbase"), ("playchess", "playchess"), ("tornelo", "tornelo"),
                ("internet", "online"), ("online", "online"))
VENUE_WORDS = re.compile(r"\d|\b(hotel|club|cent(er|re|ro)|resort|school|hall|universit|academy|museum|palace|casino|"
                         r"library|arena|stadium|complex|campus|college|institute|room|floor|street|avenue|house|"
                         r"mercado|sala)\b", re.I)
COUNTRY_NAMES = {"china", "sweden", "poland", "norway", "slovakia", "czech republic", "iceland", "india", "germany",
                 "france", "spain", "italy", "hungary", "russia", "usa", "england", "netherlands", "denmark",
                 "serbia", "croatia", "romania", "ukraine", "turkey", "greece", "austria", "switzerland"}


def file_slug(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def place_slug(place: str | None, fallback: str = "chess-com") -> str:
    """The town in a place string, as a filename part: 'Saint Louis USA' and
    'Saint Louis, US' -> 'saint-louis'; 'Hotel Habana Libre, La Habana, CU' ->
    'la-habana'. An online host -> 'chess-com' and the like; and with no place
    on record the site is Chess.com, which carried the event."""
    text = re.sub(r"\([^)]*\)", " ", place or "").strip()
    low = text.lower()
    for needle, slug in ONLINE_HOSTS:
        if needle in low:
            return slug
    parts = [part.strip() for part in re.split(r",|\s-\s|/", text) if part.strip()]
    if len(parts) > 1 and (re.fullmatch(r"[A-Za-z]{2,3}", parts[-1]) or parts[-1].lower() in COUNTRY_NAMES):
        parts.pop()                                              # 'Napoli, IT', 'xinghua, China'
    parts = [re.sub(r"\s+[A-Z]{3}$", "", part) for part in parts]  # 'Warsaw POL', 'Van Nuys, CA USA'
    towns = [part for part in parts if not VENUE_WORDS.search(part) and file_slug(part)]
    return file_slug(towns[0] if towns else (parts[0] if parts else "")) or (fallback if place is None or not text else "chess-com")


def publish_name(start: str, place: str | None, name: str, slug: str, fallback: str = "chess-com",
                 dedupe: bool = False) -> str:
    """YYYY-MM-DD-site-event. An unknown month or day is 00. The event part is
    the name without the year the date already gives."""
    words = [w for w in file_slug(name).split("-") if w != start[:4]]
    town = place_slug(place, fallback).split("-")
    if dedupe and words[:len(town)] == town:
        # "Kiev" at Kiev, "Lodz Makarczyk Memorial" at Lodz: the site already says it.
        words = words[len(town):] or ["tournament"]
    while len(words) > 1 and words[-1] in ("live", "secret", "broadcast"):
        words.pop()   # Chess.com's label for a copy of the event, not part of its name
    event = "-".join(words) or "-".join(w for w in file_slug(slug).split("-") if w != start[:4]) or "event"
    return f"{(start + '-00-00')[:10]}-{place_slug(place, fallback)}-{event}"


def twin_base(slug: str) -> str:
    while TWIN_SUFFIX.search(slug):
        slug = TWIN_SUFFIX.sub("", slug)
    return slug


def drop_twins(con: sqlite3.Connection) -> None:
    """Of the kept copies of one event (the event and its "-live", "-secret"
    or "-broadcast" ids) keep the one with the most games; the plain id wins a
    tie. A "-live" copy is often the fuller one."""
    groups: dict[str, list] = defaultdict(list)
    for ev in con.execute("select slug, games from event where decision = 'keep'"):
        groups[twin_base(ev["slug"]).lower()].append(ev)
    for members in groups.values():
        if len(members) < 2:
            continue
        best = max(members, key=lambda ev: (ev["games"], not TWIN_SUFFIX.search(ev["slug"]), ev["slug"]))
        for ev in members:
            if ev["slug"] != best["slug"]:
                con.execute("update event set decision = 'twin', reason = ? where slug = ?",
                            (f"another copy of {best['slug']} ({best['games']} games against {ev['games']})", ev["slug"]))

    # An old event Chess.com also carries (the 1972 Spassky-Fischer match) is
    # kept once, from the historical collection: the user's call, 2026-10-05.
    historical: dict[int, list] = defaultdict(list)
    for ev in con.execute("select slug, start, games from event where decision = 'keep' and origin = 'historical'"):
        historical[int(ev["start"][:4])].append(ev)

    def roster(slug: str) -> Roster:
        return Roster(r["name"] for r in con.execute("select name from player where slug = ?", (slug,)))

    def same_event(ev, ours: Roster):
        year = int(ev["start"][:4])
        for h in historical.get(year, []):
            theirs = roster(h["slug"])
            common = roster_overlap(ours, theirs) if theirs.n == ours.n else 0
            # The same field; or a match of as many games in which one name
            # agrees and the other is spelled another way (Korchnoi, Kortschnoj).
            if common == ours.n or (ours.n == 2 and common == 1 and h["games"] == ev["games"]):
                return h["slug"]
        if ours.n == 2 and len(ev["start"]) == 4:
            # Chess.com dates some old matches by guesswork ("1886 Lasker vs
            # Steinitz" is the 1894 match): the same two players over the same
            # number of games within a decade is the same match. Only where
            # our date is a bare year: Kasparov and Karpov played four 24-game
            # matches, and a dated one is not to be mistaken for another.
            for other in range(year - 10, year + 11):
                for h in historical.get(other, []):
                    if h["games"] == ev["games"] and (theirs := roster(h["slug"])).n == 2 \
                            and roster_overlap(ours, theirs) == 2:
                        return h["slug"]
        return None

    for ev in con.execute("select slug, start, games from event where decision = 'keep' and origin is null "
                          "and start < '1991'").fetchall():
        same = same_event(ev, roster(ev["slug"]))
        if same:
            con.execute("update event set decision = 'twin', reason = ? where slug = ?",
                        (f"the historical collection has this event: {same}", ev["slug"]))
    con.commit()


def assign_publish_names(con: sqlite3.Connection) -> None:
    """Give every kept event its published file name. Two events that would
    share one (a festival's classical and blitz under one crosstable name, a
    broadcast and its "-live" twin) fall back to Chess.com's own names for
    them, and after that to a number."""
    events = con.execute("select * from event where decision = 'keep' and start is not null order by slug").fetchall()

    def place(ev):
        return (ev["xt_place"] if ev["xt_id"] and ev["xt_name_ok"] else None) or ev["ely_place"] or ev["site_tag"]

    def named(ev, name):
        historical = ev["origin"] == "historical"
        # A historical file's own name ends in a hash; it is no fallback for the event's name.
        return publish_name(ev["start"], place(ev), name, "event" if historical else ev["slug"],
                            "unknown" if historical else "chess-com", dedupe=historical)

    names = {ev["slug"]: named(ev, ev["display_name"] or ev["name"]) for ev in events}
    shared = {name for name, n in Counter(names.values()).items() if n > 1}
    for ev in events:
        if names[ev["slug"]] in shared and ev["origin"] != "historical":
            names[ev["slug"]] = named(ev, ev["name"])
    taken: Counter = Counter()
    con.execute("update event set pub_name = null")
    for ev in events:
        name = names[ev["slug"]]
        taken[name] += 1
        if taken[name] > 1:
            name = f"{name}-{taken[name]}"
        con.execute("update event set pub_name = ? where slug = ?", (name, ev["slug"]))
    con.commit()


# --------------------------------------------------------------------------
# ctml
# --------------------------------------------------------------------------

_XML_BAD = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f]")


def esc(s) -> str:
    return _xml_escape(_XML_BAD.sub("", str(s)), {'"': "&quot;"})


def sha16(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def date_element(value: str, raw: str | None = None) -> str:
    """'2010-01-26' / '2010-01' / '2010' -> the matching CTML partial-date element."""
    extra = f' raw="{esc(raw)}"' if raw else ""
    parts = value.split("-")
    if len(parts) == 3:
        y, m, d = map(int, parts)
        return f'<ctml:day y="{y}" m="{m}" d="{d}" iso="{value}"{extra}/>'
    if len(parts) == 2:
        return f'<ctml:month y="{int(parts[0])}" m="{int(parts[1])}"{extra}/>'
    return f'<ctml:year y="{int(parts[0])}"{extra}/>'


def person_name_xml(name: str) -> str:
    name = " ".join((name or "").split()) or "Unknown"
    if "," in name:
        family, given = (x.strip() for x in name.split(",", 1))
        body = f"<ctml:family>{esc(family)}</ctml:family>"
        if given:
            body += f"<ctml:given>{esc(given)}</ctml:given>"
    elif " " in name:
        body = f"<ctml:unstructured>{esc(name)}</ctml:unstructured>"
    else:
        body = f"<ctml:family>{esc(name)}</ctml:family>"
    return f'<ctml:name display="{esc(name)}">{body}</ctml:name>'


def compact(value: str) -> str:
    return (value.replace("-", "") + "0000")[:8]


def event_signature(ev: sqlite3.Row, file_sig: tuple) -> str:
    keys = ("name", "start", "end", "date_basis", "avg_rating", "avg_basis", "category", "format",
            "ely_event_id", "ely_place", "ely_name", "elo_suspect", "date_suspect", "rating_month",
            "identity_doubt", "display_name", "xt_id", "xt_name_ok", "xt_rows", "xt_final", "avg_computed",
            "tags_standard", "xt_fit", "xt_pool", "xt_place", "xt_country", "xt_rounds", "site_tag", "origin")
    blob = json.dumps([VERSION, file_sig, [ev[k] for k in keys]], default=str)
    return hashlib.sha1(blob.encode()).hexdigest()


def load_fingerprinter():
    try:
        sys.path.insert(0, str(CTML_ROOT / "readers"))
        from fingerprint import SCHEME, FingerprintAccumulator  # type: ignore
        return SCHEME, FingerprintAccumulator
    except Exception:
        return None, None


def read_source_game(text: str):
    """Parse one game of a Chess.com PGN. Returns (game, variant) or (None, None)."""
    import chess.pgn

    # Chess.com writes the no-castling start position with the castling field
    # left out altogether ('... w - 0 1'); put the '-' back so it is a legal FEN.
    text = re.sub(r'(\[FEN "[^" ]+ [wb]) (- \d+ \d+"\])', r"\1 - \2", text, count=1)
    # ...and has let a stray character trail one ('... - 2 12`').
    text = re.sub(r'(\[FEN "[^"]+ \d+ \d+)[^0-9" ]+("\])', r"\1\2", text, count=1)
    game = chess.pgn.read_game(io.StringIO(text))
    if game is None:
        return None, None
    if game.errors and "FEN" in game.headers:
        # A set-up position that only parses with Chess960 castling.
        retry = chess.pgn.read_game(
            io.StringIO(re.sub(r'\[Variant "[^"]*"\]', '[Variant "Chess960"]', text, count=1)))
        if retry is not None and len(retry.errors) < len(game.errors):
            return retry, "chess960"
    return game, None


def load_openings() -> dict[str, tuple[str, str]]:
    """Position (EPD) -> (ECO code, opening name), from the CTML project's table."""
    if "openings" not in _worker_state:
        table: dict[str, tuple[str, str]] = {}
        if OPENINGS_TSV.is_file():
            with OPENINGS_TSV.open(encoding="utf-8") as fh:
                header = fh.readline().rstrip("\n").split("\t")
                if header[:2] == ["eco", "name"] and "epd" in header:
                    at = header.index("epd")
                    for line in fh:
                        cells = line.rstrip("\n").split("\t")
                        if len(cells) > at and re.fullmatch(r"[A-E]\d\d", cells[0]):
                            table[cells[at]] = (cells[0], cells[1])
        _worker_state["openings"] = table
    return _worker_state["openings"]


def build_ctml(ev: dict, players: list[dict], source: Path, fp_scheme, fp_cls) -> tuple[str, int, int]:
    import chess
    import chess.pgn

    table_ok = bool(ev["xt_id"] and ev["xt_name_ok"])
    table_label = f"{ev['xt_source']} crosstable {ev['xt_ref']}" if ev["xt_id"] else None

    def id_known(p) -> bool:
        """Is Chess.com's FIDE id for this player believed?"""
        return bool(p["fide_id"]) and not p["doubt"] and p["xt_id_ok"] != 0

    pid = {}
    for p in players:
        pid[p["key"]] = f"p-fide-{p['fide_id']}" if id_known(p) else f"p-n-{sha16(p['key'])}"
    by_fide = {p["fide_id"]: p["key"] for p in players if p["fide_id"]}
    by_name: dict[str, str] = {}
    for p in players:
        by_name.setdefault(norm_name(p["name"]), p["key"])

    historical = ev["origin"] == "historical"
    site_slug = ev["site_slug"] or ev["slug"]
    event_uri = None if historical else f"https://www.chess.com/events/{site_slug}"
    pgn_label = "historical tournament PGN" if historical else "Chess.com event PGN"
    start, end = ev["start"], ev["end"]
    date_raw = None
    if ev["date_basis"] == "rating-list-month":
        date_raw = "estimated from the rating lists the players' Elo tags match; the PGN's own dates are import dates"
    elif ev["date_basis"] == "name-year":
        date_raw = "year taken from the event's name; the PGN's own dates are import dates"
    elif ev["date_basis"] and ev["date_basis"].startswith("elysium:"):
        date_raw = f"from the matched {ev['ely_source']} record; the PGN's own dates are import dates"
    elif ev["date_basis"] and ev["date_basis"].startswith("table:"):
        date_raw = f"as stated by the {table_label}"

    # What the matched crosstable states is used only when the match is firm.
    place = (ev["xt_place"] if table_ok else None) or ev["ely_place"] or ev["site_tag"]
    country = ev["xt_country"] if table_ok and re.fullmatch(r"[A-Z]{3}", ev["xt_country"] or "") else None
    if country == "INT":  # TWIC's code for an online event, not a federation
        country = None

    L = ['<?xml version="1.0" encoding="UTF-8"?>',
         f'<ctml:tournament xmlns:ctml="{CTML_NS}" ctmlVersion="2.1" id="t-{esc(re.sub(r"[^A-Za-z0-9._-]", "-", ev["slug"]))}">',
         "  <ctml:header>",
         f"    <ctml:name>{esc(ev['display_name'] or ev['name'])}</ctml:name>",
         f'    <ctml:eventRef ref="event:{compact(start)}-{compact(end)}-{esc(ev["slug"])}"'
         + (f' source="{esc(event_uri)}"' if event_uri else "") + ">"
         f"<ctml:name>{esc(ev['name'])}</ctml:name></ctml:eventRef>"]
    if ev["format"]:
        L.append(f"    <ctml:eventType>{ev['format']}</ctml:eventType>")
    if country:
        L.append(f"    <ctml:federation>{country}</ctml:federation>")
    L += ["    <ctml:dates>",
          f"      <ctml:start>{date_element(start, date_raw)}</ctml:start>",
          f"      <ctml:end>{date_element(end, date_raw)}</ctml:end>",
          "    </ctml:dates>"]
    if place:
        L.append(f'    <ctml:placeRef ref="place:raw:{sha16(place.strip().lower())}">'
                 f"<ctml:name>{esc(place)}</ctml:name></ctml:placeRef>")
    if table_ok and ev["xt_rounds"]:
        L.append(f"    <ctml:rounds>{ev['xt_rounds']}</ctml:rounds>")
    if ev["avg_rating"]:
        basis = ev["avg_basis"] or ""
        system = "combined" if historical or ("elysium" in basis and not basis.startswith("elysium:")) else "fide"
        cat = f' category="{ev["category"]}"' if ev["category"] else ""
        L.append(f'    <ctml:averageRating system="{system}" scope="standard"{cat}>{ev["avg_rating"]}</ctml:averageRating>')
    L += ["  </ctml:header>", "  <ctml:participants>"]

    asof_month = start[:7] if len(start) >= 7 else None
    for p in players:
        known = id_known(p)
        # The crosstable's spelling replaces Chess.com's where Chess.com has
        # only a surname, or has named a different person altogether.
        name = re.sub(r"\s*,\s*", ", ", p["name"])
        given = norm_name(name.split(",", 1)[1]) if "," in name else ""
        if p["xt_name"] and (p["xt_id_ok"] == 0 or len(given) <= 1):  # 'Carlsen', 'Kramnik,W'
            name = re.sub(r"\s*,\s*", ", ", p["xt_name"])
        ref = f"player:fide:{p['fide_id']}" if known else f"player:syn:{sha16(norm_name(name))}"
        L.append(f'    <ctml:participant id="{pid[p["key"]]}">')
        L.append(f'      <ctml:playerRef ref="{ref}">')
        L.append("        " + person_name_xml(name))
        # Federation and title as the event's own crosstable printed them.
        # Elysium's federation is the player's current one (Aronian would be
        # USA in 2010), so it is never used here.
        if p["xt_fed"]:
            L.append(f"        <ctml:federation>{p['xt_fed']}</ctml:federation>")
        title = p["xt_title"] if p["xt_name"] else (
            p["title"] or next((t for t in (p["ely_title"] or "").split(",") if t in TITLES), None))
        if title and not p["doubt"]:
            L.append(f"        <ctml:title>{title}</ctml:title>")
        if known and re.fullmatch(r"[0-9]{4,12}", p["fide_id"]):
            L.append(f"        <ctml:ids><ctml:fideId>{p['fide_id']}</ctml:fideId></ctml:ids>")
        if p["doubt"]:
            L.append(f'        <ctml:resolution method="unresolved" resolver="{VERSION}" note="'
                     + esc(f"Chess.com gave FIDE id {p['fide_id']}, discarded as the wrong person: {p['doubt']}") + '"/>')
        elif p["xt_name"] and p["xt_id_ok"] == 0:
            L.append(f'        <ctml:resolution method="unresolved" resolver="{VERSION}" note="'
                     + esc(f"Chess.com has '{p['name']}' with FIDE id {p['fide_id']}; the {table_label} "
                           f"shows this player to be {p['xt_name']}, so that id is discarded") + '"/>')
        elif p["xt_name"] and norm_name(name) != norm_name(p["name"]):
            L.append(f'        <ctml:resolution method="{"fide-id" if known else "unresolved"}" resolver="{VERSION}" '
                     f'note="' + esc(f"Chess.com has '{p['name']}'; full name from the {table_label}") + '"/>')
        else:
            L.append(f'        <ctml:resolution method="{"fide-id" if known else "unresolved"}" resolver="{VERSION}"/>')
        L.append("      </ctml:playerRef>")
        rating, rating_source = rating_for(ev, p)
        if rating_source == "table":
            asof = (f"<ctml:asOf>{date_element(asof_month, f'rating printed in the {table_label}')}</ctml:asOf>"
                    if asof_month else "")
            L.append(f'      <ctml:ratingSnapshot system="fide" scope="standard"><ctml:value>{rating}</ctml:value>'
                     f"{asof}<ctml:publishedForEvent>true</ctml:publishedForEvent></ctml:ratingSnapshot>")
        elif rating_source == "pgn":
            if historical:
                # An Edo or Chessmetrics rating (the file does not say which), not one published for the event.
                L.append(f'      <ctml:ratingSnapshot system="combined" scope="standard"><ctml:value>{rating}</ctml:value>'
                         f"</ctml:ratingSnapshot>")
            else:
                asof = (f"<ctml:asOf>{date_element(asof_month, 'Elo tag in the Chess.com PGN')}</ctml:asOf>"
                        if asof_month else "")
                L.append(f'      <ctml:ratingSnapshot system="fide" scope="standard"><ctml:value>{rating}</ctml:value>'
                         f"{asof}<ctml:publishedForEvent>true</ctml:publishedForEvent></ctml:ratingSnapshot>")
        elif rating_source == "elysium":
            L.append(f'      <ctml:ratingSnapshot system="combined" scope="standard"><ctml:value>{rating}</ctml:value>'
                     f"<ctml:asOf>{date_element(p['elo_ely_period'], 'Elysium monthly rating for the event month')}</ctml:asOf>"
                     f"</ctml:ratingSnapshot>")
        if p["xt_rating"] and ev["xt_pool"] == "other":
            # The table printed a rating, but not from the standard list.
            asof = (f"<ctml:asOf>{date_element(asof_month, f'rating printed in the {table_label}; not the standard list')}</ctml:asOf>"
                    if asof_month else "")
            L.append(f'      <ctml:ratingSnapshot system="fide" scope="unknown"><ctml:value>{p["xt_rating"]}</ctml:value>'
                     f"{asof}<ctml:publishedForEvent>true</ctml:publishedForEvent></ctml:ratingSnapshot>")
        # Final score and place, only from a finished table that is firmly this event.
        if table_ok and ev["xt_final"] and (ev["xt_fit"] or 0) >= 0.8 and p["xt_score"] is not None:
            L.append(f"      <ctml:score>{p['xt_score']:g}</ctml:score>")
            if p["xt_rank"]:
                L.append(f"      <ctml:placement>{p['xt_rank']}</ctml:placement>")
        L.append("    </ctml:participant>")
    L.append("  </ctml:participants>")

    # Replay every game: UCI moves, clocks, fingerprints.
    raw = source.read_bytes().replace(b"\r\n", b"\n")
    openings = load_openings()
    games_xml, non_games, team_ids, team_members = [], [], {}, defaultdict(set)
    errors = written = 0
    for number, chunk in enumerate(split_games(raw), 1):
        game, variant = read_source_game(decode(chunk))
        if game is None:
            errors += 1
            continue
        h = game.headers
        sides = []
        for side in ("White", "Black"):
            fid = h.get(f"{side}FideId", "").strip()
            key = by_fide.get(fid) or by_name.get(norm_name(h.get(side, "")))
            sides.append(key)
        if is_bye(h.get("White", "")) or is_bye(h.get("Black", "")):
            # A bye is a fact about one participant in one round, not a game.
            real = 1 if is_bye(h.get("White", "")) else 0
            points = side_points(h.get("Result", "*"))[real]
            if sides[real] and points is not None:
                kind = {1.0: "bye-full", 0.5: "bye-half", 0.0: "bye-zero"}[points]
                rnd = " ".join(h.get("Round", "").split())
                non_games.append(f'      <ctml:nonGame participant="{pid[sides[real]]}"'
                                 + (f' round="{esc(rnd)}"' if rnd and rnd != "?" else "") + f' kind="{kind}"/>')
            continue
        if game.errors:
            errors += 1
        try:
            board = game.board()
        except ValueError:  # a set-up position too broken to stand on
            errors += 1
            continue
        if board.chess960:
            variant = "chess960"  # python-chess saw Chess960 castling rights in the set-up position
        if not sides[0] or not sides[1] or sides[0] == sides[1]:
            errors += 1
            continue
        acc = fp_cls(board) if fp_cls else None
        moves, clocks = [], []
        opening = None
        classify = bool(openings) and "FEN" not in h  # the table starts from the normal position
        for node in game.mainline():
            moves.append(node.move.uci())
            clocks.append(node.clock())
            board.push(node.move)
            if acc:
                acc.update(board)
            if classify and len(moves) <= 40:
                # The deepest named position the game passes through; reached
                # by transposition counts.
                opening = openings.get(board.epd()) or opening
        real_clock = len({c for c in clocks if c is not None}) > 4

        attrs = [f'id="g-{number:05d}"']
        rnd = " ".join(h.get("Round", "").split())
        if rnd and rnd != "?":
            attrs.append(f'round="{esc(rnd)}"')
        if h.get("Board", "").isdigit() and int(h["Board"]) > 0:
            attrs.append(f'board="{int(h["Board"])}"')
        attrs += [f'white="{pid[sides[0]]}"', f'black="{pid[sides[1]]}"']
        for side, key in (("White", sides[0]), ("Black", sides[1])):
            team = h.get(f"{side}Team", "").strip()
            if team:
                tid = team_ids.setdefault(team, f"team-{len(team_ids) + 1:03d}")
                team_members[tid].add(pid[key])
                attrs.append(f'{side.lower()}Team="{tid}"')
        result = h.get("Result", "*")
        attrs.append(f'result="{result if result in RESULTS else "*"}"')
        G = [f"    <ctml:game {' '.join(attrs)}>"]
        if opening:
            G.append(f"      <ctml:eco>{opening[0]}</ctml:eco>")
            G.append(f"      <ctml:opening>{esc(opening[1])}</ctml:opening>")
        if "FEN" in h:
            G.append(f'      <ctml:start standard="false"><ctml:fen>{esc(h["FEN"])}</ctml:fen></ctml:start>')
        if TIME_CONTROL_RE.fullmatch(h.get("TimeControl", "")):
            G.append(f"      <ctml:timeControl><ctml:raw>{esc(h['TimeControl'])}</ctml:raw></ctml:timeControl>")
        if moves:
            G.append(f'      <ctml:moves notation="uci" plyCount="{len(moves)}"'
                     + (' clockInfo="true"' if real_clock else "") + ">")
            row = []
            for ply, (mv, clk) in enumerate(zip(moves, clocks), 1):
                clock = f' clockSeconds="{int(clk)}"' if real_clock and clk is not None else ""
                row.append(f'<ctml:move ply="{ply}" value="{mv}"{clock}/>')
                if len(row) == 4:
                    G.append("".join(row))
                    row = []
            if row:
                G.append("".join(row))
            G.append("      </ctml:moves>")
        tags = []
        game_date = valid_date(h.get("Date", ""))
        if game_date and not ev["date_suspect"]:
            tags.append(f"date={game_date}")
        if variant:
            tags.append(f"variant={variant}")
        if result not in RESULTS:
            tags.append("sourceResult=" + re.sub(r"\s+", "", result))  # e.g. 1/2-0: no CTML or PGN value for it
        if tags:
            G.append("      <ctml:tags>" + "".join(f"<ctml:tag>{t}</ctml:tag>" for t in tags) + "</ctml:tags>")
        if acc and moves:
            traj, final = acc.result()
            G.append(f'      <ctml:fingerprints><ctml:fingerprint scheme="{fp_scheme}" scope="trajectory" value="{traj}"/>'
                     f'<ctml:fingerprint scheme="{fp_scheme}" scope="finalPosition" value="{final}"/></ctml:fingerprints>')
        site = h.get("Site", "")
        if site.startswith("http"):
            G.append(f'      <ctml:source kind="chesscom-events"><ctml:uri>{esc(site.replace(" ", "%20"))}</ctml:uri></ctml:source>')
        G.append("    </ctml:game>")
        games_xml.append("\n".join(G))
        written += 1

    if team_ids:
        L.append("  <ctml:teams>")
        for team, tid in team_ids.items():
            members = "".join(f'<ctml:member participant="{m}"/>' for m in sorted(team_members[tid]))
            L.append(f'    <ctml:team id="{tid}"><ctml:name>{esc(team)}</ctml:name>'
                     f"<ctml:roster>{members}</ctml:roster></ctml:team>")
        L.append("  </ctml:teams>")
    L.append("  <ctml:games>")
    L += games_xml
    if non_games:
        L += ["    <ctml:nonGames>", *non_games, "    </ctml:nonGames>"]
    L.append("  </ctml:games>")

    notes = [f"Partial record generated by {VERSION} from the {pgn_label}; it states only what that PGN, "
             "the published crosstables and the Elysium workbench supply. Rounds, cadence, organizers, "
             "standings and place are absent unless given above."]
    if historical:
        notes.append("Ratings are the Elo tags of the source PGN: Edo or Chessmetrics historical ratings, not "
                     "FIDE ratings. Annotations and variations in the source are not carried over.")
    if ev["xt_id"]:
        relation = {"same": "the same field", "whole": "a larger field containing this one",
                    "part": "part of this field"}[ev["xt_kind"]]
        notes.append(f"Matched {table_label} \"{ev['xt_title']}\" ({ev['xt_start']} to {ev['xt_end']}; {relation}, "
                     f"roster score {ev['xt_score']}; {ev['xt_obs']}). "
                     + ("Its name, dates, place and standings are used." if table_ok else
                        "A sibling event shares this roster, so only its player data is used."))
    notes.append("ECO codes and opening names are assigned here from the moves, by position, using the CTML "
                 "project's opening table; they are not from the source.")
    if ev["xt_pool"] == "other":
        notes.append("That table prints ratings from another pool (rapid or blitz); they are recorded with "
                     "scope unknown and the average is taken from standard ratings.")
    if ev["avg_rating"] and (ev["avg_basis"] or "").startswith("stated:"):
        notes.append(f"averageRating and category are as that table states them; the mean of the participants' "
                     f"ratings recorded here is {ev['avg_computed']}.")
    elif ev["avg_rating"]:
        notes.append(f"averageRating is computed, not published: {ev['reason']}.")
    if ev["date_suspect"]:
        notes.append(f"Dates: {ev['date_reason']}; basis used: {ev['date_basis']}.")
    if ev["elo_suspect"]:
        notes.append(f"Ratings: {ev['elo_reason']}. Those tags were discarded; a ratingSnapshot here is the "
                     f"rating the matched crosstable prints for the player, or failing that Elysium's rating for "
                     f"{ev['rating_month']}. A title not taken from a crosstable is the one Chess.com gave at import.")
    if ev["identity_doubt"]:
        notes.append(f"Identity not trusted, so no id, rating or title is recorded for: {ev['identity_doubt']}.")
    if ev["ely_name"]:
        notes.append(f"Matched Elysium event {ev['ely_event_id']} ({ev['ely_source']}): \"{ev['ely_name']}\", "
                     f"{ev['ely_start']} to {ev['ely_end']}, roster score {ev['ely_score']}.")
    if ev["format_note"] and not ev["format"]:
        notes.append(f"Format not established: {ev['format_note']}.")
    if errors:
        notes.append(f"{errors} game(s) in the source had parse problems; moves are kept up to the first illegal move.")
    L.append(f"  <ctml:notes>{esc(' '.join(notes))}</ctml:notes>")
    if ev["xt_id"]:
        L.append(f'  <ctml:source kind="{esc(ev["xt_source"])}"><ctml:note>'
                 + esc(f"{table_label}: {ev['xt_title']}; {ev['xt_obs']}") + "</ctml:note></ctml:source>")
    if historical:
        L.append('  <ctml:source kind="historical-collection"><ctml:note>'
                 + esc(f"{source.name}, one event cut from a historical game collection") + "</ctml:note></ctml:source>")
    else:
        L.append(f'  <ctml:source kind="chesscom-events"><ctml:uri>{esc(event_uri)}</ctml:uri>'
                 f"<ctml:retrieved>{dt.date.fromtimestamp(source.stat().st_mtime).isoformat()}</ctml:retrieved></ctml:source>")
    L.append("</ctml:tournament>")
    return "\n".join(L) + "\n", written, errors


WORKERS = max(1, min(12, (__import__("os").cpu_count() or 2) - 2))
_worker_state: dict = {}


def run_jobs(fn, jobs: list, workers: int):
    """Yield fn(job) for every job, in order, across worker processes."""
    if workers <= 1 or len(jobs) < 4:
        yield from map(fn, jobs)
        return
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(max_workers=workers) as pool:
        yield from pool.map(fn, jobs, chunksize=2)


def _quiet_chess() -> None:
    """python-chess logs every illegal move in a source PGN; the count is kept instead."""
    import logging

    logging.getLogger("chess.pgn").setLevel(logging.CRITICAL)


def _ctml_job(job: dict) -> dict:
    """Worker: build one CTML document, write it and validate it."""
    from lxml import etree

    if "schema" not in _worker_state:
        _quiet_chess()
        _worker_state["schema"] = etree.XMLSchema(etree.parse(job["xsd"]))
        _worker_state["fp"] = load_fingerprinter()
    out = Path(job["out"])
    try:
        xml, games, errors = build_ctml(job["ev"], job["players"], Path(job["source"]), *_worker_state["fp"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(xml, encoding="utf-8", newline="\n")
    except Exception as exc:  # one bad event must not stop the batch
        return {"slug": job["ev"]["slug"], "failed": f"{type(exc).__name__}: {exc}"}
    error = None
    try:
        _worker_state["schema"].assertValid(etree.parse(str(out)))
    except (etree.DocumentInvalid, etree.XMLSyntaxError) as exc:
        error = str(exc)[:300]
    return {"slug": job["ev"]["slug"], "games": games, "errors": errors, "bytes": out.stat().st_size,
            "invalid": error}


def stage_ctml(con: sqlite3.Connection, limit: int | None = None, only: str | None = None,
               workers: int = WORKERS) -> None:
    events = con.execute(
        "select e.*, f.size as f_size, f.mtime as f_mtime, f.dir as f_dir from event e "
        "join file f on f.name = e.file "
        "where e.decision = 'keep' and e.start is not null order by e.start, e.slug").fetchall()
    if only:
        events = [e for e in events if e["slug"] == only]
    todo = [e for e in events if e["ctml_sig"] != event_signature(e, (e["f_size"], e["f_mtime"]))]
    if limit:
        todo = todo[:limit]
    started = time.time()
    jobs, by_slug = [], {}
    for ev in todo:
        path = source_path(ev["f_dir"], ev["file"])
        st = path.stat() if path else None
        if st is None or (st.st_size, int(st.st_mtime)) != (ev["f_size"], ev["f_mtime"]):
            continue  # changed since the scan; the next scan picks it up
        out = CTML_OUT / ev["start"][:4] / f"{compact(ev['start'])}-{compact(ev['end'])}_{ev['slug']}.ctml"
        players = [dict(p) for p in con.execute("select * from player where slug = ? order by key", (ev["slug"],))]
        jobs.append({"ev": dict(ev), "players": players, "source": str(path), "out": str(out),
                     "xsd": str(CTML_ROOT / "xsd" / "ctml.xsd")})
        by_slug[ev["slug"]] = (ev, out)
    done = invalid = failed = 0
    for result in run_jobs(_ctml_job, jobs, workers):
        ev, out = by_slug[result["slug"]]
        if "failed" in result:
            failed += 1
            log(f"  FAILED {ev['slug']}: {result['failed']}")
            continue
        if result["invalid"]:
            invalid += 1
            log(f"  INVALID {out.name}: {result['invalid']}")
        if ev["ctml_path"] and ev["ctml_path"] != str(out.relative_to(HOME)):
            (HOME / ev["ctml_path"]).unlink(missing_ok=True)  # the name changed with the dates
        con.execute(
            "update event set ctml_path = ?, ctml_sig = ?, ctml_valid = ?, ctml_games = ?, ctml_bytes = ?, "
            "parse_errors = ? where slug = ?",
            (str(out.relative_to(HOME)), event_signature(ev, (ev["f_size"], ev["f_mtime"])),
             0 if result["invalid"] else 1, result["games"], result["bytes"], result["errors"], ev["slug"]))
        done += 1
        if done % 100 == 0:
            con.commit()
            log(f"  ctml {done}/{len(jobs)}  ({time.time() - started:.0f}s)")
    con.commit()
    log(f"ctml: wrote {done} documents ({invalid} failed validation, {failed} could not be built), "
        f"{len(events) - len(todo)} already current ({time.time() - started:.0f}s)")


# --------------------------------------------------------------------------
# pgn (out of CTML)
# --------------------------------------------------------------------------

def q(tag: str) -> str:
    return f"{{{CTML_NS}}}{tag}"


def file_stamp(path: Path) -> str | None:
    """Size and modification time: the PGN is stale whenever its CTML was rewritten."""
    try:
        st = path.stat()
    except OSError:
        return None
    return f"{st.st_size}:{st.st_mtime_ns}"


def partial_date(el) -> str:
    """A CTML start/end element -> PGN date with ?? for what is not known."""
    if el is None or len(el) == 0:
        return "????.??.??"
    d = el[0]
    return f"{int(d.get('y')):04d}.{int(d.get('m')):02d}.{int(d.get('d')):02d}" if d.get("d") else (
        f"{int(d.get('y')):04d}.{int(d.get('m')):02d}.??" if d.get("m") else f"{int(d.get('y')):04d}.??.??")


def ctml_to_pgn(ctml_path: Path) -> tuple[str, int]:
    import xml.etree.ElementTree as ET

    import chess
    import chess.pgn

    root = ET.parse(ctml_path).getroot()
    header = root.find(q("header"))
    event_name = header.findtext(q("name"), "?")
    dates = header.find(q("dates"))
    event_date = partial_date(dates.find(q("start")))
    place = header.find(q("placeRef"))
    country = header.findtext(q("federation"))
    rounds = header.findtext(q("rounds"))
    # The real place wherever it is known. Otherwise the event's only known
    # home is the site that hosted its broadcast.
    site = place.findtext(q("name")) if place is not None else None
    from_chesscom = any(src.get("kind") == "chesscom-events" for src in root.findall(q("source")))
    site = (f"{site} {country}" if country else site) if site else ("Chess.com" if from_chesscom else "?")
    event_type = header.findtext(q("eventType"))
    avg = header.find(q("averageRating"))
    category = avg.get("category") if avg is not None else None

    people = {}
    for part in root.find(q("participants")).findall(q("participant")):
        ref = part.find(q("playerRef"))
        snap = next((s for s in part.findall(q("ratingSnapshot")) if s.get("scope") == "standard"), None)
        people[part.get("id")] = {
            "name": ref.find(q("name")).get("display"),
            "title": ref.findtext(q("title")),
            "fide": ref.findtext(f"{q('ids')}/{q('fideId')}"),
            "elo": snap.findtext(q("value")) if snap is not None else None,
        }
    teams = {t.get("id"): t.findtext(q("name")) for t in root.iter(q("team"))}

    out, count = [], 0
    for g in root.find(q("games")).findall(q("game")):
        tags = {}
        tag_set = g.find(q("tags"))
        for t in ([] if tag_set is None else tag_set.findall(q("tag"))):
            if "=" in (t.text or ""):
                k, v = t.text.split("=", 1)
                tags[k] = v
        fen = g.findtext(f"{q('start')}/{q('fen')}")
        chess960 = tags.get("variant") == "chess960"
        board = chess.Board(fen, chess960=chess960) if fen else chess.Board()
        game = chess.pgn.Game()
        if fen:
            game.setup(board)
        w, b = people[g.get("white")], people[g.get("black")]
        hd = game.headers
        hd["Event"] = event_name
        hd["Site"] = site
        hd["Date"] = tags["date"].replace("-", ".") if "date" in tags else event_date
        hd["Round"] = g.get("round") or "?"
        hd["White"], hd["Black"] = w["name"], b["name"]
        hd["Result"] = g.get("result")
        for tag, key in (("Elo", "elo"), ("Title", "title"), ("FideId", "fide")):
            for side, person in (("White", w), ("Black", b)):
                if person[key]:
                    hd[f"{side}{tag}"] = person[key]
        for side in ("white", "black"):
            if g.get(f"{side}Team"):
                hd[f"{side.capitalize()}Team"] = teams.get(g.get(f"{side}Team"), "")
        if g.get("board"):
            hd["Board"] = g.get("board")
        if g.findtext(q("eco")):
            hd["ECO"] = g.findtext(q("eco"))
        if g.findtext(q("opening")):
            hd["Opening"] = g.findtext(q("opening"))
        hd["EventDate"] = event_date
        if event_type:
            hd["EventType"] = {"round-robin": "tourn", "match": "match", "team": "team", "swiss": "swiss",
                               "knockout": "k.o."}.get(event_type, event_type)
        if rounds:
            hd["EventRounds"] = rounds
        if country:
            hd["EventCountry"] = country
        if category:
            hd["EventCategory"] = category
        tc = g.findtext(f"{q('timeControl')}/{q('raw')}")
        if tc:
            hd["TimeControl"] = tc
        if chess960:
            hd["Variant"] = "Chess960"
        moves_el = g.find(q("moves"))
        node = game
        if moves_el is not None:
            hd["PlyCount"] = moves_el.get("plyCount") or str(len(moves_el))
            for mv in moves_el.findall(q("move")):
                node = node.add_variation(chess.Move.from_uci(mv.get("value")))
                if mv.get("clockSeconds") is not None:
                    node.set_clock(int(mv.get("clockSeconds")))
        uri = g.findtext(f"{q('source')}/{q('uri')}")
        if uri:
            hd["Link"] = uri
        out.append(game.accept(chess.pgn.StringExporter(columns=80, headers=True, variations=False, comments=True)))
        count += 1
    return "\n\n".join(out) + "\n", count


def prune_outputs(con: sqlite3.Connection) -> None:
    """Remove generated CTML and PGN that no kept event owns any more: an event
    reclassified out of 'keep', or a file left behind when dates renamed it."""
    con.execute("update event set ctml_path = null, ctml_sig = null, ctml_valid = null, pgn_path = null, "
                "pgn_sig = null where decision != 'keep' and (ctml_path is not null or pgn_path is not null)")
    con.commit()
    owned = set()
    for row in con.execute("select ctml_path, pgn_path from event where decision = 'keep'"):
        owned.update(str((HOME / p).resolve()) for p in row if p)
    removed = 0
    for root, pattern in ((CTML_OUT, "*.ctml"), (PGN_OUT, "*.pgn")):
        for path in root.glob(f"*/{pattern}"):
            if str(path.resolve()) not in owned:
                path.unlink()
                removed += 1
    if removed:
        log(f"pruned {removed} generated files no kept event owns")


def _pgn_job(job: tuple[str, str]) -> tuple[str, str | None]:
    """Worker: one CTML document out to PGN. Returns (ctml path, error or None)."""
    ctml_path, out_path = job
    try:
        text, _ = ctml_to_pgn(Path(ctml_path))
        out = Path(out_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8", newline="\n")
        return ctml_path, None
    except Exception as exc:
        return ctml_path, f"{type(exc).__name__}: {exc}"


def stage_pgn(con: sqlite3.Connection, limit: int | None = None, only: str | None = None,
              workers: int = WORKERS) -> None:
    events = con.execute(
        "select slug, ctml_path, ctml_sig, pgn_sig, pgn_path from event "
        "where ctml_valid = 1 and ctml_path is not null order by start, slug").fetchall()
    if only:
        events = [e for e in events if e["slug"] == only]
    todo = [e for e in events if e["pgn_sig"] != file_stamp(HOME / e["ctml_path"])]
    if limit:
        todo = todo[:limit]
    started = time.time()
    jobs, by_path = [], {}
    for ev in todo:
        ctml_path = HOME / ev["ctml_path"]
        out = PGN_OUT / ctml_path.parent.name / (ctml_path.stem + ".pgn")
        jobs.append((str(ctml_path), str(out)))
        by_path[str(ctml_path)] = (ev, ctml_path, out)
    done = failed = 0
    for path, error in run_jobs(_pgn_job, jobs, workers):
        ev, ctml_path, out = by_path[path]
        if error:
            failed += 1
            log(f"  FAILED {ev['slug']}: {error}")
            continue
        if ev["pgn_path"] and ev["pgn_path"] != str(out.relative_to(HOME)):
            (HOME / ev["pgn_path"]).unlink(missing_ok=True)
        con.execute("update event set pgn_path = ?, pgn_sig = ? where slug = ?",
                    (str(out.relative_to(HOME)), file_stamp(ctml_path), ev["slug"]))
        done += 1
        if done % 200 == 0:
            con.commit()
            log(f"  pgn {done}/{len(jobs)}  ({time.time() - started:.0f}s)")
    con.commit()
    log(f"pgn: wrote {done} files ({failed} failed), {len(events) - len(todo)} already current "
        f"({time.time() - started:.0f}s)")
    if not limit and not only:
        prune_outputs(con)


def _verify_job(job: tuple[str, str, str]) -> tuple[str, int, int, list[str]]:
    """Worker: compare one source PGN with its regenerated PGN.
    Returns (slug, games compared, name tags respelled, problems)."""
    import chess.pgn

    slug, source_path, pgn_path = job
    _quiet_chess()
    source = []
    for chunk in split_games(Path(source_path).read_bytes().replace(b"\r\n", b"\n")):
        game, _ = read_source_game(decode(chunk))
        # A stub with no players ('?' against '?') is not carried into the CTML,
        # and a bye is carried as a non-game, so neither is in the PGN.
        if game is None or is_bye(game.headers.get("White", "")) or is_bye(game.headers.get("Black", "")):
            continue
        try:
            game.board()
        except ValueError:
            continue  # a set-up position broken in the source; the CTML leaves the game out and counts it
        if norm_name(game.headers.get("White", "")) != norm_name(game.headers.get("Black", "")):
            source.append(game)
    rebuilt = []
    with open(pgn_path, encoding="utf-8") as fh:
        while (game := chess.pgn.read_game(fh)) is not None:
            rebuilt.append(game)
    problems, respelled = [], 0
    if len(source) != len(rebuilt):
        problems.append(f"{len(source)} games in the source, {len(rebuilt)} regenerated")
    for n, (a, b) in enumerate(zip(source, rebuilt), 1):
        for tag in ("White", "Black"):
            was, now = a.headers.get(tag, ""), b.headers.get(tag, "")
            if was != now:
                # A player spelled two ways in the source gets one spelling,
                # and a crosstable may supply the full or the right name; both
                # are intended. A different FIDE id is not.
                respelled += 1
                id_was, id_now = a.headers.get(f"{tag}FideId"), b.headers.get(f"{tag}FideId")
                if id_was and id_now and id_was != id_now:
                    problems.append(f"game {n}: {tag} {was!r} became {now!r} with another FIDE id")
        expected = a.headers.get("Result") if a.headers.get("Result") in RESULTS else "*"
        if expected != b.headers.get("Result"):
            problems.append(f"game {n}: result {a.headers.get('Result')} became {b.headers.get('Result')}")
        if [m.uci() for m in a.mainline_moves()] != [m.uci() for m in b.mainline_moves()]:
            problems.append(f"game {n}: moves differ")
    return slug, min(len(source), len(rebuilt)), respelled, problems


def stage_verify(con: sqlite3.Connection, limit: int | None = None, only: str | None = None,
                 workers: int = WORKERS) -> None:
    """Replay the source PGN and the regenerated PGN side by side.

    Every game must come back with the same result and moves. Games the source
    itself breaks off at an illegal move are compared up to there.
    """
    events = con.execute(
        "select e.slug, e.file, e.pgn_path, f.dir as f_dir from event e join file f on f.name = e.file "
        "where e.pgn_path is not null order by e.start, e.slug").fetchall()
    if only:
        events = [e for e in events if e["slug"] == only]
    if limit:
        events = events[:limit]
    started = time.time()
    jobs = [(e["slug"], str(source_path(e["f_dir"], e["file"])), str(HOME / e["pgn_path"])) for e in events]
    games = bad_events = respelled = 0
    for i, (slug, compared, renamed, problems) in enumerate(run_jobs(_verify_job, jobs, workers), 1):
        games += compared
        respelled += renamed
        if problems:
            bad_events += 1
            log(f"  MISMATCH {slug}: " + "; ".join(problems[:4]) + (" ..." if len(problems) > 4 else ""))
        if i % 500 == 0:
            log(f"  verify {i}/{len(events)}  ({time.time() - started:.0f}s)")
    log(f"verify: {len(events)} events, {games} games compared, {bad_events} events with differences, "
        f"{respelled} name tags respelled ({time.time() - started:.0f}s)")


# --------------------------------------------------------------------------
# publish (ZIPs and manifest in the repository's working tree)
# --------------------------------------------------------------------------

GITHUB_REPO = "ianrastall/cc-events-archive"
MANIFEST = REPO / "cc_events_manifest.json"
ZIP_TIME = (1980, 1, 1, 0, 0, 0)  # fixed, so an unchanged event zips to the same bytes


def _zip_job(job: tuple[str, str, str, str]) -> tuple[str, int, str, bool, int, int]:
    """Worker: one event's PGN and CTML into <year>/<slug>.zip.
    Returns (slug, zip bytes, sha256, rewritten, packed PGN bytes, packed CTML bytes)."""
    import zipfile

    slug, pgn_path, ctml_path, out_path = job
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, source in ((f"{slug}.pgn", pgn_path), (f"{slug}.ctml", ctml_path)):
            info = zipfile.ZipInfo(name, ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, Path(source).read_bytes(), compresslevel=9)
        packed = {i.filename.rsplit(".", 1)[1]: i.compress_size for i in archive.infolist()}
    data = buffer.getvalue()
    out = Path(out_path)
    rewritten = not out.exists() or out.read_bytes() != data
    if rewritten:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
    return slug, len(data), hashlib.sha256(data).hexdigest(), rewritten, packed["pgn"], packed["ctml"]


def iso_bounds(start: str, end: str) -> tuple[str, str, str]:
    """Catalog dates may be a year or a month; the manifest needs whole days."""
    import calendar

    precision = {4: "year", 7: "month", 10: "day"}[len(start)]
    first = start if len(start) == 10 else (start + "-01" if len(start) == 7 else start + "-01-01")
    if len(end) == 10:
        last = end
    elif len(end) == 7:
        last = f"{end}-{calendar.monthrange(int(end[:4]), int(end[5:7]))[1]:02d}"
    else:
        last = end + "-12-31"
    return first, max(first, last), precision


def stage_publish(con: sqlite3.Connection, workers: int = WORKERS) -> None:
    """Write the publishable tree: <year>/<slug>.zip for every kept event and
    cc_events_manifest.json. Nothing is committed or pushed here."""
    started = time.time()
    events = con.execute(
        "select * from event where decision = 'keep' and ctml_valid = 1 and pgn_path is not null "
        "and pgn_sig is not null order by start, slug").fetchall()
    jobs, rows = [], {}
    for ev in events:
        first, last, precision = iso_bounds(ev["start"], ev["end"])
        year = int(first[:4])
        published = ev["pub_name"] or ev["slug"]
        jobs.append((published, str(HOME / ev["pgn_path"]), str(HOME / ev["ctml_path"]),
                     str(REPO / str(year) / f"{published}.zip")))
        rows[published] = (ev, first, last, precision, year)

    entries, rewritten, pgn_bytes, ctml_bytes = [], 0, 0, 0
    for slug, size, sha, changed, packed_pgn, packed_ctml in run_jobs(_zip_job, jobs, workers):
        ev, first, last, precision, year = rows[slug]
        rewritten += changed
        pgn_bytes += packed_pgn
        ctml_bytes += packed_ctml
        firm = bool(ev["xt_id"] and ev["xt_name_ok"])
        country = ev["xt_country"] if firm and ev["xt_country"] != "INT" else None
        if country and not re.fullmatch(r"[A-Z]{3}", country):
            country = None
        entry = {
            "slug": slug, "zip": f"{slug}.zip", "pgn": f"{slug}.pgn", "ctml": f"{slug}.ctml",
            "sourceSlug": ev["slug"],
            "historical": True if ev["origin"] == "historical" else None,
            "year": year, "start": first, "end": last,
            "name": ev["display_name"] or ev["name"], "sourceName": ev["name"],
            "place": (ev["xt_place"] if firm else None) or ev["ely_place"] or ev["site_tag"] or "",
            "country": country, "games": ev["ctml_games"], "players": ev["players"],
            "ratedPlayers": ev["rated_players"], "rounds": (ev["xt_rounds"] if firm else None) or ev["rounds"],
            "format": ev["format"], "avgRating": ev["avg_rating"], "category": ev["category"],
            "avgStated": (ev["avg_basis"] or "").startswith("stated:"),
            "url": f"https://github.com/{GITHUB_REPO}/raw/main/{year}/{slug}.zip",
            "bytes": size, "sha256": sha,
        }
        if precision != "day":
            entry["datePrecision"] = precision
        entries.append({k: v for k, v in entry.items() if v is not None})

    # Events published before this pipeline existed stay listed, unchanged,
    # until their PGN has been downloaded again and judged here.
    scanned = {r[0] for r in con.execute("select slug from event")} | set(rows)
    legacy = []
    if MANIFEST.exists():
        for old in json.loads(MANIFEST.read_text(encoding="utf-8")):
            if (old.get("legacy") or "ctml" not in old) and old.get("sourceSlug", old["slug"]) not in scanned:
                legacy.append({**old, "legacy": True})
    entries += legacy
    entries.sort(key=lambda e: (e["start"], e["end"], e["slug"]), reverse=True)
    MANIFEST.write_text(json.dumps(entries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")

    listed = {str((REPO / str(e["year"]) / e["zip"]).resolve()) for e in entries}
    removed = 0
    for folder in REPO.iterdir():
        if folder.is_dir() and re.fullmatch(r"\d{4}", folder.name):
            for path in folder.glob("*.zip"):
                if str(path.resolve()) not in listed:
                    path.unlink()
                    removed += 1
            if not any(folder.iterdir()):
                folder.rmdir()
    total = sum(e["bytes"] for e in entries)
    log(f"publish: {len(entries) - len(legacy)} events from this pipeline ({rewritten} ZIPs written), "
        f"{len(legacy)} earlier events carried over, {removed} stale ZIPs removed; "
        f"{total / 1e6:.0f} MB in all ({pgn_bytes / 1e6:.0f} MB PGN + {ctml_bytes / 1e6:.0f} MB CTML packed) "
        f"({time.time() - started:.0f}s)")
    log(f"manifest: {MANIFEST}")
    stage_bundles(con)
    log("Nothing has been committed or pushed.")


# The three prepared databases: every kept event, the events averaging 2600 or
# more, and those averaging 2700 or more. Each is one PGN, oldest event first.
BUNDLE_DIR = REPO / "bundles"
BUNDLES_MANIFEST = REPO / "cc_events_bundles.json"
BUNDLES = (("cc-events-all", 0, "Every event in the archive"),
           ("cc-events-2600", 2600, "Events with a tournament average of 2600 or more"),
           ("cc-events-2700", 2700, "Events with a tournament average of 2700 or more"))
BUNDLE_PART_BYTES = 95_000_000   # GitHub refuses files over 100 MB; a bigger database is split by year
# With a Pixeldrain API key the databases are uploaded there instead: one file
# each, nothing split, nothing committed to this repository. The key is read
# from the PIXELDRAIN_API_KEY environment variable or from work/pixeldrain.key
# (work/ is not committed). Without a key they go into bundles/ as above.
PIXELDRAIN_API = "https://pixeldrain.com/api"


def pixeldrain_key() -> str | None:
    key = os.environ.get("PIXELDRAIN_API_KEY", "").strip()
    key_file = HOME / "pixeldrain.key"
    if not key and key_file.is_file():
        key = key_file.read_text(encoding="utf-8").strip()
    return key or None


def pixeldrain_upload(key: str, filename: str, blob: bytes) -> str:
    """PUT one file to Pixeldrain; returns its id. Raises on any failure."""
    request = urllib.request.Request(
        f"{PIXELDRAIN_API}/file/{urllib.request.quote(filename)}", data=blob, method="PUT",
        headers={"Authorization": "Basic " + base64.b64encode(f":{key}".encode()).decode(),
                 "Content-Type": "application/octet-stream"})
    try:
        with urllib.request.urlopen(request, timeout=1800) as response:
            reply = json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"Pixeldrain refused {filename}: HTTP {error.code} {detail}") from None
    if not reply.get("id"):
        raise RuntimeError(f"Pixeldrain returned no id for {filename}: {reply}")
    return reply["id"]


def _bundle_zip(name: str, members: list[tuple[str, list]]) -> tuple[bytes, int]:
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member, events in members:
            info = zipfile.ZipInfo(member, ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            with archive.open(info, "w", force_zip64=True) as out:
                for ev in events:
                    text = (HOME / ev["pgn_path"]).read_bytes()
                    out.write(text if text.endswith(b"\n\n") else text.rstrip(b"\r\n") + b"\n\n")
    return buffer.getvalue(), sum(len(events) for _, events in members)


def stage_bundles(con: sqlite3.Connection) -> None:
    """Write bundles/<name>.zip for each prepared database, and
    cc_events_bundles.json describing them. A database too large for one
    GitHub file is written as consecutive parts, each a run of whole years."""
    started = time.time()
    kept = con.execute(
        "select * from event where decision = 'keep' and ctml_valid = 1 and pgn_path is not null "
        "and pgn_sig is not null order by start, slug").fetchall()
    key = pixeldrain_key()
    out_dir = HOME / "bundles" if key else BUNDLE_DIR
    out_dir.mkdir(exist_ok=True)
    described, written = [], set()
    # "updated" is the day a database last changed, not the day this ran.
    before = {}
    if BUNDLES_MANIFEST.exists():
        before = {b["id"]: b for b in json.loads(BUNDLES_MANIFEST.read_text(encoding="utf-8"))}
    for name, floor, title in BUNDLES:
        events = [ev for ev in kept if ev["avg_rating"] >= floor]
        data, _ = _bundle_zip(name, [(f"{name}.pgn", events)])
        parts = [(f"{name}.zip", data, events)]
        if len(data) > BUNDLE_PART_BYTES and not key:
            # Split at year boundaries into as few parts as fit under the limit.
            by_year: dict[str, list] = defaultdict(list)
            for ev in events:
                by_year[ev["start"][:4]].append(ev)
            years, parts, run = sorted(by_year), [], []

            def close(run_years):
                label = run_years[0] if len(run_years) == 1 else f"{run_years[0]}-{run_years[-1]}"
                run_events = [ev for y in run_years for ev in by_year[y]]
                blob, _ = _bundle_zip(name, [(f"{name}-{label}.pgn", run_events)])
                return (f"{name}-{label}.zip", blob, run_events)

            for year in years:
                if run and len(close(run + [year])[1]) > BUNDLE_PART_BYTES:
                    parts.append(close(run))
                    run = []
                run.append(year)
            parts.append(close(run))
        files = []
        old = before.get(name, {})
        for filename, blob, part_events in parts:
            target = out_dir / filename
            if not target.exists() or target.read_bytes() != blob:
                target.write_bytes(blob)
            written.add(filename)
            sha = hashlib.sha256(blob).hexdigest()
            entry = {
                "file": filename, "bytes": len(blob), "sha256": sha,
                "events": len(part_events), "games": sum(ev["ctml_games"] for ev in part_events),
                "from": part_events[0]["start"][:4], "to": part_events[-1]["start"][:4],
                "url": f"https://github.com/{GITHUB_REPO}/raw/main/bundles/{filename}",
            }
            if key:
                # Upload only what changed; an unchanged database keeps its link.
                same = next((f for f in old.get("files", []) if f.get("sha256") == sha and f.get("pixeldrain")), None)
                entry["pixeldrain"] = same["pixeldrain"] if same else pixeldrain_upload(key, filename, blob)
                entry["url"] = f"https://pixeldrain.com/u/{entry['pixeldrain']}"
                for gone in old.get("files", []):
                    if gone.get("pixeldrain") and gone["pixeldrain"] != entry["pixeldrain"]:
                        log(f"bundles: the earlier copy of {gone['file']} is still on Pixeldrain as "
                            f"{gone['pixeldrain']}; delete it there when convenient")
            files.append(entry)
        unchanged = [f["sha256"] for f in old.get("files", [])] == [f["sha256"] for f in files]
        described.append({
            "id": name, "title": title, "minAverage": floor, "events": len(events),
            "updated": old["updated"] if unchanged and old.get("updated") else dt.date.today().isoformat(),
            "games": sum(ev["ctml_games"] for ev in events), "bytes": sum(f["bytes"] for f in files),
            "newest": max(ev["start"] for ev in events), "files": files,
        })
        log(f"bundles: {name}: {len(events)} events, {described[-1]['games']} games, "
            f"{described[-1]['bytes'] / 1e6:.0f} MB in {len(files)} file{'s' if len(files) != 1 else ''}")
    for folder in {out_dir, BUNDLE_DIR}:
        if folder.is_dir():
            for stale in folder.glob("*.zip"):
                if folder != out_dir or stale.name not in written:
                    stale.unlink()   # generated files only: superseded parts, or bundles/ once Pixeldrain hosts them
            if folder != out_dir and not any(folder.iterdir()):
                folder.rmdir()
    BUNDLES_MANIFEST.write_text(json.dumps(described, ensure_ascii=False, indent=2) + "\n",
                                encoding="utf-8", newline="\n")
    log(f"bundles: {BUNDLES_MANIFEST} ({time.time() - started:.0f}s)")


# --------------------------------------------------------------------------
# export / status
# --------------------------------------------------------------------------

EXPORT_COLUMNS = (
    "slug", "pub_name", "origin", "display_name", "name", "decision", "reason", "start", "end", "date_basis",
    "avg_rating", "category", "avg_basis", "avg_computed",
    "xt_place", "site_tag", "xt_country", "xt_rounds", "xt_kind", "xt_score", "xt_name_ok", "xt_source", "xt_ref",
    "xt_title", "xt_start", "xt_end", "xt_category", "xt_avg", "xt_final", "xt_fit", "xt_rows",
    "players", "rated_players", "backfilled_players", "min_rating", "max_rating", "below_floor",
    "games", "byes", "rounds", "format", "format_note", "cadence_guess", "time_control",
    "unrated_then", "unknown_rating", "tags_standard", "identity_doubt", "ely_name", "ely_place", "ely_source", "ely_kind", "ely_start", "ely_end", "ely_score",
    "ely_avg", "ely_avg_n", "elo_suspect", "elo_reason", "rating_month",
    "date_suspect", "date_reason", "tag_start", "tag_end", "est_month_first", "est_month_last",
    "rating_agree", "rating_agree_n", "fide_id_players", "header_rated_players", "avg_game_weighted",
    "date_strays", "unfinished", "fen_games", "team_games", "parse_errors", "ctml_valid", "ctml_path", "pgn_path", "file")


def stage_export(con: sqlite3.Connection) -> None:
    rows = con.execute(f"select {', '.join(EXPORT_COLUMNS)} from event order by start, slug").fetchall()
    target = HOME / "catalog.csv"
    with target.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(EXPORT_COLUMNS)
        writer.writerows([tuple(r) for r in rows])
    log(f"export: {len(rows)} events -> {target}")


def stage_status(con: sqlite3.Connection) -> None:
    one = lambda sql: con.execute(sql).fetchone()[0]
    log(f"files seen: {one('select count(*) from file')}  "
        f"(empty: {one('''select count(*) from file where status = 'empty' ''')}, "
        f"problems: {one('''select count(*) from file where status not in ('ok', 'empty')''')})")
    log(f"events: {one('select count(*) from event')}, games: {one('select coalesce(sum(games), 0) from event')}")
    for r in con.execute("select decision, count(*) n, sum(games) g from event group by 1 order by 2 desc"):
        log(f"  {r['decision'] or 'unclassified':8} {r['n']:6} events  {r['g']:9} games")
    log(f"Elo tags from the wrong rating list: {one('select count(*) from event where elo_suspect = 1')}")
    log(f"date-suspect events: {one('select count(*) from event where date_suspect = 1')}")
    log(f"matched in Elysium: {one('select count(*) from event where ely_event_id is not null')}")
    log(f"matched to an archive crosstable: {one('select count(*) from event where xt_id is not null')} "
        f"(name, dates and place taken from it: {one('select count(*) from event where xt_name_ok = 1')})")
    log(f"ctml written: {one('select count(*) from event where ctml_path is not null')} "
        f"(invalid: {one('select count(*) from event where ctml_valid = 0')}), "
        f"{one('select coalesce(sum(ctml_bytes), 0) from event') / 1e6:.0f} MB")


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=("scan", "enrich", "match", "tables", "classify", "ctml", "pgn", "verify",
                                      "publish", "bundles", "export", "status", "run"))
    ap.add_argument("--inbox", type=Path, action="append",
                    help="folder of downloaded PGNs (repeatable); default: the folders in INBOXES")
    ap.add_argument("--limit", type=int, help="process at most N events in this stage")
    ap.add_argument("--only", help="ctml/pgn: just this event slug")
    ap.add_argument("--rescan", action="store_true", help="scan: read every file again, not just new ones")
    ap.add_argument("--workers", type=int, default=WORKERS, help="worker processes for ctml, pgn and verify")
    ap.add_argument("--home", type=Path, help="where the catalog and outputs live (default: beside this script)")
    args = ap.parse_args()
    if args.home:
        global HOME, CATALOG, CTML_OUT, PGN_OUT
        HOME = args.home.resolve()
        CATALOG, CTML_OUT, PGN_OUT = HOME / "catalog.sqlite", HOME / "ctml", HOME / "pgn"
        HOME.mkdir(parents=True, exist_ok=True)
    _quiet_chess()
    con = open_catalog()
    stages = (("scan", "enrich", "match", "tables", "classify", "ctml", "pgn", "export", "status")
              if args.stage == "run" else (args.stage,))
    for stage in stages:
        if stage == "scan":
            stage_scan(con, args.inbox or INBOXES, args.limit, args.rescan)
        elif stage == "enrich":
            stage_enrich(con, args.limit)
        elif stage == "match":
            stage_match(con, args.limit)
        elif stage == "tables":
            stage_tables(con, args.limit)
        elif stage == "classify":
            stage_classify(con)
        elif stage == "ctml":
            stage_ctml(con, args.limit, args.only, args.workers)
        elif stage == "pgn":
            stage_pgn(con, args.limit, args.only, args.workers)
        elif stage == "verify":
            stage_verify(con, args.limit, args.only, args.workers)
        elif stage == "publish":
            stage_publish(con, args.workers)
        elif stage == "bundles":
            stage_bundles(con)
        elif stage == "export":
            stage_export(con)
        elif stage == "status":
            stage_status(con)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
