# Chess.com events pipeline

`cc_events.py` turns the raw Chess.com event PGNs into a catalog, a CTML record
and a clean PGN for every event that passes the strength test, and the ZIPs and
manifest this repository publishes.

**The test:** tournament average of 2300 or more, and no player below 2000, both
on FIDE standard ratings at the time of the event.

## Run it

Use the CTML project's interpreter (it has python-chess and lxml), from the
repository root:

```powershell
cd D:\dev\proj\chessnerd\cc-events-archive
D:\dev\proj\ctml\.venv\Scripts\python.exe pipeline\cc_events.py run
```

`run` does `scan`, `enrich`, `match`, `tables`, `classify`, `ctml`, `pgn`,
`export`, `status` in order. Each can be run alone. Two more are never part of
`run`:

- `verify` replays the source PGN and the regenerated PGN side by side and
  reports any game whose result or moves differ.
- `publish` writes `<year>\<slug>.zip` and `cc_events_manifest.json` into the
  working tree. It does not commit or push.

Every stage is incremental. `ctml`, `pgn`, `verify` and `publish` run across
worker processes (`--workers N`, default 12): two to five minutes each for
2,400 events. `enrich` is the slow one, about half a second per new event while
the Elysium file is cold; when thousands of events are new, run it in batches
(`enrich --limit 1100`). `--only <slug>` restricts `ctml`, `pgn` or `verify` to
one event. `scan --rescan` reads every file again.

## To publish

```powershell
D:\dev\proj\ctml\.venv\Scripts\python.exe pipeline\cc_events.py verify
D:\dev\proj\ctml\.venv\Scripts\python.exe pipeline\cc_events.py publish
git add -A
git commit -m "..."
git push
```

Then, in `D:\dev\proj\chessnerd\chessnerd`: `npm run sync:events`, `npm test`,
`npm run build`, commit `public/data`, push. Push this repository first: the
site reads the manifest from GitHub.

## What it reads and writes

| Path | Role |
| --- | --- |
| `D:\dev\proj\cc-events-download`, `D:\all\cc\events`, `D:\dev\proj\chessnerd\cc-events` | The downloaded PGNs, **read only** (`INBOXES` in the script). All three are read; a file in two of them is taken from the newer copy, and a file moved between them is recognised, not re-read. Files modified in the last two minutes and `.crdownload` files are left alone. Zero-byte `games*.pgn` files are event ids with nothing behind them. |
| `D:\elysium\db\elysium.db` | Opened read-only. Monthly FIDE standard ratings by FIDE id (through 2025-12), titles, birth years, and the Mega / GigaKing / OlimpBase event records used for places. |
| `D:\dev\proj\ctml\assets\all.tsv` | Opening table: ECO code and name by position. |
| `D:\dev\proj\ctml\crosstables\crosstables.json` | Parsed TWIC, OlimpBase, chess-results and NWChess crosstables; 37,859 dated ones are copied into the catalog's `xtab` table. The source-evidence archive's `archive.sqlite` holds the same records but is rewritten in place while it is being deduplicated, so it is not read. |
| `work\catalog.sqlite`, `work\catalog.csv` | Working state and its spreadsheet export. Not committed. |
| `work\ctml\<year>\`, `work\pgn\<year>\` | CTML and PGN for each kept event. Not committed; files no kept event owns are deleted after `pgn`. |
| `<year>\<slug>.zip`, `cc_events_manifest.json` | The published tree, written by `publish`. |

Deleting `work\catalog.sqlite` starts over; nothing else depends on it.

## Why Chess.com's tags are not taken at face value

Checked against the monthly rating lists and the events' crosstables, the files
have four recurring faults. The catalog records each per event.

1. **Elo tags from the wrong list** (`elo_suspect`). Events from 2009 through
   September 2020 carry the ratings of late 2020 to mid 2021; Chess.com
   backfilled them around April 16, 2021. World Youth U10 2014 averages 2334 by
   its tags and 1781 at the time.
2. **Elo tags from another pool** (`tags_standard` = 0). From October 2020 the
   tags are event-time, but for online, rapid and blitz events they are often
   not standard ratings: of 2,507 later events checked, 925 disagree with the
   standard list.
3. **The wrong person** (`identity_doubt`, `player.xt_id_ok` = 0). Chess.com
   sometimes attached a namesake's FIDE id and name: Anand became "Vishwa Anand
   V", Vugar Gashimov "Hashimov, Ilgar", Michael Adams "Adams, David M".
4. **Import date in place of the event date** (`date_suspect`). The name says
   2012, the Date tags say 2021 or 2026.

## What each stage decides

**scan** — per file: name, dates, rounds, players, per-player Elo, title and
FIDE id, teams, boards, whether the clocks are real, and whether the pairings
form a complete round robin (`format`: `round-robin`, `match`, `team`, or empty).
A few games dated months from the rest (`date_strays`) do not stretch the dates.

**enrich** — per player with a FIDE id: titles, birth year and the standard
rating for the event month from Elysium. Sets `elo_suspect` and `date_suspect`.
Series events (Titled Tuesday and the like) are skipped.

**match** — finds the event among Elysium's event records by start date and
roster. Used for the place, and for an independent average to cross-check.

**tables** — finds the event's own crosstable by date overlap and roster.
Catalog columns start `xt_`.

- From the header: name with city, country code, stated dates, stated category
  and average (all-play-alls), rounds (Swiss standings).
- From the rows, per player: name, title, federation and rating as printed for
  that event, and score and place.
- `xt_kind`: `same` field; `whole` (this file is part of the table's event);
  `part` (the table lists part of this file, usually a Swiss's leaders).
- `xt_fit` is the share of the table's scores that equal the points our games
  give each player. It is what separates a festival's classical event from its
  rapid or blitz, which share a roster. Short of an exact fit the scores must at
  least be possible: a finished table can only show a player more points than
  our file (a partial broadcast), a mid-event table only fewer.
- `xt_name_ok` = 1 when the table is taken to be this event: its name, dates,
  place, country and rounds are used. Scores and places are written only when
  `xt_fit` is 0.8 or more.
- `xt_pool` = `other` when the table prints ratings that are not the standard
  list; those are recorded with scope unknown and not used for the average.
- A row settles identity. A bare "Carlsen" or "Kramnik,W" gets the table's full
  name. The FIDE id is dropped, and the table's name used, only on rating
  evidence: the table rates the player 300+ above that id's rating, or at master
  level when Elysium knows the id and has no rating for it then; or the given
  names differ and the ratings differ by more than 100.

Matches are scored proposals, not reviewed identity decisions.

**classify** — a player's rating is, in order: the rating in the matched
crosstable (standard pool only); Elysium's standard rating for the event month,
or failing that the most recent earlier month that has one, however far back (a
rating carries forward; it never reaches back before the player's first);
the Elo tag, only where the event's tags have been shown to be standard ratings.
For events after Elysium's last list (2025-12) the tag is used when within 100
points of that list.

The tournament average is the mean of those, one per player. Where a matched
all-play-all states its own average and category, those are used (`avg_basis` =
`stated:twic`) and the mean is kept in `avg_computed`. `category` is the FIDE
category (category 1 starts at 2251, 25 points each).

| Decision | Meaning |
| --- | --- |
| `keep` | average 2300 or more and no known rating below 2000 |
| `cull` | average below 2300, or a player below 2000 |
| `review` | fewer than half the players have a usable rating, or the average and Elysium's crosstable average fall on opposite sides of 2300 |
| `engine` | TCEC, or a participant rated above 2900 |
| `series` | Titled Tuesday, Bullet Brawl, 3-0 Thursday, CCC: they have their own archives |

A missing rating is not a zero: the floor and the average use known ratings
only. `unrated_then` counts players Elysium knows by FIDE id and has no rating
for at the time; `unknown_rating` counts players with no usable identity.
Neither removes an event.

**ctml** — writes only what is known; absent facts are left out rather than
guessed. Federations and titles come only from a crosstable (Elysium knows a
player's current federation, not the one played under). Cadence, organizers and
arbiters are never written. Clock times are kept only where the clock actually
ran. Each game gets an ECO code and opening name from its moves, by position
(the deepest named position in the first 40 plies, so transpositions count).
Each game keeps its own date as `<ctml:tag>date=YYYY-MM-DD</ctml:tag>`
because CTML's game element has no date field. Every file is validated against
`D:\dev\proj\ctml\xsd\ctml.xsd`.

**pgn** — the CTML written back to PGN with `EventDate`, `EventType`,
`EventRounds`, `EventCountry`, `EventCategory`, event-time `WhiteElo`/`BlackElo`,
FIDE ids, titles, teams, boards, clock comments where real, and the Chess.com
game URL in `Link`, plus `ECO` and `Opening`. `Site` is the real place as
"City FED" wherever it is known, and `Chess.com` otherwise.

**publish** — one ZIP per kept event holding the PGN and the CTML, with a fixed
timestamp so an unchanged event produces identical bytes, and the manifest.
Entries already in the manifest from the earlier collection are carried over as
`"legacy": true` until their slug has been scanned here; ZIPs the manifest no
longer lists are deleted.

## Known gaps

- World Championship matches have only a schedule table in TWIC, so they get no
  crosstable data.
- The raw TWIC HTML tables (round grids, tiebreaks, issues after July 2026) and
  the chess-results pages (organizers, arbiters, time controls) are not read.
- Elysium's ratings stop at 2025-12, so 2026 events lean on their Elo tags.
- Federations are in the CTML only, not the PGN.
