# cc-events-archive

Past Chess.com events with a tournament average of 2300 or more and no player
rated below 2000, one ZIP per event.

- **Average and floor** use each player's FIDE standard rating at the time of
  the event. The ratings in Chess.com's own files are not used for this: for
  events before October 2020 they are the ratings of early 2021, and for later
  online, rapid and blitz events they are often not standard ratings.
- **Each ZIP** holds `<slug>.pgn` and `<slug>.ctml`, a CTML record of the
  tournament: dates, place, category, and each player's title, federation and
  rating at the time, as far as they are known.
- **`cc_events_manifest.json`** lists every event with its dates, place, players,
  rounds, games, average, FIDE category, size and checksum. The Events tab at
  <https://chessnerd.net/chesscom-tournaments.html> is built from it.

Names, places, dates, rounds and categories come from the event's published
crosstable (mostly The Week in Chess) where one could be matched; otherwise the
name is Chess.com's and the rest is left blank rather than guessed.

Entries marked `"legacy": true` in the manifest are from the earlier collection
(2024–2026). They hold a PGN only and are replaced as those events are
reprocessed.

## Layout

| Path | Contents |
| --- | --- |
| `<year>/<slug>.zip` | Published events, by the year the event started. |
| `cc_events_manifest.json` | The manifest. |
| `pipeline/` | `cc_events.py`, which produces all of the above, and its README. |
| `work/` | Not committed: the catalog and the unzipped CTML and PGN. |
| `..\cc-events\` | Outside this repository: the PGNs as downloaded from Chess.com. |

See [pipeline/README.md](pipeline/README.md) for how the collection is built.
