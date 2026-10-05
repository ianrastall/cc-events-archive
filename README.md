# cc-events-archive

Past Chess.com events with a tournament average of 2300 or more and no player
rated below 2200, one ZIP per event. Fields of more than 200 players only need
the average, and the Olympiads (open and women's) are included whatever theirs.

- **Average and floor** use each player's FIDE standard rating at the time of
  the event. The ratings in Chess.com's own files are not used for this: for
  events before October 2020 they are the ratings of early 2021, and for later
  online, rapid and blitz events they are often not standard ratings.
- **Names** are `YYYY-MM-DD-site-event`: the start date, the town (or
  `chess-com` for an online event or one with no venue on record) and the
  event's name, as in `2023-05-04-bucharest-superbet-classic.zip`.
- **Each ZIP** holds `<name>.pgn` and `<name>.ctml`, a CTML record of the
  tournament: dates, place, category, and each player's title, federation and
  rating at the time, as far as they are known.
- **`cc_events_manifest.json`** lists every event with its dates, place, players,
  rounds, games, average, FIDE category, size and checksum. The Events tab at
  <https://chessnerd.net/chesscom-tournaments.html> is built from it.

Names, places, dates, rounds and categories come from the event's published
crosstable (mostly The Week in Chess) where one could be matched. Otherwise the
name is Chess.com's, the place is the one in the Elysium event records or the
file's own Site tag, and the rounds are those played in the file. An event
carried under several Chess.com ids ("-live", "-secret") appears once. In the
manifest `slug` is the published name and `sourceSlug` is Chess.com's id.

## Layout

| Path | Contents |
| --- | --- |
| `<year>/<name>.zip` | Published events, by the year the event started. |
| `cc_events_manifest.json` | The manifest. |
| `pipeline/` | `cc_events.py`, which produces all of the above, and its README. |
| `work/` | Not committed: the catalog and the unzipped CTML and PGN. |
| `..\cc-events\` | Outside this repository: the PGNs as downloaded from Chess.com. |

See [pipeline/README.md](pipeline/README.md) for how the collection is built.
