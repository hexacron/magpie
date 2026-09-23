# Magpie

Self-hosted extraction of **public** X/Twitter post data into a queryable SQLite dataset,
with an optional evidence mode that seals each post into a hashed, self-contained package.

It never uses the X API, a login, or a cookie: everything it reads is reachable from a
logged-out browser. Ships with a CLI (`magpie`) and a web UI. Runs locally or in a container.

## Data extraction (the default workflow)

```sh
magpie pull @AFP @Reuters @BBCWorld       # poll profiles, hydrate, store
magpie pull https://x.com/jack/status/20  # or a specific post
magpie watch @AFP @Reuters --interval 300 # keep polling a watchlist
magpie thread <post_id> --depth 2         # crawl outward from one post
magpie posts "Trump OR Greenland" --limit 20
magpie query "SELECT screen_name, count(*) n, sum(likes) FROM posts GROUP BY 1 ORDER BY n DESC"
magpie dump jsonl -o posts.jsonl
```

Measured on this machine: **15 accounts, 75 posts, 7.9 s** (~9.5 posts/sec). Handles are
polled concurrently and ids are hydrated concurrently within each handle.

### What is actually reachable without a login

| Surface | Yield |
| --- | --- |
| `x.com/<handle>` server-rendered page | **5-6 recent post ids** (pinned + latest). The only surface that reflects the last minutes. |
| `x.com/<handle>/status/<id>` page | The post plus ~2 related/reply ids. |
| **guest GraphQL `UserTweets`** | **~100 posts in one call** (`magpie pull --deep`). Anonymous guest token, no account. |
| **guest GraphQL `TweetDetail`** | Whole threaded conversation in one call, when its query id is alive. |
| **guest GraphQL `UserByScreenName`** | Full user object. |
| syndication / fxtwitter / vxtwitter | Full hydration of any known id; fx and vx also return user profiles. |
| `with_replies`, `/media`, `/search`, `/hashtag`, `/explore` | **Nothing.** Logged out these return an empty app shell. |
| GraphQL `SearchTimeline`, `2/search/adaptive.json`, `1.1/search/*` | **Nothing.** Search requires a real account. |
| `syndication.twitter.com/srv/timeline-profile` | `429` from most egress IPs. Off by default (`MAGPIE_DISCOVER_TIMELINE=1`). |

### The guest API (`--deep`)

`POST https://api.x.com/1.1/guest/activate.json` with the public web bearer returns an
anonymous guest token; that token alone unlocks `UserTweets`, `TweetDetail` and
`UserByScreenName`. No account, no cookies, nothing to get banned.

Two measured caveats decide how it is wired:

1. **It is breadth, not freshness.** What it returns varies by account: `@BBCWorld` gave
   100 posts covering the last 4 days, while `@AFP` gave 102 posts spanning 2017-2025 with
   an average of 7.9k likes — its high-engagement archive, not its recent output. The live
   responses carried **no pagination cursor**, so you cannot page deeper. It therefore runs
   only under `--deep` and never replaces the SSR poll that provides freshness.
2. **Query ids rotate, fast.** The `TweetDetail` id verified working at the start of this
   session returned `404` roughly thirty minutes later. A dead id is a `404` with an empty
   body; `xapi.py` walks a candidate list, remembers the winner, reports
   `no working query id for <op> (ids rotate; update QUERY_IDS)`, and the caller falls back
   to the SSR path. Override without a code change: `MAGPIE_QUERY_ID_TWEETDETAIL=<id>`.

**The consequence that matters:** a profile page exposes only ~5 posts, so coverage is a
function of poll cadence. An account posting faster than 5 posts per interval will lose
posts between polls. `magpie watch` detects this — when every discovered id in a round is new,
it reports that the timeline rolled over and the interval is too long.

Hydration deliberately skips `x_page` (~215 KB per post, adds nothing the JSON sources
lack). `MAGPIE_PULL_SOURCES` defaults to `syndication,fxtwitter` — about 9 KB per post.

### The dataset

SQLite at `$MAGPIE_DATA_DIR/dataset.db`: `posts`, `users`, `media`, `cursors`, plus an FTS5
index over post text. Re-polling a stored post updates its counters and `last_seen_utc`
while preserving `first_seen_utc`, so engagement over time is recoverable. `magpie query` is
read-only (`SELECT`/`WITH` only, no stacked statements, `PRAGMA query_only=ON`).

## Monitoring a fixed set of accounts

```sh
magpie watchlist add @Reuters @AFP @BBCWorld --tag news
magpie watchlist add @elonmusk --interval 120 --tag tech
magpie watchlist tune            # set each cadence from that account's own posting rate
magpie watchlist                 # show state: interval, polls, new, rollovers, last error

magpie monitor --notify 'webhook:https://hooks.example.com/x' --notify file:/var/log/x.jsonl
```

`monitor` differs from `watch` in the thing that actually matters: it polls each account on
**its own cadence** rather than one global interval. That is not a preference. Measured over
two days of collection, `@Reuters` averaged a post every 364 s — so the 5-id profile page
rolls over in about 30 minutes — while `@CNN` averaged one every 3092 s. A single interval
either burns requests on the quiet account or silently loses posts on the busy one.

### Self-tuning cadence

When a poll returns a page where *every* id is new, the page rolled over between polls and
posts were missed. Magpie treats that as evidence and **halves that account's interval**
(floor 60 s), relaxing it back by 25% after five clean polls. An account's first poll is
exempt, since everything is new by definition.

`magpie watchlist tune` seeds the cadence from measured history, with two deliberate biases:
it ignores posts older than 14 days (a `--deep` backfill otherwise drags the median gap from
minutes to months), and it never proposes an interval *slower* than the global default —
a poll only ever reveals 5 ids, so observed gaps overstate the true rate, and the error is
asymmetric: over-polling costs requests, under-polling loses posts.

### Sinks

New posts are delivered to any combination of `--notify` targets:

| Spec | Behaviour |
| --- | --- |
| `webhook:https://…` | `POST {event, count, context, posts[]}`, chunked at 100 posts, 2 retries on 5xx/timeout, never on 4xx. `MAGPIE_WEBHOOK_TOKEN` adds a bearer header. |
| `file:/path.jsonl` | Appends one JSON object per line; reopened per dispatch so log rotation works. |
| `cmd:'notify-send {count} new'` | Runs argv (never a shell) with the posts as JSON on stdin. |
| `stdout` | Human-readable lines. |

A sink that fails cannot stop collection: each runs concurrently and any exception becomes a
logged delivery error.

### As a service

`docker compose up -d` starts the `monitor` service alongside the web UI, sharing one volume
so collected posts are immediately queryable and browsable:

```sh
docker compose exec monitor magpie watchlist add @Reuters @AFP
docker compose logs -f monitor
```

Seed a fresh deployment without an exec step by setting `MAGPIE_WATCH="@Reuters @AFP"` in
`.env`. With an empty watchlist the daemon waits for accounts rather than exiting — exiting
would crash-loop under `restart: unless-stopped`, and a crash-looping container cannot be
`exec`d into to fix itself.

## API

`magpie serve` exposes a JSON API at `/api/v1` that does what the CLI does: read the dataset,
drive collection, edit the watchlist, check the monitor. Every route — reads included —
requires `MAGPIE_AUTH_TOKEN`:

```sh
curl -H "Authorization: Bearer $MAGPIE_AUTH_TOKEN" 127.0.0.1:8099/api/v1/stats
```

`X-Auth-Token` and the `xw_token` cookie the web UI sets at `/login` are accepted too, which
is what makes the self-hosted Swagger UI at `/api/v1/docs` usable from a logged-in browser.
(`MAGPIE_PUBLIC_READ` only ever relaxes the HTML routes. It does not open the API.)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/posts` | Filter by `q`, `user`, `since`, `until`, `media`, `limit`, `offset` |
| `GET` | `/posts/{id}` | One post with its media rows |
| `GET` | `/users` | Profiles seen while collecting |
| `GET` | `/stats` | Dataset, capture-store and watchlist counters |
| `POST` | `/query` | `{"sql": "SELECT ...", "params": [...]}` — one read-only statement |
| `GET` | `/export/{jsonl,csv}` | Whole dataset, optionally `?handle=` |
| `GET` | `/watchlist` | Watched accounts and their effective cadence |
| `POST` | `/watchlist` | `{"handles": ["@a"], "interval": 300, "tags": [...]}` |
| `PATCH` | `/watchlist/{handle}` | Set `interval` (null clears) or `enabled` |
| `DELETE` | `/watchlist/{handle}` | Stop watching |
| `POST` | `/watchlist/{handle}/tune` | Cadence from that account's measured posting rate |
| `POST` | `/jobs/pull` | `{"targets": ["@handle", "<url or id>"]}` |
| `POST` | `/jobs/search` | `{"query": "..."}` or `text`/`from`/`since`/… — needs an account |
| `POST` | `/jobs/thread` | `{"tweet_id": "...", "depth": 2}` |
| `POST` | `/jobs/capture` | `{"links": [...]}` — builds evidence packages |
| `GET` | `/jobs` | Submitted jobs, newest first; filter by `status`/`kind` |
| `GET` | `/jobs/{id}` | Status, progress and `result` |
| `DELETE` | `/jobs/{id}` | Cancel a queued or running job |
| `GET` | `/monitor` | Per-account poll liveness — `magpie health` as data |
| `GET` | `/captures`, `/captures/{folder}`, `/captures/{folder}/verify` | Evidence packages |

Collection is slow — a pull walks a timeline, a capture renders a PDF — so those four
endpoints return `202` with a job id and the caller polls instead of holding a request open:

```sh
JOB=$(curl -sS -X POST -H "Authorization: Bearer $MAGPIE_AUTH_TOKEN" \
  -H 'Content-Type: application/json' -d '{"targets":["@Reuters"]}' \
  127.0.0.1:8099/api/v1/jobs/pull | jq -r .job.id)

curl -sS -H "Authorization: Bearer $MAGPIE_AUTH_TOKEN" \
  127.0.0.1:8099/api/v1/jobs/$JOB | jq '{status, new: .result.new_posts}'
# {"status": "completed", "new": 5}
```

Jobs live in the web process only: a restart forgets them. What they produce is already in
sqlite, so nothing is lost but the receipt. `MAGPIE_API_JOB_CONCURRENCY` defaults to 2 so
API-triggered collection does not stampede X alongside the monitor daemon.

## Evidence mode (optional)

`magpie capture` and the web UI produce the sealed package described below — raw source
responses, media, an HTML/PDF/PNG record, a SHA-256 manifest and an optional RFC 3161
timestamp. Use it when provenance matters; it is slower and writes far more to disk. The
extraction path above never touches it.

## How it works

For each post id, four unauthenticated sources are fetched in parallel:

| Source | Endpoint | What it contributes |
| --- | --- | --- |
| `syndication` | `cdn.syndication.twimg.com/tweet-result?id=…&token=…` | Structured tweet JSON, author profile, media variants. Text of long posts is truncated at 280 chars. |
| `fxtwitter` | `api.fxtwitter.com/<handle>/status/<id>` | Retweet/view/quote/bookmark counts, untruncated `raw_text`, community notes. |
| `vxtwitter` | `api.vxtwitter.com/<handle>/status/<id>` | Independent second structured view for cross-checking. |
| `x_page` | `https://x.com/<handle>/status/<id>` | The server-rendered HTML page, including `og:*` metadata and the post text. |

The `syndication` token is derived from the post id (base-36 of `(id / 1e15) * pi`, with
`.` and runs of `0` removed) at full precision. The handle need not be known in advance —
`i` works as a placeholder for both `x.com` and `fxtwitter`.

The raw bodies are stored verbatim, along with the request and response headers and
best-effort TLS peer information. They are then parsed, merged into a single record, and
cross-checked against each other. Disagreements are reported per field; a source whose text
is merely a truncated prefix of another's is flagged `source_truncated:<name>` rather than
as a content conflict. Media is downloaded to `media/` and referenced by relative path
(images below `MAGPIE_INLINE_IMAGE_BYTES` are inlined into `capture.html`; video never is).
Finally every immutable file is hashed, and the manifest hash can optionally be anchored
with an RFC 3161 timestamp.

Re-capturing the same post creates a new package linked to the previous one, with an
explicit list of what changed.

## Package layout

```
<data_dir>/captures/<YYYYMMDDTHHMMSSZ>_<handle|unknown>_<tweet_id>/
  manifest.json  MANIFEST.sha256  meta.json  timestamp.tsr (optional)
  capture.html  capture.pdf  capture.png
  syndication.json  fxtwitter.json  vxtwitter.json  x_page.html
  media/{avatar.jpg,banner.jpg,photo_01.jpg,video_01.mp4,video_01_thumb.jpg}
```

`MANIFEST.sha256` contains a single line: `<sha256>  manifest.json`.

`manifest.json`, `MANIFEST.sha256`, `meta.json`, `timestamp.tsr` and `timestamp.txt` are
**mutable** and are excluded from the hash tree. Everything else is hashed. Tags and notes
live in `meta.json`, outside the hash chain by design — annotating a capture must never
change its evidentiary hashes.

## Quick start — local

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e ".[all]"
playwright install chromium        # optional: enables capture.pdf / capture.png

magpie serve                           # web UI on http://127.0.0.1:8099
magpie capture https://x.com/jack/status/20
magpie verify 20240101T120000Z_jack_20
```

Rendering (`playwright`) and OCR (`pytesseract`, `pillow`, plus the `tesseract` binary) are
optional. Without them the app still runs; the corresponding steps are skipped and recorded
as warnings in the manifest.

## CLI

```
magpie serve                          # run the web UI
magpie capture <url|id> [...]         # capture one or more posts; reads stdin if given none
magpie capture --json <url>           # emit the manifest as JSON on stdout
magpie verify <folder> | --all        # recheck the hash chain of one or all packages
magpie list [--limit N]               # recent captures from the index
magpie export json|csv [-o FILE]      # dump the index
magpie reindex                        # rebuild index.db from the packages on disk
magpie config                         # print effective settings (MAGPIE_AUTH_TOKEN redacted)
```

## Quick start — Docker

```sh
cp .env.example .env
# set MAGPIE_AUTH_TOKEN in .env before exposing this anywhere
docker compose up -d
docker compose exec magpie magpie capture https://x.com/jack/status/20
```

The image includes Chromium and tesseract. Captures persist in the `magpie-data` volume mounted
at `/data`.

## Configuration

Every setting is an environment variable. See `.env.example`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `MAGPIE_DATA_DIR` | `./data` | Root for `captures/` and `index.db` |
| `MAGPIE_HOST` | `127.0.0.1` | Bind address (`0.0.0.0` in the container image) |
| `MAGPIE_PORT` | `8099` | Bind port |
| `MAGPIE_BASE_PATH` | *(empty)* | Subpath prefix when behind a reverse proxy |
| `MAGPIE_SITE_NAME` | `Magpie` | Title in the web UI |
| `MAGPIE_AUTH_TOKEN` | *(unset)* | Token required for all mutating routes |
| `MAGPIE_PUBLIC_READ` | `1` | `0` requires the token for reads as well |
| `MAGPIE_TRUST_PROXY` | `0` | Honour `X-Forwarded-*` headers |
| `MAGPIE_SOURCES` | `syndication,fxtwitter,vxtwitter,x_page` | Sources to fetch |
| `MAGPIE_HTTP_TIMEOUT` | `20.0` | Per-request timeout, seconds |
| `MAGPIE_HTTP_RETRIES` | `2` | Retries per source |
| `MAGPIE_PROXY` | *(unset)* | Outbound proxy URL for source fetches |
| `MAGPIE_MAX_BATCH` | `25` | Max inputs accepted per batch |
| `MAGPIE_CONCURRENCY` | `4` | Parallel captures per batch |
| `MAGPIE_CAPTURE_TLS` | `1` | Record TLS peer info with response headers |
| `MAGPIE_UA_SYNDICATION` | Chrome UA | Per-source User-Agent override |
| `MAGPIE_UA_FXTWITTER` | plain client UA | Per-source User-Agent override |
| `MAGPIE_UA_VXTWITTER` | plain client UA | **Must stay non-browser** or vxtwitter returns 403 |
| `MAGPIE_UA_X_PAGE` | Chrome UA | Per-source User-Agent override |
| `MAGPIE_USER_AGENT` | *(unset)* | Overrides the User-Agent for every source |
| `MAGPIE_DOWNLOAD_MEDIA` | `1` | Download avatars, images and video |
| `MAGPIE_MAX_MEDIA_BYTES` | `134217728` | Per-asset size cap (128 MiB) |
| `MAGPIE_OCR` | `1` | OCR images when tesseract is available |
| `MAGPIE_RENDER` | `1` | Render `capture.pdf` / `capture.png` |
| `MAGPIE_RENDER_PDF` | `1` | Render the PDF |
| `MAGPIE_RENDER_PNG` | `1` | Render the full-page PNG |
| `MAGPIE_CHROMIUM_PATH` | *(unset)* | Explicit Chromium executable |
| `MAGPIE_RENDER_TIMEOUT` | `60.0` | Render timeout, seconds |
| `MAGPIE_INLINE_IMAGE_BYTES` | `2000000` | Images under this size are inlined into `capture.html` |
| `MAGPIE_TSA` | `0` | Enable RFC 3161 timestamping of the manifest hash |
| `MAGPIE_TSA_URL` | `https://freetsa.org/tsr` | Timestamp authority endpoint |
| `MAGPIE_TSA_TIMEOUT` | `20.0` | TSA request timeout, seconds |
| `MAGPIE_OPERATOR` | *(unset)* | Operator recorded in each manifest |
| `MAGPIE_API_JOB_CONCURRENCY` | `2` | API collection jobs running at once; the rest queue |
| `MAGPIE_API_JOB_TTL` | `3600.0` | Seconds a finished job stays readable |
| `MAGPIE_API_JOB_MAX` | `200` | Hard cap on retained jobs |
| `MAGPIE_API_DOCS` | `1` | Serve Swagger UI at `/api/v1/docs` |

## The evidence model

What the hash chain **does** prove: the files in a package have not changed since the
manifest was written, and the manifest itself matches `MANIFEST.sha256`. Anyone with the
package can recompute this independently — no trust in this software required.

What it **does not** prove:

- **When** the capture happened. The timestamp in the manifest is whatever clock the
  capturing machine had. It is self-asserted.
- **That the post said what the package says.** The package proves what these four
  endpoints returned to this client. It is not a statement from X.
- **That the operator did not fabricate the package.** Whoever runs the tool controls the
  inputs and could produce a consistent package from invented data.

RFC 3161 timestamping (`MAGPIE_TSA=1`) is what closes the first gap: a third-party timestamp
authority signs the manifest hash, establishing that the manifest — and therefore the hashed
files — existed no later than the time in `timestamp.tsr`. Without it, a capture is a
self-signed claim. Cross-checking four independent sources raises the cost of the third gap
but does not eliminate it.

## Verification

```sh
magpie verify <folder>
```

This recomputes the hash tree, compares it against `manifest.json`, checks
`MANIFEST.sha256`, and validates the RFC 3161 token if present. It reports each file as
`ok`, `modified`, `missing`, or `extra`.

The equivalent by hand, inside the package directory:

```sh
# 1. manifest integrity
sha256sum -c MANIFEST.sha256

# 2. file integrity: every hash in manifest.json["files"]
python -c 'import json,hashlib,pathlib
m=json.load(open("manifest.json"))
for rel,want in m["files"].items():
    got=hashlib.sha256(pathlib.Path(rel).read_bytes()).hexdigest()
    print(("ok  " if got==want else "BAD "),rel)'

# 3. RFC 3161 token, if timestamp.tsr exists
openssl ts -reply -in timestamp.tsr -text
openssl ts -verify -digest "$(cut -d' ' -f1 MANIFEST.sha256)" \
    -in timestamp.tsr -CAfile tsa-ca.pem
```

The TSA's CA certificate is not bundled; fetch it from the authority named in
`MAGPIE_TSA_URL`.

## Security

**Write routes are open unless `MAGPIE_AUTH_TOKEN` is set.** With no token, anyone who can
reach the instance can trigger captures, edit tags and notes, and delete packages. Do not
expose an instance to a network you do not control without setting it. Set
`MAGPIE_PUBLIC_READ=0` as well if captures should not be readable anonymously.

The container binds `0.0.0.0` inside its namespace; the published port is what determines
exposure. Bind it to `127.0.0.1` and put a TLS-terminating reverse proxy in front — see the
commented block in `docker-compose.yml`. Enable `MAGPIE_TRUST_PROXY=1` only when a proxy you
control sets `X-Forwarded-*`.

Captures perform outbound requests to attacker-influenced URLs (media referenced by a post).
Media size is capped by `MAGPIE_MAX_MEDIA_BYTES`; run the service with no privileged network
access it does not need.

## Legal and ethics

This tool captures **public** posts only. It performs no authentication, uses no API key or
session cookie, and contains no mechanism to access protected accounts, deleted content, or
anything else gated behind a login. It reads the same endpoints a logged-out browser reads.

It is **not** a third-party attestation service. A package is evidence of what a specific
client received at a specific time on a specific machine, made verifiable against tampering
after the fact. Presenting it as proof of publication is a claim about the capture process,
not something this software can certify on your behalf. Where that matters, enable RFC 3161
timestamping and retain the operator identity in `MAGPIE_OPERATOR`.

Respect the terms of service of the endpoints you query and the privacy of the people whose
posts you archive. Capturing a public post does not make redistributing it lawful.

## Authors

Built by [hexacron](https://github.com/hexacron) and Claude (Anthropic), pair-programmed
end to end: every endpoint claim in this README was measured against live X responses
during development rather than taken from documentation or prior art.

## License

MIT — see [LICENSE](LICENSE).
