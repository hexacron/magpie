"""Command line entry point: `magpie <verb>`.

Local use never needs the web server; `magpie capture` writes the same sealed
packages the UI does.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings, load_settings
from .models import Manifest
from .store import Store

__all__ = ["main"]


def _bold(text: str) -> str:
    return f"\033[1m{text}\033[0m" if sys.stdout.isatty() else text


def _print_manifest(manifest: Manifest) -> None:
    if manifest.status == "error":
        print(f"  error   {manifest.input}: {'; '.join(manifest.warnings) or 'failed'}")
        return
    post = manifest.post
    flags = manifest.crosscheck.flags if manifest.crosscheck else []
    print(f"  {manifest.status:<11} {manifest.folder}")
    print(f"    author    @{manifest.screen_name or 'unknown'}")
    if post and post.text:
        text = post.text.replace("\n", " ")
        print(f"    text      {text[:96]}{'...' if len(text) > 96 else ''}")
    print(f"    sources   {', '.join(post.available_sources) if post else 'none'}")
    if post and post.media:
        print(f"    media     {len(post.media)} file(s)")
    if flags:
        print(f"    flags     {', '.join(flags)}")
    if manifest.render and not manifest.render.pdf and manifest.render.error:
        print(f"    render    {manifest.render.error}")
    ts = manifest.timestamp
    if ts and ts.enabled:
        state = f"ok ({ts.gen_time})" if ts.ok else f"failed: {ts.error}"
        print(f"    timestamp {state}")


def _read_inputs(args: argparse.Namespace) -> str:
    if args.links:
        return "\n".join(args.links)
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def cmd_capture(args: argparse.Namespace, settings: Settings) -> int:
    from .capture import capture_many

    raw = _read_inputs(args)
    if not raw.strip():
        print("no links given (pass URLs/ids as arguments or on stdin)", file=sys.stderr)
        return 2

    settings.ensure_dirs()
    store = Store(settings)
    manifests = asyncio.run(capture_many(raw, settings, store, operator=args.operator))

    if args.json:
        print(json.dumps([m.to_dict() for m in manifests], indent=2, ensure_ascii=False))
        return 0 if all(m.status != "error" for m in manifests) else 1

    print(_bold(f"{len(manifests)} capture(s)"))
    for manifest in manifests:
        _print_manifest(manifest)
        if manifest.folder:
            sha = store.get_row(manifest.folder)
            if sha:
                print(f"    manifest  {sha.manifest_sha256}")
    return 0 if all(m.status != "error" for m in manifests) else 1


def cmd_serve(args: argparse.Namespace, settings: Settings) -> int:
    import uvicorn

    from .web import create_app

    settings.ensure_dirs()
    host = args.host or settings.host
    port = args.port or settings.port
    if not settings.auth_token:
        print(
            "WARNING: MAGPIE_AUTH_TOKEN is unset - capture/edit/delete routes are open. "
            "Do not expose this instance publicly.",
            file=sys.stderr,
        )
    uvicorn.run(create_app(settings), host=host, port=port, log_level=args.log_level)
    return 0


def cmd_verify(args: argparse.Namespace, settings: Settings) -> int:
    store = Store(settings)
    folders = [r.folder for r in store.search(limit=100000)[0]] if args.all else args.folders
    if not folders:
        print("nothing to verify", file=sys.stderr)
        return 2

    failures = 0
    for folder in folders:
        result = store.verify(folder)
        mark = "ok  " if result.ok else "FAIL"
        print(f"{mark} {folder}")
        if not result.ok:
            failures += 1
            for name in result.mismatched:
                print(f"     modified: {name}")
            for name in result.missing:
                print(f"     missing:  {name}")
            for name in result.unexpected:
                print(f"     extra:    {name}")
            for err in result.errors:
                print(f"     error:    {err}")
        if result.timestamp_ok is not None:
            print(f"     rfc3161:  {'verified' if result.timestamp_ok else 'FAILED'}")
    print(f"\n{len(folders) - failures}/{len(folders)} package(s) intact")
    return 1 if failures else 0


def cmd_reindex(args: argparse.Namespace, settings: Settings) -> int:
    store = Store(settings)
    count = store.reindex()
    print(f"indexed {count} capture(s) from {settings.captures_dir}")
    return 0


def cmd_list(args: argparse.Namespace, settings: Settings) -> int:
    store = Store(settings)
    rows, total = store.search(q=args.query, user=args.user, limit=args.limit)
    print(_bold(f"{len(rows)} of {total} capture(s)"))
    for row in rows:
        flags = f"  [{len(row.flags)} flag]" if row.flags else ""
        text = (row.text or "").replace("\n", " ")[:60]
        print(f"  {row.capture_time_utc}  {row.status:<11} @{(row.screen_name or 'unknown'):<18} {text}{flags}")
    return 0


def cmd_export(args: argparse.Namespace, settings: Settings) -> int:
    store = Store(settings)
    payload = (
        json.dumps(store.export_rows(), indent=2, ensure_ascii=False)
        if args.format == "json"
        else store.export_csv()
    )
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
        print(f"wrote {args.output}")
    else:
        print(payload)
    return 0


def cmd_stats(args: argparse.Namespace, settings: Settings) -> int:
    store = Store(settings)
    for key, value in store.stats().items():
        print(f"{key:<16} {value}")
    return 0


def cmd_config(args: argparse.Namespace, settings: Settings) -> int:
    data = {
        "data_dir": str(settings.data_dir),
        "captures_dir": str(settings.captures_dir),
        "index_db": str(settings.index_db),
        "host": settings.host,
        "port": settings.port,
        "base_path": settings.base_path,
        "auth_token": "set" if settings.auth_token else "UNSET (writes are open)",
        "public_read": settings.public_read,
        "sources": settings.sources,
        "user_agents": settings.user_agents,
        "download_media": settings.download_media,
        "ocr": settings.ocr,
        "render": settings.render,
        "tsa": settings.tsa,
        "tsa_url": settings.tsa_url if settings.tsa else None,
        "operator": settings.operator,
    }
    print(json.dumps(data, indent=2))
    return 0


# ---------------------------------------------------------------- extraction


def _dataset(settings: Settings):
    from .dataset import Dataset

    return Dataset(settings)


def _report_line(label: str, r) -> str:
    return (
        f"{label}: {r.new_posts} new, {r.updated_posts} updated, "
        f"{r.already_known} skipped, {r.hydrated} hydrated in {r.elapsed_ms} ms"
    )


def cmd_pull(args: argparse.Namespace, settings: Settings) -> int:
    from .pull import pull

    targets = args.targets or ([] if sys.stdin.isatty() else sys.stdin.read().split())
    if not targets:
        print("nothing to pull (pass @handles, URLs or ids)", file=sys.stderr)
        return 2

    ds = _dataset(settings)
    report = asyncio.run(pull(targets, settings, ds, refresh=args.refresh, deep=args.deep))
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(_bold(_report_line("pull", report)))
        for err in report.errors:
            print(f"  ! {err}")
    ds.close()
    return 0


def cmd_watch(args: argparse.Namespace, settings: Settings) -> int:
    from .pull import watch

    if not args.handles:
        print("watch needs at least one @handle", file=sys.stderr)
        return 2
    ds = _dataset(settings)

    def on_round(n: int, r) -> None:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%SZ")
        print(f"[{stamp}] round {n}: {_report_line('poll', r)}")
        for err in r.errors:
            print(f"  ! {err}")

    try:
        asyncio.run(
            watch(
                args.handles,
                settings,
                ds,
                interval=args.interval,
                rounds=args.rounds,
                on_round=on_round,
            )
        )
    except KeyboardInterrupt:
        print("\nstopped")
    ds.close()
    return 0


def cmd_thread(args: argparse.Namespace, settings: Settings) -> int:
    from .pull import expand_thread

    ds = _dataset(settings)
    report = asyncio.run(
        expand_thread(args.tweet_id, settings, ds, depth=args.depth, max_posts=args.max_posts)
    )
    print(_bold(_report_line("thread", report)))
    for err in report.errors[:10]:
        print(f"  ! {err}")
    ds.close()
    return 0


def cmd_posts(args: argparse.Namespace, settings: Settings) -> int:
    ds = _dataset(settings)
    rows, total = ds.posts(
        handle=args.user,
        q=args.query,
        since=args.since,
        until=args.until,
        has_media=args.media or None,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    else:
        print(_bold(f"{len(rows)} of {total} post(s)"))
        for row in rows:
            text = (row.get("text") or "").replace("\n", " ")[:78]
            print(
                f"  {row.get('created_at_utc') or '-':<21} @{(row.get('screen_name') or '?'):<16}"
                f" {(row.get('likes') or 0):>7} likes  {text}"
            )
    ds.close()
    return 0


def cmd_query(args: argparse.Namespace, settings: Settings) -> int:
    ds = _dataset(settings)
    try:
        rows = ds.query(args.sql)
    except ValueError as exc:
        print(f"rejected: {exc}", file=sys.stderr)
        ds.close()
        return 2
    print(json.dumps(rows, indent=2, ensure_ascii=False) if args.json else _table(rows))
    ds.close()
    return 0


def _table(rows: list[dict]) -> str:
    if not rows:
        return "(no rows)"
    cols = list(rows[0].keys())
    widths = {c: min(40, max(len(c), *(len(str(r.get(c, ""))) for r in rows))) for c in cols}
    out = ["  ".join(c.ljust(widths[c]) for c in cols)]
    out.append("  ".join("-" * widths[c] for c in cols))
    for r in rows:
        out.append("  ".join(str(r.get(c, ""))[: widths[c]].ljust(widths[c]) for c in cols))
    return "\n".join(out)


def cmd_dump(args: argparse.Namespace, settings: Settings) -> int:
    ds = _dataset(settings)
    fn = ds.export_jsonl if args.format == "jsonl" else ds.export_csv
    result = fn(args.output, args.user)
    print(result if not args.output else f"wrote {result}")
    ds.close()
    return 0


def cmd_data_stats(args: argparse.Namespace, settings: Settings) -> int:
    ds = _dataset(settings)
    for key, value in ds.stats().items():
        print(f"{key:<18} {value}")
    ds.close()
    return 0


def _session_for(settings: Settings):
    """Authenticated session when accounts exist, guest otherwise.

    Search only works authenticated; timelines work either way, so the guest
    path stays as the fallback rather than a hard failure.
    """
    from . import xapi
    from .accounts import AccountStore
    from .xauth import AuthSession

    if settings.auth_collection:
        store = AccountStore(settings)
        session = AuthSession(settings, store)
        if session.available:
            return session, store
    return xapi.GuestSession(settings), None


def cmd_search(args: argparse.Namespace, settings: Settings) -> int:
    from .pull import _store, client_for
    from .search import build_query, search

    query = build_query(
        args.query,
        since=args.since,
        until=args.until,
        from_user=args.from_user,
        to_user=args.to_user,
        lang=args.lang,
        has_media=args.media,
        min_likes=args.min_likes,
        replies=not args.no_replies,
    )
    session, _store_ref = _session_for(settings)
    if not hasattr(session, "available") or not getattr(session, "available", True):
        print("search requires an account: add one with `magpie accounts add`", file=sys.stderr)
        return 2

    ds = _dataset(settings)
    report = PullReportShim()

    async def run():
        async with client_for(settings) as client:
            posts, err = await search(
                client, session, query, settings, product=args.product, limit=args.limit
            )
            return posts, err

    posts, err = asyncio.run(run())
    if not args.no_store:
        _store(ds, posts, report)
    print(_bold(f'query: {query}'))
    print(f"{len(posts)} post(s), {report.new_posts} new, {report.updated_posts} updated")
    if err:
        print(f"  ! {err}")
    if args.json:
        print(json.dumps([p.to_dict() for p in posts], indent=2, ensure_ascii=False))
    else:
        for p in posts[: args.show]:
            text = (p.text or "").replace("\n", " ")[:78]
            print(f"  {p.created_at_utc or '-':<21} @{(p.screen_name or '?'):<16} {text}")
    ds.close()
    return 0 if not err else 1


class PullReportShim:
    """Minimal counter object accepted by pull._store."""

    def __init__(self) -> None:
        self.new_posts = 0
        self.updated_posts = 0
        self.users = 0
        self.errors: list[str] = []


def cmd_accounts(args: argparse.Namespace, settings: Settings) -> int:
    from .accounts import AccountStore

    store = AccountStore(settings)
    action = args.action

    if action == "add":
        if not args.auth_token or not args.ct0:
            print("need --auth-token and --ct0 (copy them from a logged-in browser's cookies)",
                  file=sys.stderr)
            return 2
        acct = store.add(args.label, args.auth_token, args.ct0, proxy=args.proxy, note=args.note)
        print(f"added {acct.label} -> {store.path} (0600)")
        return 0

    if action == "rm":
        ok = store.remove(args.label)
        print(f"removed {args.label}" if ok else f"no such account: {args.label}")
        return 0 if ok else 1

    stats = store.stats()
    print(_bold(f"{stats.get('total', 0)} account(s) in {store.path}"))
    for row in stats.get("accounts", []):
        print(f"  {row.get('label','?'):<16} {row.get('state','?'):<8} "
              f"auth={row.get('auth_token','-')} ct0={row.get('ct0','-')} "
              f"fails={row.get('failures', 0)} {row.get('note') or ''}")
    for warn in store.warnings:
        print(f"  ! {warn}")
    return 0


def cmd_queryids(args: argparse.Namespace, settings: Settings) -> int:
    from . import queryids
    from .pull import client_for

    async def run():
        async with client_for(settings) as client:
            return await queryids.ensure(client, settings, refresh=args.refresh)

    ids = asyncio.run(run())
    print(_bold(f"{len(ids)} operation(s) cached at {queryids.cache_path(settings)}"))
    for op in sorted(args.only or ["SearchTimeline","UserTweets","TweetDetail","UserByScreenName",
                                   "Followers","Following","HomeTimeline","Likes"]):
        print(f"  {op:<22} {ids.get(op, '-')}")
    return 0



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="magpie",
        description="Pull public X/Twitter post data into a queryable dataset "
        "(and, optionally, capture hashed evidence packages).",
    )
    parser.add_argument("--data-dir", help="override MAGPIE_DATA_DIR")
    sub = parser.add_subparsers(dest="command", required=True)

    pl = sub.add_parser("pull", help="pull posts for @handles, URLs or ids into the dataset")
    pl.add_argument("targets", nargs="*", help="@handle, post URL or bare id (stdin when omitted)")
    pl.add_argument("--refresh", action="store_true", help="re-hydrate ids already stored")
    pl.add_argument(
        "--deep",
        action="store_true",
        help="also pull each handle's guest-API archive (~100 high-engagement historic "
        "posts; breadth, not freshness)",
    )
    pl.add_argument("--json", action="store_true")
    pl.set_defaults(func=cmd_pull)

    wa = sub.add_parser("watch", help="poll a watchlist on an interval")
    wa.add_argument("handles", nargs="*")
    wa.add_argument("--interval", type=float, help="seconds between rounds (default MAGPIE_WATCH_INTERVAL)")
    wa.add_argument("--rounds", type=int, help="stop after N rounds (default: run forever)")
    wa.set_defaults(func=cmd_watch)

    th = sub.add_parser("thread", help="crawl outward from one post")
    th.add_argument("tweet_id")
    th.add_argument("--depth", type=int)
    th.add_argument("--max-posts", type=int, dest="max_posts")
    th.set_defaults(func=cmd_thread)

    po = sub.add_parser("posts", help="list/search stored posts")
    po.add_argument("query", nargs="?", help="full-text query")
    po.add_argument("--user")
    po.add_argument("--since")
    po.add_argument("--until")
    po.add_argument("--media", action="store_true", help="only posts with media")
    po.add_argument("--limit", type=int, default=25)
    po.add_argument("--json", action="store_true")
    po.set_defaults(func=cmd_posts)

    qy = sub.add_parser("query", help="read-only SQL against the dataset")
    qy.add_argument("sql")
    qy.add_argument("--json", action="store_true")
    qy.set_defaults(func=cmd_query)

    dp = sub.add_parser("dump", help="export the dataset")
    dp.add_argument("format", choices=["jsonl", "csv"])
    dp.add_argument("-o", "--output")
    dp.add_argument("--user")
    dp.set_defaults(func=cmd_dump)

    ds = sub.add_parser("data", help="dataset statistics")
    ds.set_defaults(func=cmd_data_stats)

    se = sub.add_parser("search", help="keyword search (requires an account)")
    se.add_argument("query")
    se.add_argument("--product", default="Latest", choices=["Latest", "Top", "Media", "People"])
    se.add_argument("--limit", type=int, default=100)
    se.add_argument("--show", type=int, default=15, help="rows to print")
    se.add_argument("--since", help="YYYY-MM-DD")
    se.add_argument("--until", help="YYYY-MM-DD")
    se.add_argument("--from", dest="from_user", help="only posts from this handle")
    se.add_argument("--to", dest="to_user", help="only replies to this handle")
    se.add_argument("--lang")
    se.add_argument("--media", action="store_true", help="filter:media")
    se.add_argument("--min-likes", type=int, dest="min_likes")
    se.add_argument("--no-replies", action="store_true")
    se.add_argument("--no-store", action="store_true", help="print without writing to the dataset")
    se.add_argument("--json", action="store_true")
    se.set_defaults(func=cmd_search)

    ac = sub.add_parser("accounts", help="manage collection accounts (stored 0600, never in env)")
    ac.add_argument("action", nargs="?", default="list", choices=["list", "add", "rm"])
    ac.add_argument("label", nargs="?")
    ac.add_argument("--auth-token", dest="auth_token")
    ac.add_argument("--ct0")
    ac.add_argument("--proxy")
    ac.add_argument("--note")
    ac.set_defaults(func=cmd_accounts)

    qi = sub.add_parser("queryids", help="scrape and cache X's current GraphQL query ids")
    qi.add_argument("--refresh", action="store_true")
    qi.add_argument("--only", nargs="*")
    qi.set_defaults(func=cmd_queryids)

    cap = sub.add_parser("capture", help="evidence mode: capture a sealed package")
    cap.add_argument("links", nargs="*", help="post URLs or bare ids (stdin when omitted)")
    cap.add_argument("--json", action="store_true", help="print manifests as JSON")
    cap.add_argument("--operator", help="name recorded in the manifest")
    cap.set_defaults(func=cmd_capture)

    serve = sub.add_parser("serve", help="run the web UI + API")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=cmd_serve)

    ver = sub.add_parser("verify", help="recompute hashes and check a package")
    ver.add_argument("folders", nargs="*")
    ver.add_argument("--all", action="store_true")
    ver.set_defaults(func=cmd_verify)

    idx = sub.add_parser("reindex", help="rebuild the sqlite index from disk")
    idx.set_defaults(func=cmd_reindex)

    lst = sub.add_parser("list", help="list captures")
    lst.add_argument("query", nargs="?")
    lst.add_argument("--user")
    lst.add_argument("--limit", type=int, default=25)
    lst.set_defaults(func=cmd_list)

    exp = sub.add_parser("export", help="export the index")
    exp.add_argument("format", choices=["json", "csv"])
    exp.add_argument("-o", "--output")
    exp.set_defaults(func=cmd_export)

    st = sub.add_parser("stats", help="archive statistics")
    st.set_defaults(func=cmd_stats)

    cfg = sub.add_parser("config", help="print effective settings")
    cfg.set_defaults(func=cmd_config)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings()
    if args.data_dir:
        settings.data_dir = Path(args.data_dir).expanduser()
    settings.ensure_dirs()
    return int(args.func(args, settings) or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
