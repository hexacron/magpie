"""Environment-driven settings. No framework, no magic: every knob is MAGPIE_*."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
# vxtwitter's Cloudflare rules 403 browser-like UAs that lack a browser
# fingerprint; a plain client UA is what actually gets through.
PLAIN_UA = "magpie/2.0 (+https://github.com/self-hosted/magpie)"

DEFAULT_SOURCE_UA = {
    "syndication": BROWSER_UA,
    "fxtwitter": PLAIN_UA,
    "vxtwitter": PLAIN_UA,
    "x_page": BROWSER_UA,
}


def _b(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _i(key: str, default: int) -> int:
    try:
        return int(os.environ[key])
    except (KeyError, ValueError):
        return default


def _f(key: str, default: float) -> float:
    try:
        return float(os.environ[key])
    except (KeyError, ValueError):
        return default


def _s(key: str, default: str | None = None) -> str | None:
    val = os.environ.get(key)
    return val if val not in (None, "") else default


def _list(key: str, default: list[str]) -> list[str]:
    raw = os.environ.get(key)
    if not raw:
        return list(default)
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class Settings:
    # --- storage ---
    data_dir: Path = field(default_factory=lambda: Path(_s("MAGPIE_DATA_DIR", "./data")).expanduser())

    # --- server ---
    host: str = field(default_factory=lambda: _s("MAGPIE_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: _i("MAGPIE_PORT", 8099))
    base_path: str = field(default_factory=lambda: (_s("MAGPIE_BASE_PATH", "") or "").rstrip("/"))
    site_name: str = field(default_factory=lambda: _s("MAGPIE_SITE_NAME", "Magpie"))

    # --- auth ---
    # auth_token gates every mutating route (capture, meta, delete) and the
    # whole /api/v1 surface, which refuses to serve at all without one.
    # public_read=False additionally gates the HTML reads.
    auth_token: str | None = field(default_factory=lambda: _s("MAGPIE_AUTH_TOKEN"))
    public_read: bool = field(default_factory=lambda: _b("MAGPIE_PUBLIC_READ", True))
    trust_proxy: bool = field(default_factory=lambda: _b("MAGPIE_TRUST_PROXY", False))

    # --- fetching ---
    sources: list[str] = field(
        default_factory=lambda: _list(
            "MAGPIE_SOURCES", ["syndication", "fxtwitter", "vxtwitter", "x_page"]
        )
    )
    user_agents: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_SOURCE_UA))
    http_timeout: float = field(default_factory=lambda: _f("MAGPIE_HTTP_TIMEOUT", 20.0))
    http_retries: int = field(default_factory=lambda: _i("MAGPIE_HTTP_RETRIES", 2))
    proxy: str | None = field(default_factory=lambda: _s("MAGPIE_PROXY"))
    max_batch: int = field(default_factory=lambda: _i("MAGPIE_MAX_BATCH", 25))
    concurrency: int = field(default_factory=lambda: _i("MAGPIE_CONCURRENCY", 4))
    capture_tls_info: bool = field(default_factory=lambda: _b("MAGPIE_CAPTURE_TLS", True))

    # --- extraction (magpie pull / watch / thread) ---
    # Hydration deliberately drops x_page: it costs ~215 KB per post and adds
    # nothing the JSON sources do not already carry. syndication+fxtwitter is
    # ~9 KB per post, a ~24x saving on a crawl.
    pull_sources: list[str] = field(
        default_factory=lambda: _list("MAGPIE_PULL_SOURCES", ["syndication", "fxtwitter"])
    )
    pull_concurrency: int = field(default_factory=lambda: _i("MAGPIE_PULL_CONCURRENCY", 8))
    pull_delay: float = field(default_factory=lambda: _f("MAGPIE_PULL_DELAY", 0.0))
    watch_interval: float = field(default_factory=lambda: _f("MAGPIE_WATCH_INTERVAL", 300.0))
    thread_depth: int = field(default_factory=lambda: _i("MAGPIE_THREAD_DEPTH", 2))
    thread_max_posts: int = field(default_factory=lambda: _i("MAGPIE_THREAD_MAX_POSTS", 200))
    discover_timeline: bool = field(default_factory=lambda: _b("MAGPIE_DISCOVER_TIMELINE", False))

    # --- guest GraphQL API (no account, anonymous guest token) ---
    # Measured: one UserTweets call returns ~136 tweet ids for an account whose
    # SSR profile page exposes 5. Search is NOT reachable this way.
    xapi: bool = field(default_factory=lambda: _b("MAGPIE_XAPI", True))
    guest_token_ttl: float = field(default_factory=lambda: _f("MAGPIE_GUEST_TOKEN_TTL", 900.0))
    xapi_page_size: int = field(default_factory=lambda: _i("MAGPIE_XAPI_PAGE_SIZE", 40))
    xapi_max_pages: int = field(default_factory=lambda: _i("MAGPIE_XAPI_MAX_PAGES", 5))

    # --- authenticated collection (account cookies; ToS-breaking by design) ---
    # Credentials live in a 0600 file, never in env: an auth_token is a full
    # session (post/DM/delete), unlike MAGPIE_AUTH_TOKEN which is just this app's
    # API key. Search is the only capability that requires it -- measured:
    # SearchTimeline/TweetDetail/Followers all 404 under a guest token even
    # with current query ids, while UserTweets returns 200.
    auth_collection: bool = field(default_factory=lambda: _b("MAGPIE_AUTH_COLLECTION", True))
    accounts_file: str | None = field(default_factory=lambda: _s("MAGPIE_ACCOUNTS_FILE"))
    query_id_ttl: float = field(default_factory=lambda: _f("MAGPIE_QUERY_ID_TTL", 21600.0))
    search_product: str = field(default_factory=lambda: _s("MAGPIE_SEARCH_PRODUCT", "Latest"))
    search_limit: int = field(default_factory=lambda: _i("MAGPIE_SEARCH_LIMIT", 100))

    # --- media ---
    download_media: bool = field(default_factory=lambda: _b("MAGPIE_DOWNLOAD_MEDIA", True))
    max_media_bytes: int = field(default_factory=lambda: _i("MAGPIE_MAX_MEDIA_BYTES", 128 * 1024 * 1024))
    ocr: bool = field(default_factory=lambda: _b("MAGPIE_OCR", True))  # skipped if tesseract absent

    # --- render ---
    render: bool = field(default_factory=lambda: _b("MAGPIE_RENDER", True))
    render_pdf: bool = field(default_factory=lambda: _b("MAGPIE_RENDER_PDF", True))
    render_png: bool = field(default_factory=lambda: _b("MAGPIE_RENDER_PNG", True))
    chromium_path: str | None = field(default_factory=lambda: _s("MAGPIE_CHROMIUM_PATH"))
    render_timeout: float = field(default_factory=lambda: _f("MAGPIE_RENDER_TIMEOUT", 60.0))
    inline_image_bytes: int = field(default_factory=lambda: _i("MAGPIE_INLINE_IMAGE_BYTES", 2_000_000))

    # --- evidence ---
    # RFC 3161 anchoring: the one thing that turns a self-computed hash into
    # something a third party attests to. Off by default (network dependency).
    tsa: bool = field(default_factory=lambda: _b("MAGPIE_TSA", False))
    tsa_url: str = field(default_factory=lambda: _s("MAGPIE_TSA_URL", "https://freetsa.org/tsr"))
    tsa_timeout: float = field(default_factory=lambda: _f("MAGPIE_TSA_TIMEOUT", 20.0))
    operator: str | None = field(default_factory=lambda: _s("MAGPIE_OPERATOR"))

    # --- HTTP API ---
    # Job concurrency is 2, not `concurrency`: API-triggered collection shares
    # X's rate limits with the monitor daemon, and a stampede gets both
    # throttled. Jobs live in the web process only; a restart forgets them,
    # which is correct - the data they produce is already in sqlite.
    api_job_concurrency: int = field(default_factory=lambda: _i("MAGPIE_API_JOB_CONCURRENCY", 2))
    api_job_ttl: float = field(default_factory=lambda: _f("MAGPIE_API_JOB_TTL", 3600.0))
    api_job_max: int = field(default_factory=lambda: _i("MAGPIE_API_JOB_MAX", 200))
    api_docs: bool = field(default_factory=lambda: _b("MAGPIE_API_DOCS", True))
    # /api/v1/query runs caller SQL. `query_only` stops writes but not work: a
    # recursive CTE runs forever and a self-join materialises arbitrarily many
    # rows, so the statement needs a deadline and the result set needs a cap.
    api_query_timeout: float = field(default_factory=lambda: _f("MAGPIE_API_QUERY_TIMEOUT", 5.0))
    api_query_rows: int = field(default_factory=lambda: _i("MAGPIE_API_QUERY_ROWS", 5000))

    def __post_init__(self) -> None:
        # Absolute: package paths become file:// URIs for the renderer, and a
        # relative path cannot be expressed as one.
        self.data_dir = Path(self.data_dir).expanduser().resolve()
        for name in list(self.user_agents):
            override = os.environ.get(f"MAGPIE_UA_{name.upper()}")
            if override:
                self.user_agents[name] = override
        generic = os.environ.get("MAGPIE_USER_AGENT")
        if generic:
            self.user_agents = {k: generic for k in self.user_agents}

    @property
    def accounts_path(self) -> Path:
        """0600 credential file. Kept out of env so it never lands in `docker inspect`."""
        if self.accounts_file:
            return Path(self.accounts_file).expanduser()
        return self.data_dir / "accounts.json"

    @property
    def captures_dir(self) -> Path:
        return self.data_dir / "captures"

    @property
    def index_db(self) -> Path:
        return self.data_dir / "index.db"

    def url_for(self, path: str) -> str:
        return f"{self.base_path}{path}"

    def ua(self, source: str) -> str:
        return self.user_agents.get(source, PLAIN_UA)

    def ensure_dirs(self) -> None:
        self.captures_dir.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    return Settings()
