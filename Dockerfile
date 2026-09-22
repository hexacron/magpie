FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# System packages: curl for HEALTHCHECK, tesseract for OCR, plus the base
# libraries Chromium needs at runtime. `playwright install --with-deps`
# below fills in anything else Debian-specific that is still missing.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        tesseract-ocr \
        fonts-liberation \
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libpango-1.0-0 \
        libcairo2 \
        libasound2 \
    && rm -rf /var/lib/apt/lists/*

# --- dependency layer (cached independently of the source tree) ---
# Kept in sync with [project].dependencies + the `all` extra in pyproject.toml.
RUN pip install \
        "httpx>=0.27" \
        "jinja2>=3.1" \
        "fastapi>=0.110" \
        "uvicorn[standard]>=0.29" \
        "python-multipart>=0.0.9" \
        "playwright>=1.44" \
        "pytesseract>=0.3.10" \
        "pillow>=10"

RUN playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*

# --- source layer ---
WORKDIR /app
COPY . /app
RUN pip install --no-deps .

# Non-root runtime. /data is the mount point for capture packages + index.db.
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data \
    && chown -R app:app /data /app \
    && chmod -R a+rX /ms-playwright

VOLUME /data

ENV MAGPIE_DATA_DIR=/data \
    MAGPIE_HOST=0.0.0.0 \
    MAGPIE_PORT=8099

USER app
EXPOSE 8099

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8099/healthz || exit 1

CMD ["magpie", "serve"]
