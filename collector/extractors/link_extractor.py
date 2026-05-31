"""
Link Extractor — находит и скачивает вложенные подписки
"""

from __future__ import annotations

import asyncio
import re
from urllib.parse import urlparse

import httpx
from loguru import logger

from collector.exporters.subscription import extract_uris
from collector.parsers.utils import decode_base64_text

_SKIP_DOMAINS = frozenset({
    "github.com", "raw.githubusercontent.com", "gist.githubusercontent.com",
    "api.github.com", "t.me", "telegram.me", "telegram.org",
})
_SKIP_EXTENSIONS = frozenset({
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".svg",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mkv",
    ".zip", ".tar", ".gz", ".rar",
    ".pdf", ".doc", ".docx",
    ".py", ".go", ".rs", ".java", ".cpp", ".h",
    ".md", ".html", ".xml", ".toml",
})
_SUB_PATH_RE = re.compile(
    r"(?:/sub|/config|/configs|/subscription|/clash|/v2ray"
    r"|/api/v\d|/link|/dl|/download|/proxy|/node|/free"
    r"|/whitelist|/wl|/mega|/gen|/mixed|/all)", re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+", re.IGNORECASE)

MAX_DISCOVERED = 200
MIN_URIS_TO_KEEP = 1

def _should_skip(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        path = parsed.path or ""
        if any(host == d or host.endswith("." + d) for d in _SKIP_DOMAINS): return True
        ext = "." + path.rsplit(".", 1)[-1].lower() if "." in path else ""
        if ext in _SKIP_EXTENSIONS: return True
        return False
    except Exception: return True

def _is_likely_subscription(url: str) -> bool:
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        qs = parsed.query or ""
        return bool(_SUB_PATH_RE.search(path) or _SUB_PATH_RE.search(qs))
    except Exception: return False

def find_candidate_urls(text: str, known_urls: set[str]) -> list[str]:
    found = _URL_RE.findall(text)
    seen: set[str] = set()
    likely: list[str] = []
    others: list[str] = []
    for url in found:
        url = url.rstrip(".,;)]}\"'>")
        if url in seen or url in known_urls: continue
        if _should_skip(url): continue
        seen.add(url)
        if _is_likely_subscription(url): likely.append(url)
        else: others.append(url)
    candidates = likely + others
    return candidates[:MAX_DISCOVERED]

def _try_decode(text: str) -> str:
    compact = "".join(text.split())
    if len(compact) < 32 or not re.fullmatch(r"[A-Za-z0-9_+/=\-]+", compact): return text
    try:
        decoded = decode_base64_text(compact)
        if "://" in decoded: return decoded
    except Exception: pass
    return text

async def _fetch_one(client: httpx.AsyncClient, url: str, timeout: float) -> str | None:
    try:
        resp = await client.get(url, timeout=timeout, follow_redirects=True)
        if resp.status_code == 200: return _try_decode(resp.text)
        return None
    except Exception: return None

async def extract_nested_subscriptions(
    texts: list[str], known_urls: set[str], *,
    concurrency: int = 20,
    timeout: float = 15.0,
    user_agent: str = "Mozilla/5.0 (compatible; ConfigCollector/2.0)",
) -> list[str]:
    all_candidates: list[str] = []
    combined_known = set(known_urls)
    for text in texts:
        urls = find_candidate_urls(text, combined_known)
        for url in urls:
            combined_known.add(url)
            all_candidates.append(url)
    if not all_candidates:
        logger.debug("LinkExtractor: no candidate URLs found")
        return []
    logger.info("LinkExtractor: found {} candidate URLs to fetch", len(all_candidates))
    sem = asyncio.Semaphore(concurrency)
    headers = {"User-Agent": user_agent}
    async with httpx.AsyncClient(headers=headers, http2=True, follow_redirects=True, max_redirects=5) as client:
        async def bounded(url: str) -> tuple[str, str | None]:
            async with sem:
                return url, await _fetch_one(client, url, timeout)
        results = await asyncio.gather(*[bounded(url) for url in all_candidates], return_exceptions=True)
    new_texts: list[str] = []
    fetched = 0
    for result in results:
        if isinstance(result, Exception): continue
        url, text = result
        if not text: continue
        fetched += 1
        uris = extract_uris(text)
        if len(uris) >= MIN_URIS_TO_KEEP:
            new_texts.append(text)
            logger.debug("LinkExtractor: +{} URIs from {}", len(uris), url)
    logger.info("LinkExtractor: {}/{} URLs yielded proxy configs", len(new_texts), fetched)
    return new_texts
