"""
GitHub Source — с поддержкой зеркал
"""

from __future__ import annotations

import asyncio
import base64
import fnmatch
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import AsyncIterator
from urllib.parse import urlparse

import httpx
from loguru import logger

from collector.sources.base import BaseSource, sleep_for_retry_after

_API_BASE = "https://api.github.com"
_RAW_BASE = "https://raw.githubusercontent.com"
_DEFAULT_PATTERNS = ["*.txt", "*.yaml", "*.yml", "*.conf", "*.json", "*.list", "sub", "config", "configs", "mixed"]
_BINARY_EXT = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".svg", ".zip", ".tar", ".gz", ".bz2", ".xz", ".7z", ".rar", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".mp3", ".mp4", ".avi", ".mkv", ".pyc", ".pyo", ".so", ".dll", ".exe", ".whl", ".egg", ".db", ".sqlite"})
_RATE_WARN = 50
_URI_RE = re.compile(r"(?:vless|vmess|ss|trojan|hysteria2|hy2|tuic|wireguard)://[^\s"'<>`\[\]]+", re.IGNORECASE)

class _MirrorType(Enum):
    DOMAIN = "domain"
    PROXY  = "proxy"

@dataclass
class _Mirror:
    name: str
    host: str
    kind: _MirrorType
    failures: int = 0
    cooldown: float = 0.0

    def is_available(self) -> bool:
        return not self.cooldown or time.monotonic() >= self.cooldown

    def mark_failure(self) -> None:
        self.failures += 1
        wait = min(30 * (2 ** (self.failures - 1)), 600)
        self.cooldown = time.monotonic() + wait

    def mark_success(self) -> None:
        self.failures = max(0, self.failures - 1)
        self.cooldown = 0.0

    def build_url(self, raw_url: str) -> str:
        if self.kind == _MirrorType.PROXY:
            return f"https://{self.host}/{raw_url}"
        path = raw_url.removeprefix(_RAW_BASE).lstrip("/")
        parts = path.split("/", 3)
        if len(parts) >= 3:
            user, repo, branch = parts[0], parts[1], parts[2]
            rest = parts[3] if len(parts) == 4 else ""
            return f"https://{self.host}/{user}/{repo}/raw/{branch}/{rest}"
        return f"https://{self.host}/{path}"

_MIRRORS = [
    _Mirror("hub.yzuu.cf",    "hub.yzuu.cf",    _MirrorType.DOMAIN),
    _Mirror("hub.nuaa.cf",    "hub.nuaa.cf",    _MirrorType.DOMAIN),
    _Mirror("hub.scholar.ht", "hub.scholar.ht", _MirrorType.DOMAIN),
    _Mirror("ghproxy.com",    "ghproxy.com",    _MirrorType.PROXY),
    _Mirror("ghps.cc",        "ghps.cc",        _MirrorType.DOMAIN),
    _Mirror("gh.ddlc.top",    "gh.ddlc.top",    _MirrorType.DOMAIN),
    _Mirror("gh.wget.cool",   "gh.wget.cool",   _MirrorType.DOMAIN),
]

class MirrorManager:
    def __init__(self, mirrors: list[_Mirror] = _MIRRORS) -> None:
        self._mirrors = list(mirrors)

    def available(self) -> list[_Mirror]:
        return [m for m in self._mirrors if m.is_available()]

    async def fetch(self, client: httpx.AsyncClient, raw_url: str, source_name: str = "") -> str | None:
        text = await _get_text(client, raw_url, source_name=source_name)
        if text is not None:
            return text
        for mirror in self.available():
            mirror_url = mirror.build_url(raw_url)
            logger.debug("[{}] trying mirror {}: {}", source_name, mirror.name, mirror_url)
            text = await _get_text(client, mirror_url, source_name=source_name)
            if text is not None:
                mirror.mark_success()
                return text
            mirror.mark_failure()
        logger.warning("[{}] all mirrors failed for {}", source_name, raw_url.split("/")[-1])
        return None

@dataclass
class _RateLimitState:
    remaining: int = 5000
    reset_at:  float = 0.0
    warned:    bool = False

    def update(self, headers: httpx.Headers) -> None:
        if r := headers.get("X-RateLimit-Remaining"): self.remaining = int(r) if r.isdigit() else self.remaining
        if r := headers.get("X-RateLimit-Reset"): self.reset_at = float(r) if r.isdigit() else self.reset_at

    async def wait_if_exhausted(self) -> None:
        if self.remaining > 0: return
        wait = max(self.reset_at - time.monotonic(), 1.0)
        logger.warning("GitHub API exhausted — waiting {:.0f}s for reset", wait)
        await asyncio.sleep(wait)

    def warn_if_low(self, name: str) -> None:
        if not self.warned and self.remaining < _RATE_WARN:
            logger.warning("[{}] GitHub API low: {} requests left. Add GITHUB_TOKEN.", name, self.remaining)
            self.warned = True

def _api_headers(token: str | None, user_agent: str) -> dict[str, str]:
    h = {"User-Agent": user_agent, "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if token: h["Authorization"] = f"Bearer {token}"
    return h

async def _api_get(client: httpx.AsyncClient, url: str, rate_state: _RateLimitState, name: str, attempts: int = 4) -> httpx.Response | None:
    for attempt in range(attempts):
        try:
            resp = await client.get(url)
            rate_state.update(resp.headers)
            rate_state.warn_if_low(name)
            if resp.status_code == 429:
                await asyncio.sleep(float(resp.headers.get("Retry-After") or 2 ** attempt))
                continue
            if resp.status_code == 403 and (reset := resp.headers.get("X-RateLimit-Reset")) and reset.isdigit():
                wait = max(int(reset) - time.time(), 1.0)
                logger.warning("[{}] rate limit 403 — waiting {:.0f}s", name, wait)
                await asyncio.sleep(min(wait, 300))
                continue
            if resp.status_code >= 500:
                await asyncio.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp
        except (httpx.TimeoutException, httpx.TransportError):
            if attempt < attempts - 1: await asyncio.sleep(2 ** attempt)
        except httpx.HTTPStatusError as e:
            logger.debug("[{}] API HTTP {}: {}", name, e.response.status_code, url)
            return None
    return None

async def _get_text(client: httpx.AsyncClient, url: str, source_name: str = "", attempts: int = 2) -> str | None:
    for attempt in range(attempts):
        try:
            resp = await client.get(url)
            if resp.status_code == 200: return resp.text
            if resp.status_code in (429, 503):
                await sleep_for_retry_after(resp.headers.get("Retry-After"), default=2 ** attempt)
                continue
            return None
        except (httpx.TimeoutException, httpx.TransportError):
            if attempt < attempts - 1: await asyncio.sleep(1.5)
    return None

class ScanMode(str, Enum):
    TREE = "tree"
    PATH = "path"
    RAW  = "raw"

def _parse_owner_repo(repo: str) -> tuple[str, str]:
    repo = repo.strip().rstrip("/")
    if repo.startswith("http"):
        parts = urlparse(repo).path.strip("/").split("/")
        if len(parts) >= 2: return parts[0], parts[1].removesuffix(".git")
        raise ValueError(f"Cannot parse repo URL: {repo}")
    parts = repo.split("/")
    if len(parts) == 2: return parts[0], parts[1].removesuffix(".git")
    raise ValueError(f"Invalid repo format: {repo!r}")

def _blob_to_raw(url: str) -> str:
    if _RAW_BASE in url or "gist.githubusercontent.com" in url: return url
    if "github.com" in url and "/blob/" in url: return url.replace("https://github.com/", f"{_RAW_BASE}/").replace("/blob/", "/")
    return url

def _raw_url(owner: str, repo: str, path: str, branch: str = "main") -> str:
    """Использует конкретную ветку вместо HEAD."""
    return f"{_RAW_BASE}/{owner}/{repo}/{branch}/{path}"

def _is_binary(path: str) -> bool:
    return PurePosixPath(path).suffix.lower() in _BINARY_EXT

def _matches(name: str, patterns: list[str]) -> bool:
    nl = name.lower()
    return any(fnmatch.fnmatch(nl, p.lower()) for p in patterns)

def _try_decode_b64(text: str) -> str | None:
    compact = "".join(text.split())
    if len(compact) < 32 or not re.fullmatch(r"[A-Za-z0-9_+/=\-]+", compact): return None
    padded = compact + "=" * (-len(compact) % 4)
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            decoded = decoder(padded.encode()).decode("utf-8", errors="strict")
            if "://" in decoded: return decoded
        except Exception: continue
    return None

def _enrich(text: str) -> str:
    decoded = _try_decode_b64(text)
    return f"{text}\n{decoded}" if decoded else text

class GithubSource(BaseSource):
    def __init__(
        self, *, repo: str | None = None, url: str | None = None,
        scan_mode: str = ScanMode.TREE, path: str = "",
        patterns: list[str] | None = None, token: str | None = None,
        enabled: bool = True, request_timeout: float = 20.0,
        user_agent: str = "Mozilla/5.0 (compatible; ConfigCollector/2.0)",
        max_file_fetches: int = 30, mirrors: list[_Mirror] = _MIRRORS,
        branch: str = "main",
    ) -> None:
        if not repo and not url: raise ValueError("GithubSource requires 'repo' or 'url'")
        self.raw_url = _blob_to_raw(url) if url else None
        self.scan_mode = ScanMode(scan_mode)
        self.path = path.strip("/")
        self.patterns = patterns or _DEFAULT_PATTERNS
        self.token = token
        self.enabled = enabled
        self.timeout = request_timeout
        self.user_agent = user_agent
        self.max_file_fetches = max_file_fetches
        self.mirror_manager = MirrorManager(mirrors)
        self.branch = branch

        if repo:
            self.owner, self.repo_name = _parse_owner_repo(repo)
            self.name = f"github:{self.owner}/{self.repo_name}"
        else:
            self.owner = self.repo_name = ""
            self.name = f"github:raw:{self.raw_url}"

        if self.raw_url and not repo: self.scan_mode = ScanMode.RAW

    async def fetch_raw(self) -> AsyncIterator[str]:
        rate = _RateLimitState()
        api_client = httpx.AsyncClient(http2=True, follow_redirects=True, max_redirects=5, timeout=self.timeout, headers=_api_headers(self.token, self.user_agent))
        raw_client = httpx.AsyncClient(http2=True, follow_redirects=True, max_redirects=5, timeout=self.timeout, headers={"User-Agent": self.user_agent})

        async with api_client, raw_client:
            if self.scan_mode == ScanMode.RAW:
                text = await self.mirror_manager.fetch(raw_client, self.raw_url, self.name)
                if text: yield _enrich(text)
            elif self.scan_mode == ScanMode.TREE:
                urls = await self._tree_urls(api_client, rate)
                async for chunk in self._fetch_all(raw_client, urls): yield chunk
            elif self.scan_mode == ScanMode.PATH:
                urls = await self._contents_urls(api_client, self.path, rate)
                async for chunk in self._fetch_all(raw_client, urls): yield chunk

    async def _tree_urls(self, client: httpx.AsyncClient, rate: _RateLimitState) -> list[str]:
        url = f"{_API_BASE}/repos/{self.owner}/{self.repo_name}/git/trees/{self.branch}?recursive=1"
        resp = await _api_get(client, url, rate, self.name)
        if not resp: return []
        try: tree = resp.json().get("tree", [])
        except Exception: return []
        result, skipped = [], 0
        blobs = [i for i in tree if i.get("type") == "blob"]
        for item in blobs:
            p = str(item.get("path") or "")
            if self.path and not p.startswith(self.path + "/"): skipped += 1; continue
            if _is_binary(p): skipped += 1; continue
            if not _matches(PurePosixPath(p).name, self.patterns): skipped += 1; continue
            result.append(_raw_url(self.owner, self.repo_name, p, self.branch))
        logger.info("[{}] tree: {}/{} files match ({} skipped)", self.name, len(result), len(blobs), skipped)
        return result

    async def _contents_urls(self, client: httpx.AsyncClient, path: str, rate: _RateLimitState) -> list[str]:
        url = f"{_API_BASE}/repos/{self.owner}/{self.repo_name}/contents/{path}"
        resp = await _api_get(client, url, rate, self.name)
        if not resp: return []
        try: payload = resp.json()
        except Exception: return []
        items = payload if isinstance(payload, list) else [payload]
        result, subtasks = [], []
        for item in items:
            kind = item.get("type")
            name = str(item.get("name") or "")
            ipath = str(item.get("path") or "")
            if kind == "dir":
                subtasks.append(asyncio.create_task(self._contents_urls(client, ipath, rate)))
            elif kind == "file":
                if _is_binary(name) or not _matches(name, self.patterns): continue
                dl = item.get("download_url")
                if dl: result.append(str(dl))
                else: result.append(_raw_url(self.owner, self.repo_name, ipath, self.branch))
        if subtasks:
            for sub in await asyncio.gather(*subtasks, return_exceptions=True):
                if isinstance(sub, list): result.extend(sub)
        return result

    async def _fetch_all(self, client: httpx.AsyncClient, urls: list[str]) -> AsyncIterator[str]:
        if not urls: return
        sem = asyncio.Semaphore(self.max_file_fetches)
        queue: asyncio.Queue[str | None] = asyncio.Queue()
        total = len(urls)
        done_count = 0
        done_lock = asyncio.Lock()
        errors: list[Exception] = []

        async def worker(url: str):
            nonlocal done_count
            async with sem:
                try:
                    text = await self.mirror_manager.fetch(client, url, self.name)
                    if text:
                        enriched = _enrich(text)
                        if _URI_RE.search(enriched): 
                            await queue.put(enriched)
                except Exception as exc:
                    errors.append(exc)
                    logger.debug("[{}] Worker error: {}", self.name, exc)
                finally:
                    async with done_lock:
                        done_count += 1
                        if done_count % 50 == 0 or done_count == total:
                            logger.debug("[{}] {}/{} files done", self.name, done_count, total)

        tasks = [asyncio.create_task(worker(u)) for u in urls]
        finished, remaining = set(), len(tasks)

        while remaining > 0 or not queue.empty():
            while not queue.empty(): yield queue.get_nowait()
            newly = {t for t in tasks if t not in finished and t.done()}
            for t in newly: 
                finished.add(t)
                remaining -= 1
                if t.exception():
                    logger.debug("[{}] Task exception: {}", self.name, t.exception())
            if remaining > 0: await asyncio.sleep(0.05)

        while not queue.empty(): yield queue.get_nowait()

        if errors:
            logger.warning("[{}] {} errors during fetch", self.name, len(errors))
        logger.info("[{}] fetched {}/{} files", self.name, len(finished), total)
