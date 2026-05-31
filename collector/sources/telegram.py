"""
Telegram Source — t.me/s/<channel>
"""

from __future__ import annotations

import asyncio
import re
from typing import AsyncIterator
from urllib.parse import parse_qs, urlparse

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from collector.sources.base import BaseSource, sleep_for_retry_after

_HREF_RE = re.compile(r"^https?://", re.IGNORECASE)

try:
    from lxml import etree
    DEFAULT_PARSER = "lxml"
except ImportError:
    DEFAULT_PARSER = "html.parser"
    logger.warning("lxml not available, using html.parser (slower)")

class TelegramSource(BaseSource):
    def __init__(
        self,
        channel: str,
        *,
        max_pages: int = 5,
        enabled: bool = True,
        request_timeout: float = 15.0,
        user_agent: str = "Mozilla/5.0 (compatible; ConfigCollector/2.0)",
    ) -> None:
        self.channel = channel.lstrip("@")
        self.max_pages = max_pages
        self.enabled = enabled
        self.request_timeout = request_timeout
        self.user_agent = user_agent
        self.name = f"telegram:{self.channel}"
        self.parser = DEFAULT_PARSER

    async def fetch_raw(self) -> AsyncIterator[str]:
        headers = {"User-Agent": self.user_agent}
        before: str | None = None
        async with httpx.AsyncClient(
            http2=True, follow_redirects=True, max_redirects=5,
            headers=headers, timeout=self.request_timeout,
        ) as client:
            for _ in range(self.max_pages):
                url = f"https://t.me/s/{self.channel}"
                if before: url = f"{url}?before={before}"
                try:
                    html = await self._get_text(client, url)
                except RuntimeError:
                    if before: break
                    raise
                text, before = self._parse_page(html)
                if text: yield text
                if not before: break

    async def _get_text(self, client: httpx.AsyncClient, url: str) -> str:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                resp = await client.get(url)
                if resp.status_code == 429:
                    await sleep_for_retry_after(resp.headers.get("Retry-After"), default=2**attempt)
                    continue
                resp.raise_for_status()
                return resp.text
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt == 2: break
                await asyncio.sleep(2**attempt)
        raise RuntimeError(f"Failed to fetch {url}: {self._fmt(last_error)}")

    def _parse_page(self, html: str) -> tuple[str, str | None]:
        soup = BeautifulSoup(html, self.parser)
        parts: list[str] = []
        for msg in soup.select(".tgme_widget_message_text"):
            text = msg.get_text("\n", strip=True)
            if text: parts.append(text)
            for a in msg.select("a[href]"):
                href = str(a.get("href", "")).strip()
                if not href or not _HREF_RE.match(href): continue
                try:
                    domain = urlparse(href).hostname or ""
                    if any(domain == d or domain.endswith("." + d) for d in ("t.me", "telegram.me", "telegram.org")):
                        continue
                except Exception: continue
                parts.append(href)

        next_before = None
        more = soup.select_one("a.tme_messages_more[href]") or soup.select_one("a.js-messages_more[href]")
        if more:
            href = str(more.get("href", ""))
            query = parse_qs(urlparse(href).query)
            vals = query.get("before")
            if vals: next_before = vals[0]
        return "\n".join(parts), next_before

    @staticmethod
    def _fmt(error: Exception | None) -> str:
        if error is None: return "unknown error"
        return f"{type(error).__name__}: {str(error).strip() or repr(error)}"
