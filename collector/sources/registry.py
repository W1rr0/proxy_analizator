from pathlib import Path
from typing import Any
import yaml
from loguru import logger
from collector.config import Settings
from collector.sources.base import BaseSource
from collector.sources.github import GithubSource
from collector.sources.telegram import TelegramSource

class SourceLoader:
    @classmethod
    def load(cls, sources_file: str, settings: Settings | None = None) -> list[BaseSource]:
        active = settings or Settings()
        path = Path(sources_file)
        if not path.exists():
            logger.warning("Sources file not found: {}", path)
            return []
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        entries = data.get("sources") or []
        sources: list[BaseSource] = []
        for entry in entries:
            try:
                sources.append(cls._from_entry(entry, active))
            except Exception as exc:
                logger.warning("Skipping invalid source {}: {}", entry, exc)
        logger.info("Loaded {} sources ({} enabled)", len(sources), sum(1 for s in sources if s.enabled))
        return sources

    @staticmethod
    def _from_entry(entry: dict[str, Any], settings: Settings) -> BaseSource:
        source_type = str(entry.get("type") or "").lower()
        enabled = bool(entry.get("enabled", True))

        if source_type == "github":
            return GithubSource(
                repo=entry.get("repo"),
                url=entry.get("url") or entry.get("raw_url"),
                scan_mode=str(entry.get("scan_mode", "tree")),
                path=str(entry.get("path") or ""),
                patterns=entry.get("patterns"),
                token=settings.github_token,
                enabled=enabled,
                request_timeout=settings.request_timeout,
                user_agent=settings.user_agent,
                max_file_fetches=settings.max_concurrent_fetches,
            )

        if source_type == "telegram":
            return TelegramSource(
                str(entry["channel"]),
                max_pages=int(entry.get("max_pages", 5)),
                enabled=enabled,
                request_timeout=settings.request_timeout,
                user_agent=settings.user_agent,
            )

        raise ValueError(f"Unsupported source type: {source_type!r}")
