#!/usr/bin/env python3
"""
main.py
────────
Флаги запуска:
  python main.py                  — полный цикл (сбор + экспорт + валидация)
  python main.py --collect-only   — только сбор и экспорт
  python main.py --validate-only  — только TCP-проверка уже собранных конфигов
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from loguru import logger

from collector.config import Settings
from collector.exporters.subscription import SubscriptionExporter, extract_uris
from collector.extractors.link_extractor import extract_nested_subscriptions
from collector.sources.registry import SourceLoader
from collector.validators.tcp_validator import validate_all


def normalize_uri(uri: str) -> str | None:
    """
    Приводит URI к единообразному виду БЕЗ сортировки query-параметров.
    Сохраняет оригинальный порядок, но нормализует кодирование.
    """
    try:
        parsed = urlparse(uri)
    except ValueError:
        return None
    if not parsed.query:
        return uri
    qs = parse_qs(parsed.query, keep_blank_values=True)
    normalized_qs = urlencode(list(qs.items()), doseq=True)
    normalized = parsed._replace(query=normalized_qs)
    return urlunparse(normalized)


def split_large_files(directory: str = "configs", max_size_mb: int = 85) -> None:
    """
    Разбивает файлы > max_size_mb на части, гарантируя целостность строк.
    Не разрезает URI посередине.
    """
    dir_path = Path(directory)
    max_bytes = max_size_mb * 1024 * 1024

    for file_path in sorted(dir_path.iterdir()):
        if not file_path.is_file():
            continue
        fsize = file_path.stat().st_size
        if fsize <= max_bytes:
            continue

        print(f"Splitting {file_path.name} ({fsize / 1024 / 1024:.1f} MB)…")

        part_num = 1
        current_size = 0
        current_lines: list[bytes] = []

        with file_path.open("rb") as f:
            for line in f:
                line_len = len(line)

                if line_len > max_bytes and not current_lines:
                    part_path = dir_path / f"{file_path.stem}_part{part_num:03d}.txt"
                    part_path.write_bytes(line)
                    part_num += 1
                    continue

                if current_size + line_len > max_bytes and current_lines:
                    part_path = dir_path / f"{file_path.stem}_part{part_num:03d}.txt"
                    part_path.write_bytes(b"".join(current_lines))
                    part_num += 1
                    current_lines = []
                    current_size = 0

                current_lines.append(line)
                current_size += line_len

            if current_lines:
                part_path = dir_path / f"{file_path.stem}_part{part_num:03d}.txt"
                part_path.write_bytes(b"".join(current_lines))
                part_num += 1

        file_path.unlink()
        print(f"  → создано {part_num - 1} частей")


async def collect(settings: Settings) -> list[str]:
    """Шаг 1: Сбор URIs из всех источников."""
    sources = SourceLoader.load(settings.sources_file, settings)
    enabled = [s for s in sources if s.enabled]
    logger.info("Loaded {} sources ({} enabled)", len(sources), len(enabled))

    all_uris: list[str] = []
    all_texts: list[str] = []

    for source in enabled:
        logger.info("[{}] Starting...", source.name)
        try:
            async for chunk in source.fetch_raw():
                uris = extract_uris(chunk)
                all_uris.extend(uris)
                all_texts.append(chunk)
                if uris:
                    logger.info("[{}] +{} URIs", source.name, len(uris))
        except Exception as exc:
            logger.warning("[{}] Error: {}", source.name, exc)

    known_urls: set[str] = set()
    for s in enabled:
        for attr in ("raw_url", "url"):
            if val := getattr(s, attr, None):
                known_urls.add(val)

    nested_texts = await extract_nested_subscriptions(
        all_texts,
        known_urls,
        concurrency=min(settings.max_concurrent_fetches, 20),
        timeout=settings.request_timeout,
        user_agent=settings.user_agent,
    )
    for text in nested_texts:
        all_uris.extend(extract_uris(text))

    logger.info("Total raw URIs: {}", len(all_uris))

    seen_norm = set()
    unique_uris = []
    skipped = 0
    for uri in all_uris:
        norm = normalize_uri(uri)
        if norm is None:
            skipped += 1
            continue
        if norm not in seen_norm:
            seen_norm.add(norm)
            unique_uris.append(uri)

    if skipped:
        logger.warning("Skipped {} invalid URIs (unparseable)", skipped)
    logger.info("Total unique URIs (after dedup): {}", len(unique_uris))
    return unique_uris


def export(uris: list[str], settings: Settings) -> None:
    """Шаг 2: Экспорт в файлы подписок."""
    exporter = SubscriptionExporter(
        output_dir=Path(settings.output_dir),
        encode=True,
        split=True,
        repo_owner=settings.repo_owner or "Darkoflox",
        repo_name=settings.repo_name or "Kfg-analizator",
        repo_branch=settings.repo_branch,
        configs_path="configs",
    )
    results = exporter.export(uris)
    exporter.print_mirror_table(results)

    split_large_files(settings.output_dir, max_size_mb=85)
    logger.info("Export done: {} files in {}/", len(results), settings.output_dir)


async def validate(settings: Settings) -> None:
    """Шаг 3: TCP-проверка с поддержкой разбитых файлов."""
    output_dir = Path(settings.output_dir)
    sub_raw_path = output_dir / "sub_raw.txt"

    if not sub_raw_path.exists():
        parts = sorted(output_dir.glob("sub_raw_part*.txt"))
        if not parts:
            logger.error("sub_raw.txt and its parts not found — run collect first")
            sys.exit(1)
        logger.info("Loading URIs from {} part file(s)", len(parts))
        uris: list[str] = []
        for part in parts:
            text = part.read_text(encoding="utf-8")
            uris.extend(
                line.strip()
                for line in text.splitlines()
                if line.strip() and "://" in line
            )
    else:
        uris = [
            line.strip()
            for line in sub_raw_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and "://" in line
        ]

    logger.info("Loaded {} URIs for validation", len(uris))

    max_concurrent = min(settings.max_concurrent_validations, 200)
    timeout = min(settings.tcp_timeout, 3.0)

    working = await validate_all(
        uris,
        timeout=timeout,
        max_concurrent=max_concurrent,
    )

    checked_path = output_dir / "checked_sub_raw.txt"
    checked_path.write_text("\n".join(working), encoding="utf-8")
    logger.info(
        "Validation done: {}/{} alive → saved to {}",
        len(working), len(uris), checked_path,
    )

    split_large_files(settings.output_dir, max_size_mb=85)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect-only", action="store_true", help="Только сбор и экспорт")
    parser.add_argument("--validate-only", action="store_true", help="Только TCP-валидация")
    args = parser.parse_args()

    settings = Settings()

    if args.validate_only:
        await validate(settings)
        return

    uris = await collect(settings)
    export(uris, settings)
    logger.info("Files saved. Ready to commit.")

    if args.collect_only:
        return

    await validate(settings)


if __name__ == "__main__":
    asyncio.run(main())
