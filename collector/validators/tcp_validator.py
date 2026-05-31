"""
TCP/TLS Validator — проверяет доступность всех серверов
"""

from __future__ import annotations

import asyncio
import time
import platform
from dataclasses import dataclass, field
from urllib.parse import urlparse

from loguru import logger


async def _try_tcp(host: str, port: int, timeout: float) -> bool:
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def check_tcp(host: str, port: int, timeout: float = 3.0) -> bool:
    return await _try_tcp(host, port, timeout)


@dataclass
class _Stats:
    alive: int = 0
    dead: int = 0
    start: float = field(default_factory=time.monotonic)

    @property
    def total(self) -> int:
        return self.alive + self.dead

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

    @property
    def rps(self) -> float:
        return self.total / self.elapsed if self.elapsed > 0 else 0


async def validate_all(
    uris: list[str], *,
    timeout: float = 3.0,
    max_concurrent: int = 1000,
    log_every: int = 10000,
) -> list[str]:
    """
    Проверяет TCP-доступность списка URI.
    Использует очередь + воркеры и общий таймаут.
    """
    if not uris:
        return []

    total = len(uris)
    stats = _Stats()
    passed: list[tuple[int, str]] = []
    stats_lock = asyncio.Lock()
    passed_lock = asyncio.Lock()

    multiplier = 4.0 if platform.machine().startswith(("arm", "aarch")) else 3.0
    total_timeout = max(600, (total / max(max_concurrent, 1)) * timeout * multiplier)

    logger.info(
        "TCP validation: {} proxies | timeout={:.1f}s | concurrency={} | total_timeout={:.0f}s",
        total, timeout, max_concurrent, total_timeout,
    )

    queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue()

    for i, uri in enumerate(uris):
        await queue.put((i, uri))
    for _ in range(max_concurrent):
        await queue.put(None)

    async def worker() -> None:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            idx, uri = item
            try:
                parsed = urlparse(uri)
                host = parsed.hostname
                port = parsed.port
                if host and port:
                    if await check_tcp(host, port, timeout):
                        async with stats_lock:
                            stats.alive += 1
                        async with passed_lock:
                            passed.append((idx, uri))
                    else:
                        async with stats_lock:
                            stats.dead += 1
                else:
                    async with stats_lock:
                        stats.dead += 1
            except Exception as exc:
                logger.debug("Validation error for {}: {}", uri, exc)
                async with stats_lock:
                    stats.dead += 1

            async with stats_lock:
                done = stats.total
                if done % log_every == 0 or done == total:
                    logger.info(
                        "  [{:5.1f}%] {}/{} | alive: {} dead: {} | {:.0f} req/s",
                        done / total * 100, done, total,
                        stats.alive, stats.dead, stats.rps,
                    )
            queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(max_concurrent)]

    try:
        await asyncio.wait_for(queue.join(), timeout=total_timeout)
    except asyncio.TimeoutError:
        logger.warning(
            "Validation timed out after {:.0f}s — cancelling remaining workers",
            total_timeout,
        )
        for w in workers:
            w.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*workers, return_exceptions=True), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("Some workers did not cancel in time")
    else:
        await asyncio.gather(*workers, return_exceptions=True)

    passed.sort(key=lambda x: x[0])
    logger.info(
        "Done in {:.1f}s — alive: {} ({:.1f}%) | dead: {}",
        stats.elapsed, stats.alive,
        stats.alive / total * 100 if total else 0, stats.dead,
    )
    return [uri for _, uri in passed]
