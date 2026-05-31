from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import AsyncIterator

class BaseSource(ABC):
    name: str
    enabled: bool = True

    @abstractmethod
    async def fetch_raw(self) -> AsyncIterator[str]:
        ...

    async def fetch_all(self) -> str:
        chunks = []
        async for chunk in self.fetch_raw():
            if chunk:
                chunks.append(chunk)
        return "\n".join(chunks)

async def sleep_for_retry_after(value: str | None, default: float = 1.0) -> None:
    if value is None:
        await asyncio.sleep(default)
        return
    try:
        delay = max(float(value), 0.0)
        await asyncio.sleep(delay)
    except ValueError:
        await asyncio.sleep(default)
