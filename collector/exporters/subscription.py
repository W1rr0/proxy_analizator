"""
Subscription Exporter — генерирует файлы подписок
"""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

_URI_RE = re.compile(r"((?:vless|vmess|ss|trojan|hysteria2|hy2|tuic|wireguard)://[^\s"'<>`\[\]]+)", re.IGNORECASE)

_MIRROR_DOMAINS = ["raw.githubusercontent.com", "hub.yzuu.cf", "hub.nuaa.cf", "hub.scholar.ht", "ghps.cc", "gh.ddlc.top", "gh.wget.cool"]
_PROXY_MIRROR = "ghproxy.com"
_PROTOCOL_ORDER = ["vless", "vmess", "trojan", "ss", "hysteria2", "tuic", "wireguard"]

@dataclass
class SubscriptionMeta:
    path: Path
    protocol: str
    total: int
    mirror_urls: list[str]
    encoded: bool

def extract_uris(text: str) -> list[str]:
    return _URI_RE.findall(text)

def group_by_protocol(uris: list[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for uri in uris:
        proto = uri.split("://", 1)[0].lower()
        if proto == "hy2":
            proto = "hysteria2"
        groups.setdefault(proto, []).append(uri)
    return groups

def encode_subscription(uris: list[str]) -> str:
    return base64.b64encode("\n".join(uris).encode("utf-8")).decode("ascii")

def decode_subscription(b64_text: str) -> list[str]:
    cleaned = b64_text.strip()
    padded = cleaned + "=" * (-len(cleaned) % 4)
    raw = base64.b64decode(padded).decode("utf-8", errors="ignore")
    return [line.strip() for line in raw.splitlines() if "://" in line]

def build_mirror_urls(owner: str, repo: str, branch: str, rel_path: str) -> list[str]:
    urls = []
    raw_direct = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rel_path}"
    urls.append(raw_direct)
    for domain in _MIRROR_DOMAINS[1:]:
        urls.append(f"https://{domain}/{owner}/{repo}/raw/{branch}/{rel_path}")
    urls.append(f"https://{_PROXY_MIRROR}/{raw_direct}")
    return urls

@dataclass
class SubscriptionExporter:
    output_dir: Path
    encode: bool = True
    split: bool = True
    repo_owner: str = ""
    repo_name: str = ""
    repo_branch: str = "main"
    configs_path: str = "configs"

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(self, uris: list[str]) -> list[SubscriptionMeta]:
        if not uris:
            logger.warning("No URIs to export")
            return []
        results: list[SubscriptionMeta] = []
        results.append(self._write("sub.txt", uris, "mixed", encode=True))
        results.append(self._write("sub_raw.txt", uris, "mixed", encode=False))
        if self.split:
            groups = group_by_protocol(uris)
            for proto in _PROTOCOL_ORDER:
                if proto in groups:
                    results.append(self._write(f"{proto}.txt", groups[proto], proto, encode=self.encode))
        return results

    def _write(self, filename: str, uris: list[str], protocol: str, encode: bool) -> SubscriptionMeta:
        path = self.output_dir / filename
        content = encode_subscription(uris) if encode else "\n".join(uris)
        path.write_text(content, encoding="utf-8")
        mirrors = []
        if self.repo_owner and self.repo_name:
            rel = f"{self.configs_path}/{filename}"
            mirrors = build_mirror_urls(self.repo_owner, self.repo_name, self.repo_branch, rel)
        return SubscriptionMeta(path=path, protocol=protocol, total=len(uris), mirror_urls=mirrors, encoded=encode)

    def print_mirror_table(self, results: list[SubscriptionMeta]) -> None:
        if not any(r.mirror_urls for r in results):
            return
        logger.info("─" * 60)
        logger.info("Subscription mirror links:")
        logger.info("─" * 60)
        for meta in results:
            if not meta.mirror_urls:
                continue
            logger.info("[{}] {} URIs {}", meta.path.name, meta.total, "[base64]" if meta.encoded else "[plain]")
            for url in meta.mirror_urls:
                logger.info("  {}", url)
        logger.info("─" * 60)
