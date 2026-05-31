import base64
from urllib.parse import unquote

def decode_base64_text(value: str) -> str:
    cleaned = unquote(value.strip()).replace("\n", "").replace("\r", "")
    padded = cleaned + "=" * (-len(cleaned) % 4)
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            return decoder(padded.encode("utf-8")).decode("utf-8", errors="strict")
        except Exception:
            continue
    raise ValueError("Invalid base64 payload")
