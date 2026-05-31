from pydantic_settings import BaseSettings, SettingsConfigDict
import platform

def _default_concurrent() -> int:
    """Определяет разумный лимит конкурентности для устройства."""
    if platform.machine().startswith(("arm", "aarch")):
        return 10
    try:
        import resource
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        return min(soft // 4, 30)
    except Exception:
        return 20

def _default_validation_concurrent() -> int:
    if platform.machine().startswith(("arm", "aarch")):
        return 50
    return 200

class Settings(BaseSettings):
    sources_file:  str   = "sources.yaml"
    output_dir:    str   = "configs"
    request_timeout: float = 20.0
    user_agent:    str   = "Mozilla/5.0 (compatible; ConfigCollector/2.0)"
    github_token:  str | None = None
    max_concurrent_fetches: int = _default_concurrent()
    log_level:     str   = "INFO"
    repo_owner:    str   = ""
    repo_name:     str   = ""
    repo_branch:   str   = "main"
    tcp_timeout:                float = 3.0
    max_concurrent_validations: int   = _default_validation_concurrent()

    model_config = SettingsConfigDict(
        env_prefix="BS_",
        env_file=".env",
        extra="ignore",
    )
