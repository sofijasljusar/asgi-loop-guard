from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ELG_")

    host: str = "127.0.0.1"
    port: int = 8000
    upstream_host: str = "127.0.0.1"
    upstream_port: int = 8001

    lag_interval_ms: float = 10.0
    lag_history_size: int = 2000

    max_concurrent_upstream: int = 64
    max_thread_workers: int = 16
    max_queued_threads: int = 32
    default_timeout_s: float = 2.0
    retry_after_s: int = 1

settings = Settings()
