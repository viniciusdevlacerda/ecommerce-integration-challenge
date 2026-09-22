from __future__ import annotations

from functools import lru_cache
from urllib.parse import quote_plus, urlencode

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    mssql_host: str = "mssql"
    mssql_port: int = 1433
    mssql_user: str = "sa"
    mssql_password: str = "Str0ng@Pass2026"
    mssql_db: str = "ecommerce_ops"
    odbc_driver: str = "ODBC Driver 18 for SQL Server"

    db_pool_size: int = 8
    db_max_overflow: int = 0
    db_pool_timeout: int = 30
    db_pool_recycle: int = 1800

    redis_url: str = "redis://redis:6379/0"
    stream_name: str = "events:ingest"
    stream_maxlen: int = 1_000_000
    consumer_group: str = "cg-persist"
    dedupe_ttl_seconds: int = 86_400

    buffer_max_batch: int = 500
    buffer_flush_ms: int = 5
    buffer_queue_size: int = 20_000
    backpressure_max_lag: int = 100_000
    backpressure_probe_ms: int = 500

    consumer_batch_size: int = 500
    consumer_block_ms: int = 1_000
    consumer_idle_reclaim_ms: int = 60_000
    consumer_max_delivery: int = 5

    erp_base_url: str = "http://erp-mock:8000"
    erp_timeout_seconds: float = 5.0
    dispatch_batch_size: int = 50
    dispatch_poll_interval_ms: int = 500
    dispatch_max_attempts: int = 5
    dispatch_backoff_base_seconds: float = 1.0
    dispatch_backoff_max_seconds: float = 60.0

    erp_failure_rate: float = Field(default=0.25, ge=0.0, le=1.0)
    erp_latency_ms: int = 40

    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        return self._build_url(self.mssql_db)

    @property
    def master_url(self) -> str:
        return self._build_url("master")

    def _build_url(self, database: str) -> str:
        query = urlencode(
            {
                "driver": self.odbc_driver,
                "TrustServerCertificate": "yes",
                "Encrypt": "yes",
            }
        )
        return (
            f"mssql+pyodbc://{self.mssql_user}:{quote_plus(self.mssql_password)}"
            f"@{self.mssql_host}:{self.mssql_port}/{database}?{query}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
