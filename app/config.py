from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    storage_dir: str = "/data/recordings"
    transcriptions_dir: str = "/data/transcriptions"
    nats_url: str = "nats://localhost:4222"
    nats_subject: str = "audio.ingested"
    groq_api_key: str
    log_level: str = "INFO"
    reindex_poll_interval: int = 60
    
    model_config = SettingsConfigDict(env_file=".env.secrets", env_file_encoding="utf-8", extra="ignore")

settings = Settings()  # type: ignore[call-arg]
