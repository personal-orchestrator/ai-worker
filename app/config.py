from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    storage_dir: str = "/data/recordings"
    transcriptions_raw_dir: str = "/data/transcriptions-raw"
    transcriptions_dir: str = "/data/transcriptions"
    nats_url: str = "nats://localhost:4222"
    nats_subject: str = "audio.ingested"
    nats_todos_subject: str = "extractor.todos.created"
    nats_transcriptions_subject: str = "transcription.completed"
    groq_api_key: str
    groq_rate_limit_per_minute: int = 10
    groq_extraction_model: str = "llama-3.3-70b-versatile"
    log_level: str = "INFO"
    reindex_poll_interval: int = 60
    
    model_config = SettingsConfigDict(env_file=".env.secrets", env_file_encoding="utf-8", extra="ignore")

settings = Settings()  # type: ignore[call-arg]
