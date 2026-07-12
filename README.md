# AI Worker

An asynchronous pipeline built for processing and transcribing audio recordings in the Personal Orchestrator.

## Architecture & Flow

1. **Ingestion**: Listens to the NATS subject `audio.ingested` for incoming file notifications.
2. **Transcription Strategy**: Uses the Groq API (`whisper-large-v3`) to process audio stored in the shared volume. It employs a dual-output strategy:
   - **Raw Transcriptions** (`/data/transcriptions-raw`): A literal transcription in the audio's original language.
   - **Normalized Transcriptions** (`/data/transcriptions`): An English-translated transcription. To optimize token usage, the audio is only sent to the translation endpoint if the original transcription was non-English.
3. **Reindexing**: A built-in watcher monitors for a `reindex` file trigger. When triggered, it scans the storage directory for any audio files missing from the transcriptions directory and queues them for processing.

## Configuration

The worker is configured via environment variables (or `.env.secrets`):
- `NATS_URL`: The NATS server connection URL.
- `GROQ_API_KEY`: API key for Groq transcription services.
- `STORAGE_DIR`: Directory where incoming audio files are mounted.
- `TRANSCRIPTIONS_RAW_DIR`: Output directory for raw literal transcriptions.
- `TRANSCRIPTIONS_DIR`: Output directory for English-normalized transcriptions.
