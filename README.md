# AI Worker

An asynchronous pipeline built for processing and transcribing audio recordings in the Personal Orchestrator.

## Architecture & Flow

1. **Ingestion (`audio.ingested`)**: `TranscriptionWorker` listens to this NATS JetStream subject for incoming audio file notifications.
2. **Transcription & Translation**: Uses the Groq API (`whisper-large-v3`) to process audio stored in the shared volume. It employs a dual-output strategy:
   - **Raw Transcriptions** (`/data/transcriptions-raw`): Literal transcription in the original language.
   - **Normalized Transcriptions** (`/data/transcriptions`): English-translated transcription. To optimize token usage, audio is only sent to the translation endpoint if the original transcription was non-English.
3. **Internal Event Queue (`transcription.completed`)**: Successfully transcribed English text is published to an internal JetStream queue.
4. **Downstream Processors (`ProcessorWorker`)**: Consumes the translated text and routes it to various `LiveProcessors`:
   - **ToDo Extractor**: Leverages LangChain and Groq (`llama-3.3-70b-versatile`) with strict prompt engineering and Pydantic validation to extract explicitly dictated action items. Extracted `ToDo` items (with priorities and reminders) are published as JSON to `extractor.todos.created`.
5. **Reindexing**: A built-in watcher monitors for a `reindex` file trigger. When triggered, it scans the storage directory for any audio files missing from the transcriptions directory and queues them for processing.

## Configuration

The worker is configured via environment variables (or `.env.secrets`):
- `NATS_URL`: The NATS server connection URL.
- `GROQ_API_KEY`: API key for Groq transcription and extraction services.
- `GROQ_RATE_LIMIT_PER_MINUTE`: Throttling rate for Groq API calls (defaults to 10).
- `GROQ_EXTRACTION_MODEL`: LLM used for extraction (defaults to `llama-3.3-70b-versatile`).
- `STORAGE_DIR`: Directory where incoming audio files are mounted.
- `TRANSCRIPTIONS_RAW_DIR`: Output directory for raw literal transcriptions.
- `TRANSCRIPTIONS_DIR`: Output directory for English-normalized transcriptions.
- `NATS_SUBJECT`: Subject for raw audio events (defaults to `audio.ingested`).
- `NATS_TRANSCRIPTIONS_SUBJECT`: Internal queue subject (defaults to `transcription.completed`).
- `NATS_TODOS_SUBJECT`: Outgoing tasks subject (defaults to `extractor.todos.created`).
