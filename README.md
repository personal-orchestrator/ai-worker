# AI Worker

An asynchronous pipeline built for processing and transcribing audio recordings in the Personal Orchestrator.

## Architecture & Flow

1. **Ingestion (`audio.ingested`)**: `TranscriptionWorker` listens to this NATS JetStream subject for incoming audio file notifications.
2. **Transcription & Translation**: Uses the Groq API (`whisper-large-v3`) to process audio stored in the shared volume. It employs a dual-output strategy:
   - **Raw Transcriptions** (`/data/transcriptions-raw`): Literal transcription in the original language.
   - **Normalized Transcriptions** (`/data/transcriptions`): English-translated transcription. To optimize token usage, audio is only sent to the translation endpoint if the original transcription was non-English.
3. **Internal Event Queue (`transcription.completed`)**: Successfully transcribed English text is published to an internal JetStream queue.
4. **Downstream Processors (`ProcessorWorker`)**: Consumes the translated text and routes it to various `LiveProcessors`:
   - **ToDo Extractor**: Leverages LangChain and Groq (`llama-3.3-70b-versatile`) with strict prompt engineering and Pydantic validation to extract explicitly dictated action items. Extracted `ToDo` items (with priorities and reminders) are published as JSON to `extractor.todos.created`. Additionally, if tasks are found, they are saved alongside the original transcript to an evaluation log (`tasks.jsonl` in `tasks_eval_dir`) for fine-tuning and false-positive tracking.
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
- `TASKS_EVAL_DIR`: Output directory for task extraction evaluation logs (defaults to `/data/tasks-eval`).
- `NATS_SUBJECT`: Subject for raw audio events (defaults to `audio.ingested`).
- `NATS_TRANSCRIPTIONS_SUBJECT`: Internal queue subject (defaults to `transcription.completed`).
- `NATS_TODOS_SUBJECT`: Outgoing tasks subject (defaults to `extractor.todos.created`).
- `NATS_ACK_WAIT`: How long JetStream waits for an ack before redelivering (defaults to 120s).
- `NATS_MAX_DELIVER`: Redelivery ceiling per message (defaults to 3).
- `NATS_MAX_ACK_PENDING`: Outstanding unacked messages the server may push (defaults to 1).
- `NATS_PROGRESS_INTERVAL`: How often `msg.in_progress()` resets the ack timer while a message is in flight (defaults to 30s).

## JetStream consumer configuration

The `ai-processor-consumer` durable is created with explicit settings rather than NATS server
defaults, which are a poor fit for this workload — extraction is a rate-limited LLM call that
can outrun the default 30s `ack_wait`, and `max_deliver=-1` means a message that keeps failing
is retried forever.

The heartbeat is what protects slow messages: `+WPI` resets `ack_wait` without counting as a
delivery, so a slow extraction stays on its first delivery instead of being handed out again.
If the worker dies, the heartbeats stop and the message is redelivered as normal.

**Applying this to an existing consumer.** `nats-py`'s `js.subscribe()` only applies the config
when the durable consumer does not yet exist; an existing one keeps whatever the server holds.
Consumers created before this change need a one-off:

```bash
nats consumer edit processing_events ai-processor-consumer \
  --ack-wait=2m --max-deliver=3 --max-pending=1
```

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not
replay the stream from the start. Verify with `nats consumer info processing_events
ai-processor-consumer` — `Redelivered Messages` should stay near zero under normal load.
