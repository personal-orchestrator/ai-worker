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

## JetStream consumer configuration

The `ai-processor-consumer` durable is created with explicit settings rather than NATS server
defaults, which are a poor fit for this workload. The values are constants in `app/main.py`:

| setting | value | why |
| --- | --- | --- |
| `ack_wait` | 120s | a **death-detection window**, not a duration budget — see below |
| `max_deliver` | 3 | a finite redelivery ceiling; the server default of `-1` never stops |
| `max_ack_pending` | 1 | the worker drains messages serially at `replicas: 1` |
| progress interval | 20s | how often `msg.in_progress()` (`+WPI`) resets the ack timer while a message is in flight |

**`ack_wait` is not sized to cover the work.** `ChatGroq` retries at the HTTP layer with a 60s
timeout per attempt, so a slow call can exceed 120s on its own. `ack_wait` is how long the server
waits after the last sign of life before assuming the worker died; the heartbeat covers duration.
`+WPI` resets the timer without counting as a delivery, so a slow extraction stays on its first
delivery. If the worker dies the beats stop, `ack_wait` expires, and the message is redelivered,
which is the behaviour you want.

The progress interval is 20s rather than something sized against our own 120s because it also has
to beat the **30s server default** that is in force until the step below has been run. A failed
beat is logged and retried on the next tick.

`max_deliver` is the setting that matters most here. Extraction has failed on every message since
2026-08-14 with `groq.NotFoundError` for the configured model, and with `max_deliver=-1` those
failures redeliver forever. Note the consequence: a message that exhausts three deliveries is
**dropped with no record** — there is no dead-letter path yet (issue `B3`) and, unlike
`transcription-worker`, no reindexer to rescan for missed work. A dropped
`transcription.completed` means that dictation's todos are never extracted and nothing notices.

### Applying this to an existing consumer — run this first

`nats-py`'s `js.subscribe()` only applies the config when the durable consumer does not yet
exist; an existing one keeps whatever the server holds, and the code does not detect or repair
that. **On any cluster where the consumer already exists, this one-off command is the entire
fix** — the code change only governs consumers created from scratch afterwards.

```bash
nats consumer edit processing_events ai-processor-consumer \
  --ack-wait=2m --max-deliver=3 --max-pending=1
```

Run it **before** deploying, not after, so there is no interim window still running
`max_ack_pending=1000`.

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not
replay the stream from the start. Verify with `nats consumer info processing_events
ai-processor-consumer` — the startup log deliberately does **not** print the effective settings,
because `js.subscribe()` cannot know them; `consumer info` is the only source of truth.
`Redelivered Messages` should stay near zero under normal load.
