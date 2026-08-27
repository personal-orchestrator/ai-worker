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
defaults, which are a poor fit for this workload. The values are constants in `app/main.py` and
`app/workers/processor.py`:

| setting | value | why |
| --- | --- | --- |
| `ack_wait` | 120s | a **death-detection window**, not a duration budget — the heartbeat covers duration |
| `max_deliver` | 3 | a finite redelivery ceiling; the server default of `-1` never stops |
| `max_ack_pending` | 1 | the worker drains messages serially at `replicas: 1` |
| progress interval | 20s | how often `msg.in_progress()` (`+WPI`) resets the ack timer while a message is in flight |

The interval is 20s rather than something sized against our own 120s because it also has to beat
the **30s server default** in force until the step below has been run.

**Known limitation.** The extraction call has no HTTP timeout — `ChatGroq` is built without
`request_timeout` — so a hung socket never returns, and since `nats-py` dispatches a
subscription's callbacks serially that blocks the consumer entirely. Recovering means restarting
the pod. Giving the call a real timeout is the fix and is tracked separately; bounding the
heartbeat is not, since it would release the ack claim while the work continued and duplicate the
message when the call returned.

**Messages that exhaust `max_deliver` are dropped with no record** — there is no dead-letter path
yet (issue `B3`), and this service has no reindexer, so a lost `transcription.completed` means
that dictation's todos are never extracted and nothing notices. Note this only bites on a crash or
a hang: a message whose *processing* fails is acked anyway and never redelivered, which is issues
`B1`/`B2`.

### Applying this to an existing consumer

`nats-py`'s `js.subscribe()` only applies the config when the durable does not yet exist; an
existing one keeps whatever the server holds, and the code does not detect or repair that. The
heartbeat ships in code and applies either way, but the three consumer settings do not — on an
existing cluster they arrive only via:

```bash
nats consumer edit processing_events ai-processor-consumer \
  --ack-wait=2m --max-pending=1
nats consumer edit processing_events ai-processor-consumer --max-deliver=3
```

**Two commands, in that order, and check the backlog between them.** `--ack-wait` and
`--max-pending` stop the amplification without dropping anything. `--max-deliver=3` discards every
message already past three deliveries as it comes up for redelivery — against the measured backlog
(35,785 deliveries for 9,270 messages), all of them — and nothing here recovers them. Apply it
only once that set has drained.

Check with `nats consumer info processing_events ai-processor-consumer` and watch
**`Outstanding Acks`** (`num_ack_pending`) fall to zero. Watch that field, not `Unprocessed
Messages`: the latter is `num_pending`, the messages never delivered at all, and while the
consumer is saturated at `max_ack_pending=1000` it reads zero even with the whole backlog sitting
in the redelivery set. Reading it as "drained" is how you lose the backlog.

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not replay
the stream from the start. `nats consumer info` is the source of truth for what is actually in
force; `Redelivered Messages` should stay near zero under normal load.
