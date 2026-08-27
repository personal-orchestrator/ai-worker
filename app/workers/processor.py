import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from app.processors.base import LiveProcessor

logger = logging.getLogger("ai-worker")

# Must stay below the consumer's ack_wait, including the 30s server default that applies until
# the one-off `nats consumer edit` in the README has been run.
PROGRESS_INTERVAL_SECONDS = 20.0

# How long the heartbeat will hold a single message alive. Past this the beats stop, ack_wait
# expires and the message is redelivered. This matters more here than in transcription-worker:
# ChatGroq is constructed without a request timeout, so a hung socket makes the extraction call
# block forever. Without a cap the heartbeat would hold that message alive indefinitely —
# max_deliver only engages on redelivery, and with max_ack_pending=1 the consumer would never be
# handed another message, going silent behind a healthy-looking pod.
MAX_KEEPALIVE_SECONDS = 600.0

class ProcessorPayload(BaseModel):
    filename: str
    text: str
    out_of_order: bool = False

class ProcessorWorker:
    """
    Worker responsible for passing transcribed text and metadata to a list of live processors.
    """
    
    def __init__(
        self,
        processors: list['LiveProcessor'],
        progress_interval: float = PROGRESS_INTERVAL_SECONDS,
        max_keepalive: float = MAX_KEEPALIVE_SECONDS,
    ):
        self.processors = processors
        self.progress_interval = progress_interval
        self.max_keepalive = max_keepalive

    async def handle_message(self, msg: 'Msg') -> None:
        """Process incoming transcribed text events."""
        subject = msg.subject
        data = msg.data.decode()
        logger.info(f"ProcessorWorker: Received message on {subject}")

        try:
            payload = ProcessorPayload.model_validate_json(data)
            metadata = {"filename": payload.filename, "out_of_order": payload.out_of_order}
            async with self._keep_alive(msg):
                await self._run_processors(payload.text, metadata)
            await msg.ack()
        except ValidationError as e:
            logger.error(f"ProcessorWorker: Invalid payload data: {e}")
            await msg.ack()
        except Exception as e:
            logger.error(f"ProcessorWorker: Error processing message: {e}", exc_info=True)

    @asynccontextmanager
    async def _keep_alive(self, msg: 'Msg'):
        """Hold the message's ack_wait timer open for as long as the body takes."""
        task = asyncio.create_task(self._send_progress(msg))
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _send_progress(self, msg: 'Msg') -> None:
        """Periodically tell JetStream the message is still being worked on.

        +WPI resets the ack timer without counting as a delivery, so a slow extraction stays on
        its first delivery. A failed beat is transient (a reconnect), so the loop keeps going.
        """
        deadline = time.monotonic() + self.max_keepalive
        while time.monotonic() < deadline:
            await asyncio.sleep(self.progress_interval)
            try:
                await msg.in_progress()
            except Exception as e:
                logger.warning(f"ProcessorWorker: In-progress heartbeat failed, retrying next tick: {e}")

        logger.error(
            f"ProcessorWorker: Held a message alive for {self.max_keepalive}s without finishing; "
            f"giving up so ack_wait can expire and it is redelivered rather than blocking the consumer"
        )

    async def _run_processors(self, text: str, metadata: dict) -> None:
        for processor in self.processors:
            logger.info(f"ProcessorWorker: Running processor {processor.__class__.__name__} for {metadata.get('filename')}")
            await processor.process(transcription_text=text, metadata=metadata)
