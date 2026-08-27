import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from app.processors.base import LiveProcessor

logger = logging.getLogger("ai-worker")

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
        # Must stay well below the consumer's ack_wait, including the 30s server default that
        # applies until the one-off `nats consumer edit` in the README has been run.
        progress_interval: float = 20.0,
    ):
        self.processors = processors
        self.progress_interval = progress_interval

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

        The +WPI this sends resets the ack timer without counting as a delivery, so a slow
        extraction stays on its first delivery instead of being handed out again and
        re-processed.

        A failed beat is logged and retried on the next tick rather than ending the loop: the
        publish fails transiently while the client reconnects, and giving up on the first one
        would silently retire the protection for the rest of the message. The loop is ended by
        _keep_alive cancelling it.
        """
        while True:
            await asyncio.sleep(self.progress_interval)
            try:
                await msg.in_progress()
            except Exception as e:
                logger.warning(f"ProcessorWorker: In-progress heartbeat failed, retrying next tick: {e}")

    async def _run_processors(self, text: str, metadata: dict) -> None:
        for processor in self.processors:
            logger.info(f"ProcessorWorker: Running processor {processor.__class__.__name__} for {metadata.get('filename')}")
            await processor.process(transcription_text=text, metadata=metadata)
