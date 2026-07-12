import logging
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
    
    def __init__(self, processors: list['LiveProcessor']):
        self.processors = processors

    async def handle_message(self, msg: 'Msg') -> None:
        """Process incoming transcribed text events."""
        subject = msg.subject
        data = msg.data.decode()
        logger.info(f"ProcessorWorker: Received message on {subject}")

        try:
            payload = ProcessorPayload.model_validate_json(data)
            metadata = {"filename": payload.filename, "out_of_order": payload.out_of_order}
            await self._run_processors(payload.text, metadata)
            await msg.ack()
        except ValidationError as e:
            logger.error(f"ProcessorWorker: Invalid payload data: {e}")
            await msg.ack()
        except Exception as e:
            logger.error(f"ProcessorWorker: Error processing message: {e}", exc_info=True)

    async def _run_processors(self, text: str, metadata: dict) -> None:
        for processor in self.processors:
            logger.info(f"ProcessorWorker: Running processor {processor.__class__.__name__} for {metadata.get('filename')}")
            await processor.process(transcription_text=text, metadata=metadata)
