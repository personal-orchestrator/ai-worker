import asyncio
import logging
import signal
import nats
from nats.aio.client import Client as NATS

from app.config import settings
from app.services.transcription import GroqTranscriptionService
from app.worker import TranscriptionWorker

logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai-worker")


class Application:
    def __init__(self):
        self.nc = nats.NATS()
        self.stop_event = asyncio.Event()

    async def run(self):
        logger.info(f"Connecting to NATS at {settings.nats_url}")
        await self.nc.connect(settings.nats_url, connect_timeout=10)
        logger.info("Connected to NATS")

        transcription_service = GroqTranscriptionService(api_key=settings.groq_api_key)
        worker = TranscriptionWorker(
            transcription_service=transcription_service,
            storage_dir=settings.storage_dir,
            transcriptions_dir=settings.transcriptions_dir,
        )

        sub = await self.nc.subscribe(settings.nats_subject, cb=worker.handle_message)
        logger.info(f"Subscribed to subject {settings.nats_subject}")

        self._setup_signal_handlers()

        await self.stop_event.wait()

        logger.info("Unsubscribing and closing NATS connection")
        await sub.unsubscribe()
        await self.nc.close()

    def _setup_signal_handlers(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._signal_handler)

    def _signal_handler(self):
        logger.info("Shutdown signal received")
        self.stop_event.set()


if __name__ == "__main__":
    app = Application()
    asyncio.run(app.run())
