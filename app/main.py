import asyncio
import logging
import signal
import nats
from dataclasses import dataclass
from typing import Callable, Awaitable, Any

from app.config import settings
from app.workers import ProcessorWorker
from app.processors.task_extractor import TaskExtractorProcessor

@dataclass
class StreamConfig:
    name: str
    subjects: list[str]

@dataclass
class SubscriptionConfig:
    subject: str
    cb: Callable[[Any], Awaitable[None]]
    durable: str
    stream: str

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

        task_extractor = TaskExtractorProcessor(nc=self.nc)
        processor_worker = ProcessorWorker(processors=[task_extractor])

        js = self.nc.jetstream()
        
        streams = [
            StreamConfig(name="processing_events", subjects=[settings.nats_transcriptions_subject]),
            StreamConfig(name="extractor_events", subjects=[settings.nats_todos_subject]),
        ]
        
        for stream in streams:
            try:
                await js.add_stream(name=stream.name, subjects=stream.subjects)
                logger.info(f"JetStream stream '{stream.name}' ensured")
            except Exception as e:
                logger.info(f"Stream '{stream.name}' may already exist or cannot be modified: {e}")

        subscriptions = [
            SubscriptionConfig(
                subject=settings.nats_transcriptions_subject,
                cb=processor_worker.handle_message,
                durable="ai-processor-consumer",
                stream="processing_events"
            )
        ]
        
        self.subs = []
        for sub_config in subscriptions:
            sub = await js.subscribe(
                sub_config.subject,
                cb=sub_config.cb,
                durable=sub_config.durable,
                stream=sub_config.stream
            )
            self.subs.append(sub)
            logger.info(f"Subscribed to JetStream subject {sub_config.subject} with durable consumer {sub_config.durable}")

        self._setup_signal_handlers()

        await self.stop_event.wait()

        logger.info("Unsubscribing and closing NATS connection")
        for sub in getattr(self, 'subs', []):
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
