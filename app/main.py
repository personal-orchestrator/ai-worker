import asyncio
import logging
import signal
import nats
from dataclasses import dataclass
from typing import Callable, Awaitable, Any

from nats.js import api

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
    consumer_config: api.ConsumerConfig

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
        processor_worker = ProcessorWorker(
            processors=[task_extractor],
            progress_interval=settings.nats_progress_interval,
        )

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

        await self._subscribe_consumers(js, processor_worker)

        self._setup_signal_handlers()

        await self.stop_event.wait()

        logger.info("Unsubscribing and closing NATS connection")
        for sub in getattr(self, 'subs', []):
            await sub.unsubscribe()
        await self.nc.close()

    @staticmethod
    def _consumer_config() -> api.ConsumerConfig:
        """Consumer settings sized for a single worker doing rate-limited LLM work.

        NOTE: nats-py only applies this when the durable consumer does not exist yet. An
        already-created consumer keeps whatever config the server holds, so consumers that
        predate this change need a one-off `nats consumer edit`.
        """
        return api.ConsumerConfig(
            ack_wait=settings.nats_ack_wait,
            max_deliver=settings.nats_max_deliver,
            max_ack_pending=settings.nats_max_ack_pending,
        )

    async def _subscribe_consumers(self, js, processor_worker):
        subscriptions = [
            SubscriptionConfig(
                subject=settings.nats_transcriptions_subject,
                cb=processor_worker.handle_message,
                durable="ai-processor-consumer",
                stream="processing_events",
                consumer_config=self._consumer_config()
            )
        ]

        self.subs = []
        for sub_config in subscriptions:
            sub = await js.subscribe(
                sub_config.subject,
                cb=sub_config.cb,
                durable=sub_config.durable,
                stream=sub_config.stream,
                config=sub_config.consumer_config
            )
            self.subs.append(sub)
            logger.info(
                f"Subscribed to JetStream subject {sub_config.subject} with durable consumer "
                f"{sub_config.durable} (ack_wait: {sub_config.consumer_config.ack_wait}s, "
                f"max_deliver: {sub_config.consumer_config.max_deliver}, "
                f"max_ack_pending: {sub_config.consumer_config.max_ack_pending})"
            )

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
