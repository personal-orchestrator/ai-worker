import asyncio
import logging
import signal
import os
import nats

from app.config import settings
from app.services.transcription import GroqTranscriptionService
from app.worker import TranscriptionWorker
from app.reindexer import Reindexer

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

        transcription_service = GroqTranscriptionService(
            api_key=settings.groq_api_key,
            rate_limit_per_minute=settings.groq_rate_limit_per_minute
        )
        worker = TranscriptionWorker(
            transcription_service=transcription_service,
            storage_dir=settings.storage_dir,
            transcriptions_raw_dir=settings.transcriptions_raw_dir,
            transcriptions_dir=settings.transcriptions_dir,
        )

        js = self.nc.jetstream()
        try:
            await js.add_stream(name="audio_events", subjects=[settings.nats_subject])
            logger.info("JetStream stream 'audio_events' ensured")
        except Exception as e:
            logger.info(f"Stream may already exist or cannot be modified: {e}")

        sub = await js.subscribe(
            settings.nats_subject,
            cb=worker.handle_message,
            durable="ai-worker-consumer",
            stream="audio_events"
        )
        logger.info(f"Subscribed to JetStream subject {settings.nats_subject} with durable consumer")

        self._setup_signal_handlers()
        
        watcher_task = asyncio.create_task(self._watch_for_reindex())

        await self.stop_event.wait()
        
        watcher_task.cancel()

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

    async def _watch_for_reindex(self):
        reindex_file = os.path.join(os.path.dirname(settings.storage_dir), "reindex")
        logger.info(f"Starting reindex watcher on {reindex_file}, polling every {settings.reindex_poll_interval}s")
        
        reindexer = Reindexer(
            storage_dir=settings.storage_dir,
            transcriptions_dir=settings.transcriptions_dir,
            nc=self.nc,
            nats_subject=settings.nats_subject
        )
        
        while not self.stop_event.is_set():
            try:
                if os.path.exists(reindex_file):
                    try:
                        os.remove(reindex_file)
                        logger.info("Reindex file detected and removed. Triggering reindex.")
                        await reindexer.run()
                    except FileNotFoundError:
                        # Another worker replica might have removed it, ignore
                        pass
                    except Exception as e:
                        logger.error(f"Error removing reindex file or running reindexer: {e}")
            except Exception as e:
                logger.error(f"Error in reindex watcher loop: {e}")
                
            try:
                await asyncio.sleep(settings.reindex_poll_interval)
            except asyncio.CancelledError:
                break


if __name__ == "__main__":
    app = Application()
    asyncio.run(app.run())
