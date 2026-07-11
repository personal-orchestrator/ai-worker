import os
import json
import logging
from typing import Set

logger = logging.getLogger("ai-worker")

class Reindexer:
    def __init__(self, storage_dir: str, transcriptions_dir: str, nc, nats_subject: str):
        self.storage_dir = storage_dir
        self.transcriptions_dir = transcriptions_dir
        self.nc = nc
        self.nats_subject = nats_subject

    async def run(self):
        logger.info("Starting reindexing process")
        transcribed_files = self._get_transcribed_files()
        
        if not os.path.exists(self.storage_dir):
            logger.warning(f"Storage directory {self.storage_dir} does not exist.")
            return

        all_audio_files = [
            f for f in os.listdir(self.storage_dir) 
            if os.path.isfile(os.path.join(self.storage_dir, f)) and (f.endswith('.m4a') or f.endswith('.mp4'))
        ]
        
        untranscribed = [f for f in all_audio_files if f not in transcribed_files]
        logger.info(f"Found {len(untranscribed)} untranscribed files out of {len(all_audio_files)} total audio files")

        for filename in untranscribed:
            try:
                payload = json.dumps({"filename": filename, "out_of_order": True}).encode("utf-8")
                await self.nc.publish(self.nats_subject, payload)
                logger.info(f"Published reindex task for {filename}")
            except Exception as e:
                logger.error(f"Failed to publish reindex task for {filename}: {e}")

        logger.info("Reindexing process finished publishing tasks")

    def _get_transcribed_files(self) -> Set[str]:
        transcribed: Set[str] = set()
        if not os.path.exists(self.transcriptions_dir):
            return transcribed

        for filename in os.listdir(self.transcriptions_dir):
            if filename.endswith(".jsonl"):
                file_path = os.path.join(self.transcriptions_dir, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            try:
                                data = json.loads(line)
                                if "original_file" in data:
                                    transcribed.add(data["original_file"])
                            except json.JSONDecodeError:
                                pass
                except Exception as e:
                    logger.error(f"Failed to read transcripts file {file_path}: {e}")
                    
        return transcribed
