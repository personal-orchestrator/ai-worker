import fcntl
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient
    from nats.aio.msg import Msg

from app.services.transcription import TranscriptionService, TranscriptionResult

logger = logging.getLogger("ai-worker")

class TranscriptionPayload(BaseModel):
    filename: str
    out_of_order: bool = False

class TranscriptionWorker:
    """
    Worker responsible for handling audio transcription requests, translating non-English audio to English,
    and publishing the completed English transcription to an internal topic.
    """
    
    def __init__(
        self, 
        transcription_service: TranscriptionService, 
        storage_dir: str, 
        transcriptions_raw_dir: str, 
        transcriptions_dir: str, 
        nc: Optional['NatsClient'] = None, 
        nats_transcriptions_subject: Optional[str] = None
    ):
        self.transcription_service = transcription_service
        self.storage_dir = storage_dir
        self.transcriptions_raw_dir = transcriptions_raw_dir
        self.transcriptions_dir = transcriptions_dir
        self.nc = nc
        self.nats_transcriptions_subject = nats_transcriptions_subject

        os.makedirs(self.storage_dir, exist_ok=True)
        os.makedirs(self.transcriptions_raw_dir, exist_ok=True)
        os.makedirs(self.transcriptions_dir, exist_ok=True)

    async def handle_message(self, msg: 'Msg') -> None:
        """Process incoming audio transcription events."""
        subject = msg.subject
        data = msg.data.decode()
        logger.info(f"Received message on {subject}: {data}")

        try:
            payload = TranscriptionPayload.model_validate_json(data)
            
            file_path = self._get_file_path(payload.filename)
            if not file_path:
                await msg.ack()
                return

            await self._process_file(file_path, payload.filename, payload.out_of_order)
            await msg.ack()

        except ValidationError as e:
            logger.error(f"Invalid payload data: {e}")
            await msg.ack()
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def _get_file_path(self, filename: str) -> Optional[str]:
        file_path = os.path.join(self.storage_dir, filename)
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        return file_path

    async def _process_file(self, file_path: str, filename: str, out_of_order: bool = False) -> None:
        logger.info(f"Starting transcription for {filename}")
        raw_result = await self.transcription_service.transcribe(file_path)
        
        # Save raw transcription
        self._save_transcription(raw_result, filename, self.transcriptions_raw_dir, out_of_order)
        
        language = (raw_result.language or "").lower()
        if language in ("english", "en"):
            logger.info(f"Audio is already English. Copying raw transcription to translated directory for {filename}")
            self._save_transcription(raw_result, filename, self.transcriptions_dir, out_of_order)
            final_text = raw_result.text
        else:
            logger.info(f"Audio is non-English ({language}). Starting translation for {filename}")
            translated_result = await self.transcription_service.translate(file_path)
            self._save_transcription(translated_result, filename, self.transcriptions_dir, out_of_order)
            final_text = translated_result.text

        await self._publish_completed_transcription(filename, final_text, out_of_order)

    async def _publish_completed_transcription(self, filename: str, text: str, out_of_order: bool) -> None:
        if not (self.nc and self.nats_transcriptions_subject):
            return
            
        payload = {
            "filename": filename,
            "text": text,
            "out_of_order": out_of_order
        }
        logger.info(f"Publishing completed transcription to {self.nats_transcriptions_subject} for {filename}")
        await self.nc.publish(self.nats_transcriptions_subject, json.dumps(payload).encode())

    def _extract_timestamp(self, filename: str) -> str:
        # Example: rec_1783484942586_4676c04a-e4bc-473b-ab31-43d5966a9be7.m4a
        try:
            parts = filename.split("_")
            if len(parts) >= 2 and parts[1].isdigit():
                timestamp_ms = int(parts[1])
                return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        except Exception as e:
            logger.warning(f"Could not extract timestamp from {filename}: {e}")
        
        return datetime.now(timezone.utc).isoformat()

    def _save_transcription(self, result: TranscriptionResult, original_filename: str, target_dir: str, out_of_order: bool = False) -> None:
        timestamp = self._extract_timestamp(original_filename)
        transcription_data = self._create_transcription_record(result, original_filename, timestamp)
        output_path = self._get_output_path(timestamp, target_dir)
        
        self._append_to_file(transcription_data, output_path)
        logger.info(f"Successfully appended transcription to {output_path}")
        
        if out_of_order:
            self._sort_file(output_path)
            logger.info(f"Successfully sorted transcripts in {output_path} after processing out-of-order file")

    def _create_transcription_record(self, result: TranscriptionResult, original_filename: str, timestamp: str) -> dict:
        data = {
            "timestamp": timestamp,
            "original_file": original_filename,
            "transcription": result.text,
        }
        if result.language:
            data["language"] = result.language
        return data

    def _get_output_path(self, timestamp: str, target_dir: str) -> str:
        try:
            date_str = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d")
        except Exception:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return os.path.join(target_dir, f"transcripts_{date_str}.jsonl")

    @contextmanager
    def _file_lock(self, file_obj):
        fcntl.flock(file_obj, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(file_obj, fcntl.LOCK_UN)

    def _append_to_file(self, data: dict, output_path: str) -> None:
        with open(output_path, "a", encoding="utf-8") as f:
            with self._file_lock(f):
                json.dump(data, f, ensure_ascii=False)
                f.write("\n")

    def _sort_file(self, output_path: str) -> None:
        if not os.path.exists(output_path):
            return
            
        with open(output_path, "r+", encoding="utf-8") as f:
            with self._file_lock(f):
                records = self._read_records(f)
                records.sort(key=lambda x: x.get("timestamp", ""))
                self._write_records(f, records)

    def _read_records(self, f) -> list[dict]:
        f.seek(0)
        records = []
        for line in f:
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return records

    def _write_records(self, f, records: list[dict]) -> None:
        f.seek(0)
        f.truncate()
        for record in records:
            json.dump(record, f, ensure_ascii=False)
            f.write("\n")
