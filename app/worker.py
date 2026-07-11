import fcntl
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from app.services.transcription import TranscriptionService, TranscriptionResult

logger = logging.getLogger("ai-worker")

class TranscriptionWorker:
    def __init__(self, transcription_service: TranscriptionService, storage_dir: str, transcriptions_dir: str):
        self.transcription_service = transcription_service
        self.storage_dir = storage_dir
        self.transcriptions_dir = transcriptions_dir

        os.makedirs(self.storage_dir, exist_ok=True)
        os.makedirs(self.transcriptions_dir, exist_ok=True)

    async def handle_message(self, msg):
        subject = msg.subject
        data = msg.data.decode()
        logger.info(f"Received message on {subject}: {data}")

        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON payload: {data}")
            return
            
        filename = payload.get("filename")
        out_of_order = payload.get("out_of_order", False)
        
        if not filename:
            logger.error("Message missing 'filename' field")
            return

        try:
            file_path = self._get_file_path(filename)
            if not file_path:
                return

            await self._process_file(file_path, filename, out_of_order)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def _get_file_path(self, filename: str) -> Optional[str]:
        file_path = os.path.join(self.storage_dir, filename)
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
        return file_path

    async def _process_file(self, file_path: str, filename: str, out_of_order: bool = False):
        logger.info(f"Starting transcription for {filename}")
        result = await self.transcription_service.transcribe(file_path)
        
        self._save_transcription(result, filename, out_of_order)

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

    def _save_transcription(self, result: TranscriptionResult, original_filename: str, out_of_order: bool = False):
        timestamp = self._extract_timestamp(original_filename)
        transcription_data = self._create_transcription_record(result, original_filename, timestamp)
        output_path = self._get_output_path(timestamp)
        
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

    def _get_output_path(self, timestamp: str) -> str:
        try:
            date_str = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d")
        except Exception:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return os.path.join(self.transcriptions_dir, f"transcripts_{date_str}.jsonl")

    def _append_to_file(self, data: dict, output_path: str):
        with open(output_path, "a", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                json.dump(data, f, ensure_ascii=False)
                f.write("\n")
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _sort_file(self, output_path: str):
        if not os.path.exists(output_path):
            return
            
        with open(output_path, "r+", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.seek(0)
                lines = f.readlines()
                records = []
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
                
                records.sort(key=lambda x: x.get("timestamp", ""))
                
                f.seek(0)
                f.truncate()
                for record in records:
                    json.dump(record, f, ensure_ascii=False)
                    f.write("\n")
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

