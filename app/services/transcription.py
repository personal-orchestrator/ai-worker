import abc
import os
import logging
from typing import Dict, Any, Optional
from groq import AsyncGroq
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

logger = logging.getLogger("ai-worker")

class TranscriptionResult:
    def __init__(self, text: str, language: Optional[str] = None):
        self.text = text
        self.language = language

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text,
            "language": self.language
        }

class TranscriptionService(abc.ABC):
    @abc.abstractmethod
    async def transcribe(self, audio_file_path: str) -> TranscriptionResult:
        """Transcribe an audio file and return the raw literal result."""
        pass

    @abc.abstractmethod
    async def translate(self, audio_file_path: str) -> TranscriptionResult:
        """Translate an audio file and return the English normalized result."""
        pass

class GroqTranscriptionService(TranscriptionService):
    def __init__(self, api_key: str):
        self.client = AsyncGroq(api_key=api_key)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    async def transcribe(self, audio_file_path: str) -> TranscriptionResult:
        with open(audio_file_path, "rb") as file:
            response = await self.client.audio.transcriptions.create(
                file=(os.path.basename(audio_file_path), file.read()),
                model="whisper-large-v3",
                response_format="verbose_json"
            )
            
            text = response.text
            language = getattr(response, "language", None)
            
            return TranscriptionResult(text=text, language=language)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    async def translate(self, audio_file_path: str) -> TranscriptionResult:
        with open(audio_file_path, "rb") as file:
            response = await self.client.audio.translations.create(
                file=(os.path.basename(audio_file_path), file.read()),
                model="whisper-large-v3",
                response_format="verbose_json"
            )
            
            text = response.text
            # Translations to English usually have "english" as the implied language
            # But the response format might still contain it. We can set it to "english" explicitly.
            language = getattr(response, "language", "english")
            
            return TranscriptionResult(text=text, language=language)
