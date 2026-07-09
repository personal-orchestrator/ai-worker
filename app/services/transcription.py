import abc
import os
from typing import Dict, Any, Optional
from groq import AsyncGroq

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
        """Transcribe an audio file and return the result."""
        pass

class GroqTranscriptionService(TranscriptionService):
    def __init__(self, api_key: str):
        self.client = AsyncGroq(api_key=api_key)

    async def transcribe(self, audio_file_path: str) -> TranscriptionResult:
        with open(audio_file_path, "rb") as file:
            response = await self.client.audio.transcriptions.create(
                file=(os.path.basename(audio_file_path), file.read()),
                model="whisper-large-v3-turbo",
                response_format="verbose_json"
            )
            
            # groq returns a Transcription object which for verbose_json contains language
            text = response.text
            language = getattr(response, "language", None)
            
            return TranscriptionResult(text=text, language=language)
