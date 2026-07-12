from abc import ABC, abstractmethod

class LiveProcessor(ABC):
    """
    Base class for processors that run immediately on incoming single-dictation transcripts.
    """
    
    @abstractmethod
    async def process(self, transcription_text: str, metadata: dict) -> None:
        """
        Process the given transcription text.
        
        Args:
            transcription_text: The English transcription text to process.
            metadata: Metadata associated with the transcription (e.g. filename, timestamp, etc.)
        """
        pass
