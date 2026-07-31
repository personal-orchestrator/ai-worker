import json
import pytest
from unittest.mock import AsyncMock, Mock
from app.workers.processor import ProcessorWorker

@pytest.mark.asyncio
async def test_processor_worker_handle_message():
    mock_processor = AsyncMock()
    mock_processor.process = AsyncMock()

    worker = ProcessorWorker(processors=[mock_processor])

    msg = Mock()
    msg.subject = "transcription.completed"
    msg.data = json.dumps({"filename": "test.m4a", "text": "Buy milk", "out_of_order": False}).encode("utf-8")
    msg.ack = AsyncMock()

    await worker.handle_message(msg)

    mock_processor.process.assert_called_once_with(
        transcription_text="Buy milk",
        metadata={"filename": "test.m4a", "out_of_order": False}
    )
    msg.ack.assert_called_once()
