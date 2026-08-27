import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock
from app.workers.processor import ProcessorWorker

def _js_msg():
    msg = Mock()
    msg.subject = "transcription.completed"
    msg.data = json.dumps({"filename": "test.m4a", "text": "Buy milk", "out_of_order": False}).encode("utf-8")
    msg.ack = AsyncMock()
    msg.in_progress = AsyncMock()
    return msg

async def _slow_process(**kwargs):
    """Processing slow enough to outlive a short heartbeat interval."""
    await asyncio.sleep(0.05)

def _slow_worker():
    mock_processor = AsyncMock()
    mock_processor.process = AsyncMock(side_effect=_slow_process)
    return ProcessorWorker(processors=[mock_processor], progress_interval=0.001)

@pytest.mark.asyncio
async def test_processor_worker_handle_message():
    mock_processor = AsyncMock()
    mock_processor.process = AsyncMock()

    worker = ProcessorWorker(processors=[mock_processor])
    msg = _js_msg()

    await worker.handle_message(msg)

    mock_processor.process.assert_called_once_with(
        transcription_text="Buy milk",
        metadata={"filename": "test.m4a", "out_of_order": False}
    )
    msg.ack.assert_called_once()

@pytest.mark.asyncio
async def test_heartbeat_sent_while_processing():
    worker = _slow_worker()
    msg = _js_msg()

    await worker.handle_message(msg)

    assert msg.in_progress.await_count > 0
    msg.ack.assert_awaited_once()

@pytest.mark.asyncio
async def test_heartbeat_stops_once_processing_finishes():
    worker = _slow_worker()
    msg = _js_msg()

    await worker.handle_message(msg)
    settled = msg.in_progress.await_count

    await asyncio.sleep(0.05)

    assert settled > 0, "no heartbeat ran, so this proves nothing about it stopping"
    assert msg.in_progress.await_count == settled

@pytest.mark.asyncio
async def test_failing_heartbeat_neither_stops_beating_nor_fails_the_message():
    """A failed +WPI is transient, so the loop must keep going and the work must complete.

    Beating more than once while every beat raises proves both halves: the loop survived a
    failure, and the failure never reached handle_message.
    """
    worker = _slow_worker()
    msg = _js_msg()
    msg.in_progress = AsyncMock(side_effect=RuntimeError("connection draining"))

    await worker.handle_message(msg)

    assert msg.in_progress.await_count > 1, "heartbeat stopped after the first failure"
    msg.ack.assert_awaited_once()
