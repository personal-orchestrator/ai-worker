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

def _slow_worker(**kwargs):
    mock_processor = AsyncMock()
    mock_processor.process = AsyncMock(side_effect=_slow_process)
    return ProcessorWorker(processors=[mock_processor], progress_interval=0.001, **kwargs)

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

@pytest.mark.asyncio
async def test_heartbeat_gives_up_after_max_keepalive():
    """An unbounded heartbeat would hold a hung message alive forever.

    ChatGroq is built without a request timeout, so a hung socket blocks the extraction call
    indefinitely. max_deliver only engages on redelivery, and with max_ack_pending=1 the
    consumer would never be handed another message — it would go silent behind a live pod.
    """
    worker = _slow_worker(max_keepalive=0.005)
    msg = _js_msg()

    await worker.handle_message(msg)

    # The body runs ~0.05s, ten times the cap, so beating must have stopped well before it ended.
    assert 0 < msg.in_progress.await_count < 20
