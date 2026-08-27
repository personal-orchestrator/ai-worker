import inspect
from unittest.mock import AsyncMock, Mock

import pytest

from app.main import ACK_WAIT_SECONDS, MAX_ACK_PENDING, MAX_DELIVER, Application
from app.workers.processor import ProcessorWorker


@pytest.mark.asyncio
async def test_subscribe_requests_explicit_consumer_config():
    """A newly created consumer must not be left on server defaults.

    Note this covers the creation path only: nats-py discards the config when the durable
    already exists, which is why the README carries a one-off `nats consumer edit`.
    """
    app = Application()
    js = AsyncMock()
    processor_worker = Mock()
    processor_worker.handle_message = AsyncMock()

    await app._subscribe_consumers(js, processor_worker)

    js.subscribe.assert_awaited_once()
    config = js.subscribe.await_args.kwargs["config"]

    assert config.ack_wait == ACK_WAIT_SECONDS
    assert config.max_deliver == MAX_DELIVER
    assert config.max_ack_pending == MAX_ACK_PENDING


def test_heartbeat_outpaces_the_server_default_ack_wait():
    """The heartbeat has to beat the 30s server default, not just our requested ack_wait.

    Until the one-off `nats consumer edit` runs, the consumer still holds ack_wait=30s. An
    interval sized against ACK_WAIT_SECONDS alone would send its first +WPI after the message
    had already been redelivered.
    """
    progress_interval = inspect.signature(ProcessorWorker).parameters["progress_interval"].default

    assert progress_interval < 30, "must beat the NATS server default ack_wait"
    assert progress_interval < ACK_WAIT_SECONDS
