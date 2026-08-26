from unittest.mock import AsyncMock, Mock

import pytest

from app.config import settings
from app.main import Application


@pytest.mark.asyncio
async def test_subscribe_applies_consumer_config():
    """The consumer must not be created on server defaults — that is what caused the
    35,785 deliveries for 9,270 messages measured on 2026-08-26."""
    app = Application()
    js = AsyncMock()
    processor_worker = Mock()
    processor_worker.handle_message = AsyncMock()

    await app._subscribe_consumers(js, processor_worker)

    js.subscribe.assert_awaited_once()
    config = js.subscribe.await_args.kwargs["config"]

    assert config.ack_wait == settings.nats_ack_wait
    assert config.max_deliver == settings.nats_max_deliver
    assert config.max_ack_pending == settings.nats_max_ack_pending


def test_consumer_config_defaults_are_sane():
    """Guard the values themselves: the defaults are the whole point of the fix."""
    assert settings.nats_ack_wait >= 60
    assert settings.nats_max_deliver > 0
    assert settings.nats_max_ack_pending <= 2
    assert settings.nats_progress_interval < settings.nats_ack_wait
