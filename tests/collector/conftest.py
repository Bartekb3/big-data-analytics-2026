import threading

import pytest

from tests.collector.fakes import FakeClock


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def stop():
    return threading.Event()
