# coding: utf8

from collections import deque
# noinspection PyPackageRequirements
import pytest
from pandas import DataFrame
# noinspection PyProtectedMember
from dfqueue.core.dfqueue import QueuesHandler, QueueHandlerItem


def test_singleton():
    handler_a = QueuesHandler()
    handler_b = QueuesHandler()

    assert id(handler_a) != id(handler_b)
    assert id(handler_a._QueuesHandler__instance) == id(handler_b._QueuesHandler__instance)
    assert handler_a.default_queue_name == handler_b.default_queue_name


def test_valid_get_item():
    handler = QueuesHandler()
    default_queue_name = handler.default_queue_name
    queue_data = handler[default_queue_name]

    assert isinstance(queue_data, dict)
    assert len(queue_data) == len(QueueHandlerItem)
    assert all([item in queue_data for item in QueueHandlerItem])

    assert isinstance(queue_data[QueueHandlerItem.QUEUE], deque)
    assert queue_data[QueueHandlerItem.DATAFRAME] is None
    assert isinstance(queue_data[QueueHandlerItem.MAX_SIZE], int)


def test_invalid_get_item():
    handler = QueuesHandler()
    invalid_queue_name = "UNKNOWN"

    with pytest.raises(AssertionError):
        handler[invalid_queue_name]


@pytest.mark.parametrize("queue_iterable,dataframe,max_size", [
    (deque(), DataFrame(), 1),
    (deque((1, {"A": "a", "B": "b"})), DataFrame(), 1),
    (deque(), DataFrame(), 1234567890),
    ([], DataFrame(), 1),
    ([(1, {"A": "a", "B": "b"})], DataFrame(), 1),
    ([], DataFrame(), 1234567890)
])
def test_valid_set_item(queue_iterable, dataframe, max_size):
    handler = QueuesHandler()
    default_queue_name = handler.default_queue_name
    handler[default_queue_name] = {QueueHandlerItem.QUEUE: queue_iterable,
                                   QueueHandlerItem.DATAFRAME: dataframe,
                                   QueueHandlerItem.MAX_SIZE: max_size}
    queue_data = handler[default_queue_name]

    assert queue_data[QueueHandlerItem.QUEUE] == deque(queue_iterable)
    assert id(queue_data[QueueHandlerItem.DATAFRAME]) == id(dataframe)
    assert queue_data[QueueHandlerItem.MAX_SIZE] == max_size


def test_invalid_set_item():
    handler = QueuesHandler()
    default_queue_name = handler.default_queue_name

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: deque(),
                                       QueueHandlerItem.DATAFRAME: DataFrame()}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.MAX_SIZE: 1,
                                       QueueHandlerItem.DATAFRAME: DataFrame()}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.MAX_SIZE: 1,
                                       QueueHandlerItem.QUEUE: deque()}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: deque(),
                                       QueueHandlerItem.DATAFRAME: DataFrame(),
                                       QueueHandlerItem.MAX_SIZE: None}

    with pytest.raises(TypeError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: None,
                                       QueueHandlerItem.DATAFRAME: DataFrame(),
                                       QueueHandlerItem.MAX_SIZE: 1}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: deque(),
                                       QueueHandlerItem.DATAFRAME: "UNKNOWN",
                                       QueueHandlerItem.MAX_SIZE: 1}
