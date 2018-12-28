# coding: utf8

from collections import deque
# noinspection PyPackageRequirements
import pytest
from pandas import DataFrame
# noinspection PyProtectedMember
from dfqueue.core.dfqueue import QueuesHandler, QueueHandlerItem, QueueBehaviour
from collections import Counter


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


@pytest.mark.parametrize("queue_iterable,dataframe,max_size,counter,behaviour", [
    (deque(), DataFrame(), 1, Counter(), QueueBehaviour.LAST_ITEM),
    (deque((1, {"A": "a", "B": "b"})), DataFrame(), 1, {1: Counter({"A": 1, "B": 1})},
     QueueBehaviour.ALL_ITEMS),
    (deque(), DataFrame(), 1234567890, {}, QueueBehaviour.LAST_ITEM),
    ([], DataFrame(), 1, {}, QueueBehaviour.ALL_ITEMS),
    ([(1, {"A": "a", "B": "b"})], DataFrame(), 1, {1: Counter({"A": 1, "B": 1})},
     QueueBehaviour.ALL_ITEMS),
    ([], DataFrame(), 1234567890, Counter(), QueueBehaviour.LAST_ITEM)
])
def test_valid_set_item(queue_iterable, dataframe, max_size, counter, behaviour):
    handler = QueuesHandler()
    default_queue_name = handler.default_queue_name
    handler[default_queue_name] = {QueueHandlerItem.QUEUE: queue_iterable,
                                   QueueHandlerItem.COUNTER: counter,
                                   QueueHandlerItem.DATAFRAME: dataframe,
                                   QueueHandlerItem.MAX_SIZE: max_size,
                                   QueueHandlerItem.BEHAVIOUR: behaviour}
    queue_data = handler[default_queue_name]

    assert queue_data[QueueHandlerItem.QUEUE] == deque(queue_iterable)
    assert queue_data[QueueHandlerItem.COUNTER] == counter
    assert id(queue_data[QueueHandlerItem.DATAFRAME]) == id(dataframe)
    assert queue_data[QueueHandlerItem.MAX_SIZE] == max_size
    assert queue_data[QueueHandlerItem.BEHAVIOUR] == behaviour


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
                                       QueueHandlerItem.COUNTER: dict()}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.MAX_SIZE: 1,
                                       QueueHandlerItem.COUNTER: dict()}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.DATAFRAME: DataFrame(),
                                       QueueHandlerItem.COUNTER: dict()}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: deque(),
                                       QueueHandlerItem.DATAFRAME: DataFrame(),
                                       QueueHandlerItem.MAX_SIZE: None}

    with pytest.raises(TypeError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: None,
                                       QueueHandlerItem.COUNTER: dict(),
                                       QueueHandlerItem.DATAFRAME: DataFrame(),
                                       QueueHandlerItem.MAX_SIZE: 1,
                                       QueueHandlerItem.BEHAVIOUR: QueueBehaviour.LAST_ITEM}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: deque(),
                                       QueueHandlerItem.COUNTER: dict(),
                                       QueueHandlerItem.DATAFRAME: "UNKNOWN",
                                       QueueHandlerItem.MAX_SIZE: 1,
                                       QueueHandlerItem.BEHAVIOUR: QueueBehaviour.LAST_ITEM}

    with pytest.raises(AssertionError):
        handler[default_queue_name] = {QueueHandlerItem.QUEUE: deque(),
                                       QueueHandlerItem.COUNTER: dict(),
                                       QueueHandlerItem.DATAFRAME: DataFrame(),
                                       QueueHandlerItem.MAX_SIZE: 1,
                                       QueueHandlerItem.BEHAVIOUR: "UNKNOWN"}
