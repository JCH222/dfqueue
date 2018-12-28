# coding: utf8

from uuid import uuid4
from collections import Counter, deque
# noinspection PyPackageRequirements
from numpy import array
from pandas import DataFrame
# noinspection PyPackageRequirements
import pytest
from dfqueue import assign_dataframe, get_info_provider


def test_queue_info_provider():
    dataframe = DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]),
                          index=['a1', 'a2', 'a3'], columns=['A', 'B', 'C', 'D'])
    queue_name = str(uuid4())
    max_size = 4
    assign_dataframe(dataframe, max_size, selected_columns=['A', 'D'], queue_name=queue_name)

    provider = get_info_provider(queue_name)

    assert id(provider.assigned_dataframe) == id(dataframe)
    assert provider.max_size == max_size
    assert provider.queue_name == queue_name
    assert provider.is_default_queue is False
    assert provider.queue[0:2] == (('a1', {'A': 1, 'D': 4}), ('a2', {'A': 5, 'D': 8}))
    assert provider.queue[-1] == ('a3', {'A': 9, 'D': 12})
    assert provider.queue[0] == ('a1', {'A': 1, 'D': 4})
    assert provider.queue == deque((('a1', {'A': 1, 'D': 4}), ('a2', {'A': 5, 'D': 8}),
                                    ('a3', {'A': 9, 'D': 12})))
    assert provider.queue != deque((('a1', {'A': 1, 'D': 4}),))
    assert ('a1', {'A': 1, 'D': 4}) in provider.queue
    assert ('a5', {'A': 10, 'D': 40}) not in provider.queue
    assert provider.counter['a1'] == Counter({frozenset(['A', 'D']): 1})
    assert provider.counter['a1'] != Counter({frozenset(['A', 'D']): 10})
    assert provider.counter == {'a1': Counter({frozenset(['A', 'D']): 1}),
                                'a2': Counter({frozenset(['A', 'D']): 1}),
                                'a3': Counter({frozenset(['A', 'D']): 1})}
    assert provider.counter != {'a1': Counter({frozenset(['A', 'D']): 10}),
                                'a2': Counter({frozenset(['A', 'D']): 10}),
                                'a3': Counter({frozenset(['A', 'D']): 10})}
    assert set(provider.counter.keys()) == set(['a1', 'a2', 'a3'])
    assert set(provider.counter.keys()) != set(['a4'])
    assert list(provider.counter.values()) == [Counter({frozenset(['A', 'D']): 1}),
                                               Counter({frozenset(['A', 'D']): 1}),
                                               Counter({frozenset(['A', 'D']): 1})]
    assert list(provider.counter.values()) != [Counter({frozenset(['A', 'D']): 10}),
                                               Counter({frozenset(['A', 'D']): 10}),
                                               Counter({frozenset(['A', 'D']): 10})]
    assert list(provider.counter.items()) == [('a1', Counter({frozenset(['A', 'D']): 1})),
                                              ('a2', Counter({frozenset(['A', 'D']): 1})),
                                              ('a3', Counter({frozenset(['A', 'D']): 1}))]
    assert list(provider.counter.items()) != [('a4', Counter({frozenset(['A', 'D']): 10})),
                                              ('a5', Counter({frozenset(['A', 'D']): 10})),
                                              ('a6', Counter({frozenset(['A', 'D']): 10}))]

    dataframe_2 = DataFrame(array([[10, 20, 30, 40], [50, 60, 70, 80], [90, 100, 110, 120]]),
                            index=['a1', 'a2', 'a3'], columns=['A', 'B', 'C', 'D'])
    max_size_2 = 100
    assign_dataframe(dataframe_2, max_size_2, selected_columns=['B'], queue_name=queue_name)

    assert id(provider.assigned_dataframe) == id(dataframe_2)
    assert provider.max_size == max_size_2
    assert provider.queue_name == queue_name
    assert provider.is_default_queue is False
    assert provider.queue[0:2] == (('a1', {'B': 20}), ('a2', {'B': 60}))
    assert provider.queue[-1] == ('a3', {'B': 100})
    assert provider.queue[0] == ('a1', {'B': 20})
    assert provider.queue == deque((('a1', {'B': 20}), ('a2', {'B': 60}), ('a3', {'B': 100})))
    assert provider.queue != deque((('a1', {'B': 20}),))
    assert ('a1', {'B': 20}) in provider.queue
    assert ('a5', {'B': 200}) not in provider.queue


def test_queue_info_provider_default_queue():
    provider = get_info_provider()
    assert provider.is_default_queue is True


def test_queue_info_provider_error():
    provider = get_info_provider()

    with pytest.raises(ValueError):
        provider.queue['a']

    with pytest.raises(AttributeError):
        provider.is_default_queue = False

    with pytest.raises(AttributeError):
        provider.queue_name = 'Another Name'

    with pytest.raises(AttributeError):
        provider.max_size = 2

    with pytest.raises(AttributeError):
        provider.assigned_dataframe = DataFrame()

    with pytest.raises(AssertionError):
        get_info_provider(str(uuid4()))
