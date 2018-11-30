# coding: utf8

import logging
import pytest

from . import add_row, change_row_value, create_queue_item, remove_row, create_queue_items

# noinspection PyProtectedMember
from dfqueue.core.dfqueue import QueuesHandler

from collections import deque
from pandas import DataFrame

from typing import Tuple, Dict, List
from dfqueue import adding, managing, assign_dataframe

logging.getLogger().setLevel("DEBUG")


@pytest.mark.parametrize("queue_name", [
    None,
    "TEST_1",
    "TEST_2"
])
def test_sequential_1(queue_name):
    selected_columns = ["B", "D"]
    queue_name = queue_name if queue_name is not None else QueuesHandler().default_queue_name

    @managing(queue_name=queue_name)
    @adding(queue_items_creation_function=create_queue_item, other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def sequential_add_row(dataframe: DataFrame, index: str, columns_dict: dict) -> Tuple[str, Dict]:
        return add_row(dataframe, index, columns_dict)

    @adding(queue_items_creation_function=create_queue_item, other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def sequential_change_row_value(dataframe: DataFrame, index: str, new_columns_dict: dict) -> Tuple[str, Dict]:
        return change_row_value(dataframe, index, new_columns_dict)

    dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
    assign_dataframe(dataframe, 2, selected_columns, queue_name)

    assert len(dataframe) == 0
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque()

    sequential_add_row(dataframe, "1", {'A': 1.0, 'B': 2.0, 'C': 3.0, 'D': 4.0})
    assert len(dataframe) == 1
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("1", {'B': 2.0, 'D': 4.0})])

    sequential_add_row(dataframe, "2", {'A': 5.0, 'B': 6.0, 'C': 7.0, 'D': 8.0})
    assert len(dataframe) == 2
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("1", {'B': 2.0, 'D': 4.0}),
                                                                        ("2", {'B': 6.0, 'D': 8.0})])

    sequential_add_row(dataframe, "3", {'A': 9.0, 'B': 10.0, 'C': 11.0, 'D': 12.0})
    assert len(dataframe) == 2
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("2", {'B': 6.0, 'D': 8.0}),
                                                                        ("3", {'B': 10.0, 'D': 12.0})])

    remove_row(dataframe, "3")
    assert len(dataframe) == 1
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("2", {'B': 6.0, 'D': 8.0}),
                                                                        ("3", {'B': 10.0, 'D': 12.0})])

    remove_row(dataframe, "2")
    assert len(dataframe) == 0
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("2", {'B': 6.0, 'D': 8.0}),
                                                                        ("3", {'B': 10.0, 'D': 12.0})])

    sequential_add_row(dataframe, "4", {'A': 13.0, 'B': 14.0, 'C': 15.0, 'D': 16.0})
    sequential_add_row(dataframe, "5", {'A': 17.0, 'B': 18.0, 'C': 19.0, 'D': 20.0})
    assert len(dataframe) == 2
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("2", {'B': 6.0, 'D': 8.0}),
                                                                        ("3", {'B': 10.0, 'D': 12.0}),
                                                                        ("4", {'B': 14.0, 'D': 16.0}),
                                                                        ("5", {'B': 18.0, 'D': 20.0})])

    sequential_add_row(dataframe, "6", {'A': 21.0, 'B': 22.0, 'C': 23.0, 'D': 24.0})
    assert len(dataframe) == 2
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("5", {'B': 18.0, 'D': 20.0}),
                                                                        ("6", {'B': 22.0, 'D': 24.0})])

    sequential_change_row_value(dataframe, "5", {'A': 25.0, 'B': 26.0, 'C': 27.0, 'D': 28.0})
    assert len(dataframe) == 2
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("5", {'B': 18.0, 'D': 20.0}),
                                                                        ("6", {'B': 22.0, 'D': 24.0}),
                                                                        ("5", {'B': 26.0, 'D': 28.0})])

    sequential_add_row(dataframe, "7", {'A': 29.0, 'B': 30.0, 'C': 31.0, 'D': 32.0})
    assert len(dataframe) == 2
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("5", {'B': 26.0, 'D': 28.0}),
                                                                        ("7", {'B': 30.0, 'D': 32.0})])


@pytest.mark.parametrize("queue_name", [
    "TEST_3"
])
def test_sequential_2(queue_name):
    selected_columns_a = ["A", "B"]

    @managing(queue_name=queue_name)
    @adding(queue_items_creation_function=create_queue_items, other_args={"selected_columns": selected_columns_a}, queue_name=queue_name)
    def sequential_add_rows(dataframe: DataFrame, indexes: List[str], columns_dicts: List[dict]) -> List[Tuple[str, Dict]]:
        assert len(indexes) == len(columns_dicts)
        result = list()
        for index, columns_dict in zip(indexes, columns_dicts):
            result.append(add_row(dataframe, index, columns_dict))
        return result

    dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
    assign_dataframe(dataframe, 5, selected_columns_a, queue_name)

    assert len(dataframe) == 0
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque()

    sequential_add_rows(dataframe, ["1"], [{'A': 1.0, 'B': 2.0, 'C': 3.0, 'D': 4.0}])
    assert len(dataframe) == 1
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([("1", {'A': 1.0, 'B': 2.0})])

    sequential_add_rows(dataframe,
                        ["2", "3", "4"],
                        [
                            {'A': 5.0, 'B': 6.0, 'C': 7.0, 'D': 8.0},
                            {'A': 9.0, 'B': 10.0, 'C': 11.0, 'D': 12.0},
                            {'A': 13.0, 'B': 14.0, 'C': 15.0, 'D': 16.0}
                        ]
                        )
    assert len(dataframe) == 4
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([
        ("1", {'A': 1.0, 'B': 2.0}),
        ("2", {'A': 5.0, 'B': 6.0}),
        ("3", {'A': 9.0, 'B': 10.0}),
        ("4", {'A': 13.0, 'B': 14.0})
    ])

    sequential_add_rows(dataframe,
                        ["5", "6", "7"],
                        [
                            {'A': 17.0, 'B': 18.0, 'C': 19.0, 'D': 20.0},
                            {'A': 21.0, 'B': 22.0, 'C': 23.0, 'D': 24.0},
                            {'A': 25.0, 'B': 26.0, 'C': 27.0, 'D': 28.0}
                        ]
                        )
    assert len(dataframe) == 5
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([
        ("3", {'A': 9.0, 'B': 10.0}),
        ("4", {'A': 13.0, 'B': 14.0}),
        ("5", {'A': 17.0, 'B': 18.0}),
        ("6", {'A': 21.0, 'B': 22.0}),
        ("7", {'A': 25.0, 'B': 26.0})
    ])
