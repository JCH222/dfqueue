# coding: utf8

import logging
from typing import Tuple, Dict, Callable
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from random import randint
# noinspection PyPackageRequirements
import pytest
from pandas import DataFrame
# noinspection PyProtectedMember
from dfqueue.core.dfqueue import QueuesHandler
from dfqueue import adding, managing, synchronized, assign_dataframe
from . import add_row, change_row_value, create_queue_item

logging.getLogger().setLevel("DEBUG")


@pytest.mark.parametrize("queue_name", [
    None,
    'TEST_1',
    'TEST_2'
])
def test_parallel_1(queue_name):
    selected_columns = ["A", "C"]
    queue_name = queue_name if queue_name is not None else QueuesHandler().default_queue_name

    @synchronized(queue_name=queue_name)
    @managing(queue_name=queue_name)
    @adding(queue_items_creation_function=create_queue_item,
            other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def parallel_add_row(dataframe: DataFrame, index: str, columns_dict: dict) -> Tuple[str, Dict]:
        return add_row(dataframe, index, columns_dict)

    @synchronized(queue_name=queue_name)
    @adding(queue_items_creation_function=create_queue_item,
            other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def parallel_change_row_value(dataframe: DataFrame,
                                  index: str,
                                  new_columns_dict: dict) -> Tuple[str, Dict]:
        return change_row_value(dataframe, index, new_columns_dict)

    def thread_adding(operation_number: int, dataframe: DataFrame):
        for _ in range(operation_number):
            parallel_add_row(dataframe, str(uuid4()), {'A': str(uuid4()), 'B': str(uuid4()),
                                                       'C': str(uuid4()), 'D': str(uuid4())})

    def thread_change(operation_number: int, dataframe: DataFrame):
        for _ in range(operation_number):
            parallel_change_row_value(dataframe,
                                      dataframe.index.values[randint(0, len(dataframe)-1)],
                                      {'A': str(uuid4()), 'B': str(uuid4()), 'C': str(uuid4()),
                                       'D': str(uuid4())})

    dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
    assign_dataframe(dataframe, 1000, selected_columns, queue_name)

    assert dataframe.empty

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(thread_adding, 4000, dataframe)
        future_b = executor.submit(thread_adding, 4000, dataframe)
        future_c = executor.submit(thread_change, 1000, dataframe)
        future_a.result()
        future_b.result()
        future_c.result()

    assert len(dataframe) == 1000


# Two queues share the same dataframe
def test_parallel_2():
    selected_columns_a = ["A", "B"]
    selected_columns_b = ["C", "D"]

    @synchronized(queue_name='TEST_3')
    @managing(queue_name='TEST_3')
    @adding(queue_items_creation_function=create_queue_item,
            other_args={"selected_columns": selected_columns_a},
            queue_name='TEST_3')
    def parallel_add_row_a(dataframe: DataFrame,
                           index: str,
                           columns_dict: dict) -> Tuple[str, Dict]:
        return add_row(dataframe, index, columns_dict)

    @synchronized(queue_name='TEST_4')
    @managing(queue_name='TEST_4')
    @adding(queue_items_creation_function=create_queue_item,
            other_args={"selected_columns": selected_columns_b},
            queue_name='TEST_4')
    def parallel_add_row_b(dataframe: DataFrame,
                           index: str,
                           columns_dict: dict) -> Tuple[str, Dict]:
        return add_row(dataframe, index, columns_dict)

    def thread_adding(operation_number: int, dataframe: DataFrame, adding_function: Callable):
        for _ in range(operation_number):
            adding_function(dataframe, str(uuid4()), {'A': str(uuid4()), 'B': str(uuid4()),
                                                      'C': str(uuid4()), 'D': str(uuid4())})

    dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
    assign_dataframe(dataframe, 1000, selected_columns_a, 'TEST_3')
    assign_dataframe(dataframe, 500, selected_columns_b, 'TEST_4')

    assert dataframe.empty

    # noinspection PyProtectedMember
    queue_handler_instance = QueuesHandler._QueuesHandler__instance
    assert id(QueuesHandler._QueuesHandler__instance.get_assigned_lock('TEST_3')) != \
           id(queue_handler_instance.get_assigned_lock(QueuesHandler().default_queue_name))
    assert id(QueuesHandler._QueuesHandler__instance.get_assigned_lock('TEST_4')) != \
           id(queue_handler_instance.get_assigned_lock(QueuesHandler().default_queue_name))
    assert id(QueuesHandler._QueuesHandler__instance.get_assigned_lock('TEST_3')) == \
           id(queue_handler_instance.get_assigned_lock('TEST_4'))

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(thread_adding, 4000, dataframe, parallel_add_row_a)
        future_b = executor.submit(thread_adding, 4000, dataframe, parallel_add_row_b)
        future_a.result()
        future_b.result()

    # We can't predict if dataframe's size will be 500 or 1000
    assert len(dataframe) in [500, 1000]
