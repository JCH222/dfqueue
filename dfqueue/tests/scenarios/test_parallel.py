import logging
import pytest

from . import add_row, change_row_value, create_queue_item

from dfqueue import QueuesHandler
from pandas import DataFrame

from typing import Tuple, Dict
from dfqueue import adding, scheduling, synchronized, assign_dataframe
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

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
    @scheduling(queue_name=queue_name)
    @adding(queue_item_creation_function=create_queue_item, other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def parallel_add_row(dataframe: DataFrame, index: str, columns_dict: dict) -> Tuple[str, Dict]:
        return add_row(dataframe, index, columns_dict)

    @synchronized(queue_name=queue_name)
    @adding(queue_item_creation_function=create_queue_item, other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def parallel_change_row_value(dataframe: DataFrame, index: str, new_columns_dict: dict) -> Tuple[str, Dict]:
        return change_row_value(dataframe, index, new_columns_dict)

    def thread_adding(operation_number: int, dataframe: DataFrame):
        for _ in range(operation_number):
            parallel_add_row(dataframe, str(uuid4()), {'A': str(uuid4()), 'B': str(uuid4()), 'C': str(uuid4()),
                                                       'D': str(uuid4())})

    dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
    assign_dataframe(dataframe, 1000, selected_columns, queue_name)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(thread_adding, 5000, dataframe)
        future_b = executor.submit(thread_adding, 5000, dataframe)
        future_a.result()
        future_b.result()

    len(dataframe) == 1000
    len(QueuesHandler()._QueuesHandler__queues[queue_name]) == 1000
