# coding: utf8

from collections import deque
# noinspection PyPackageRequirements
import pytest
# noinspection PyPackageRequirements
from numpy import array
from pandas import DataFrame
from dfqueue import assign_dataframe
# noinspection PyProtectedMember
from dfqueue.core.dfqueue import QueuesHandler, QueueHandlerItem
from datetime import datetime


@pytest.mark.parametrize("queue_name", [
    None,
    "TEST"
])
@pytest.mark.parametrize("dataframe,max_size,selected_columns", [
    (DataFrame(columns=['A', 'B', 'C', 'D']), 2, ["D", "B"]),
    (DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 2, ["B"]),
    (DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 2, ["D", "B"]),
    (DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 10, ["B"]),
    (DataFrame(columns=['A', 'B', 'C', 'D']), 2, ["D", "B"]),
    (DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 2, ["B"]),
    (DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 2, ["D", "B"]),
    (DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 10, ["B"]),
    (DataFrame(array([[1, 2, 3, datetime.today()],
                      [5, 6, 7, datetime.today()],
                      [9, 10, 11, datetime.today()]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 2, ["B", "D"]),
    (DataFrame(array([[1, 2, 3, datetime.today()],
                      [5, 6, 7, datetime.today()],
                      [9, 10, 11, datetime.today()]]), index=['a1', 'a2', 'a3'],
               columns=['A', 'B', 'C', 'D']), 2, ["D"])
])
def test_assign_dataframe(queue_name, dataframe, max_size, selected_columns):
    real_queue_name = QueuesHandler().default_queue_name if queue_name is None else queue_name
    assign_dataframe(dataframe, max_size, selected_columns, queue_name=real_queue_name)
    queue_data = QueuesHandler()[real_queue_name]

    assert queue_data[QueueHandlerItem.MAX_SIZE] == max_size
    assert id(queue_data[QueueHandlerItem.DATAFRAME]) == id(dataframe)

    assert len(queue_data[QueueHandlerItem.QUEUE]) == len(dataframe)
    for queue_item in queue_data[QueueHandlerItem.QUEUE]:
        index = queue_item[0]
        values = queue_item[1]

        assert len(values) == len(selected_columns)

        for column_name, column_value in values.items():
            assert dataframe[column_name][index] == column_value


@pytest.mark.parametrize("queue_name", [
    None, "TEST_2"
])
def test_assign_none(queue_name):
    real_queue_name = QueuesHandler().default_queue_name if queue_name is None else queue_name
    assign_dataframe(DataFrame(columns=['A', 'B', 'C', 'D']), 2, ['B'], queue_name=real_queue_name)

    assign_dataframe(None, 1, [], queue_name=real_queue_name)
    queue_data = QueuesHandler()[real_queue_name]

    assert queue_data[QueueHandlerItem.MAX_SIZE] == 1
    assert queue_data[QueueHandlerItem.DATAFRAME] is None
    assert queue_data[QueueHandlerItem.QUEUE] == deque()
