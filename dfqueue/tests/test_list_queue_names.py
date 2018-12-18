# coding: utf8

from uuid import uuid4
# noinspection PyPackageRequirements
from numpy import array
from pandas import DataFrame
from dfqueue import assign_dataframe, list_queue_names


def test_list_queue_names():
    dataframe_a = DataFrame(array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]),
                            index=['a1', 'a2', 'a3'], columns=['A', 'B', 'C', 'D'])
    dataframe_b = DataFrame(array([[13, 14, 15], [17, 18, 19]]), index=['a4', 'a5'],
                            columns=['E', 'F', 'G'])

    initial_size = len(list_queue_names())

    assert len(list_queue_names()) == initial_size

    queue_name_1 = str(uuid4())
    assign_dataframe(dataframe_b, 2, 'E', queue_name=queue_name_1)
    queue_names = list_queue_names()
    assert len(queue_names) == initial_size + 1
    assert queue_name_1 in queue_names

    queue_name_2 = str(uuid4())
    assign_dataframe(dataframe_a, 10, 'D', queue_name= queue_name_2)
    queue_names = list_queue_names()
    assert len(queue_names) == initial_size + 2
    assert all(queue_name in queue_names for queue_name in [queue_name_1, queue_name_2])
