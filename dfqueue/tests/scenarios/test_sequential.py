# coding: utf8

import time
from uuid import uuid4
import logging
from collections import deque
from typing import Tuple, Dict, List, NoReturn
# noinspection PyPackageRequirements
import numpy
from pandas import DataFrame, Series, MultiIndex
# noinspection PyPackageRequirements
import pytest
# noinspection PyProtectedMember
from dfqueue.core.dfqueue import QueuesHandler
from dfqueue import adding, managing, assign_dataframe, QueueBehaviour
from . import add_row, change_row_value, create_queue_item, remove_row, create_queue_items

logging.getLogger().setLevel("DEBUG")


@pytest.mark.parametrize("queue_name", [
    None,
    "TEST_1",
    "TEST_2"
])
@pytest.mark.parametrize("columns, selected_columns", [
    (['A', 'B', 'C', 'D'], ['B', 'D']),
    (MultiIndex.from_tuples(list(zip(*[['A', 'A', 'B', 'B'], ['1', '2', '1', '2']])),
                            names=['first', 'second']), [('A', '1'), ('B', '2')]),
    (['A', 'B', 'C', 'D'], ['B'])
])
def test_sequential_1(queue_name, columns, selected_columns):
    queue_name = queue_name if queue_name is not None else QueuesHandler().default_queue_name

    @managing(queue_name=queue_name)
    @adding(queue_items_creation_function=create_queue_item,
            other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def sequential_add_row(dataframe: DataFrame,
                           index: str,
                           columns_dict: dict) -> Tuple[str, Dict]:
        return add_row(dataframe, index, columns_dict)

    @adding(queue_items_creation_function=create_queue_item,
            other_args={"selected_columns": selected_columns},
            queue_name=queue_name)
    def sequential_change_row_value(dataframe: DataFrame,
                                    index: str,
                                    new_columns_dict: dict) -> Tuple[str, Dict]:
        return change_row_value(dataframe, index, new_columns_dict)

    dataframe = DataFrame(columns=columns)
    assign_dataframe(dataframe, 2, selected_columns, queue_name)

    assert dataframe.empty
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque()

    columns_dict_row_1 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "1", columns_dict_row_1)
    assert len(dataframe) == 1
    result_row_1 = ("1", {selected_column: columns_dict_row_1[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_1])

    columns_dict_row_2 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "2", columns_dict_row_2)
    assert len(dataframe) == 2
    result_row_2 = ("2", {selected_column: columns_dict_row_2[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_1,
                                                                        result_row_2])

    columns_dict_row_3 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "3", columns_dict_row_3)
    assert len(dataframe) == 2
    result_row_3 = ("3", {selected_column: columns_dict_row_3[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_2,
                                                                        result_row_3])

    remove_row(dataframe, "3")
    assert len(dataframe) == 1
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_2,
                                                                        result_row_3])

    remove_row(dataframe, "2")
    assert dataframe.empty
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_2,
                                                                        result_row_3])

    columns_dict_row_4 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "4", columns_dict_row_4)
    columns_dict_row_5 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "5", columns_dict_row_5)
    assert len(dataframe) == 2
    result_row_4 = ("4", {selected_column: columns_dict_row_4[selected_column]
                          for selected_column in selected_columns})
    result_row_5 = ("5", {selected_column: columns_dict_row_5[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_2,
                                                                        result_row_3,
                                                                        result_row_4,
                                                                        result_row_5])

    columns_dict_row_6 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "6", columns_dict_row_6)
    assert len(dataframe) == 2
    result_row_6 = ("6", {selected_column: columns_dict_row_6[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_5,
                                                                        result_row_6])

    new_columns_dict_row_5 = {column: str(uuid4()) for column in columns}
    sequential_change_row_value(dataframe, "5", new_columns_dict_row_5)
    assert len(dataframe) == 2
    new_result_row_5 = ("5", {selected_column: new_columns_dict_row_5[selected_column]
                              for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_5,
                                                                        result_row_6,
                                                                        new_result_row_5])

    columns_dict_row_7 = {column: str(uuid4()) for column in columns}
    sequential_add_row(dataframe, "7", columns_dict_row_7)
    assert len(dataframe) == 2
    result_row_7 = ("7", {selected_column: columns_dict_row_7[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([new_result_row_5,
                                                                        result_row_7])

    ################################################################################################

    for behaviour in [QueueBehaviour.ALL_ITEMS, QueueBehaviour.LAST_ITEM]:
        values_row_1 = [str(uuid4()) for _ in range(len(columns))]
        values_row_2 = [str(uuid4()) for _ in range(len(columns))]
        dataframe_2 = DataFrame(data=[values_row_1, values_row_2], index=['1', '2'],
                                columns=columns)
        assign_dataframe(dataframe_2, 2, selected_columns, queue_name,
                         queue_behaviour=behaviour)
        assert len(dataframe_2) == 2
        assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque(
            [
                ('1', {column: values_row_1[list(columns).index(column)] for column in
                       selected_columns}),
                ('2', {column: values_row_2[list(columns).index(column)] for column in
                       selected_columns})
            ])

        new_values_row_1 = [str(uuid4()) for _ in range(len(columns))]
        sequential_change_row_value(dataframe_2, '1', {column: new_values_row_1[i] for i, column in
                                                       enumerate(list(columns))})
        assert len(dataframe_2) == 2
        assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque(
            [
                ('1', {column: values_row_1[list(columns).index(column)] for column in
                       selected_columns}),
                ('2', {column: values_row_2[list(columns).index(column)] for column in
                       selected_columns}),
                ('1', {column: new_values_row_1[list(columns).index(column)] for column in
                       selected_columns})
            ])

        sequential_change_row_value(dataframe_2, '1', {column: values_row_1[i] for i, column in
                                                       enumerate(list(columns))})
        assert len(dataframe_2) == 2
        assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque(
            [
                ('1', {column: values_row_1[list(columns).index(column)] for column in
                       selected_columns}),
                ('2', {column: values_row_2[list(columns).index(column)] for column in
                       selected_columns}),
                ('1', {column: new_values_row_1[list(columns).index(column)] for column in
                       selected_columns}),
                ('1', {column: values_row_1[list(columns).index(column)] for column in
                       selected_columns})
            ])

        values_row_3 = [str(uuid4()) for _ in range(len(columns))]
        sequential_add_row(dataframe_2, '3', {column: values_row_3[i] for i, column
                                              in enumerate(columns)})
        assert len(dataframe_2) == 2
        if behaviour == QueueBehaviour.ALL_ITEMS:
            assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque(
                [
                    ('2', {column: values_row_2[list(columns).index(column)] for column in
                           selected_columns}),
                    ('1', {column: new_values_row_1[list(columns).index(column)] for column in
                           selected_columns}),
                    ('1', {column: values_row_1[list(columns).index(column)] for column in
                           selected_columns}),
                    ('3', {column: values_row_3[list(columns).index(column)] for column in
                           selected_columns})
                ])
            assert all(label in dataframe_2.index for label in ['2', '3'])
        elif behaviour == QueueBehaviour.LAST_ITEM:
            assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque(
                [
                    ('1', {column: new_values_row_1[list(columns).index(column)] for column in
                           selected_columns}),
                    ('1', {column: values_row_1[list(columns).index(column)] for column in
                           selected_columns}),
                    ('3', {column: values_row_3[list(columns).index(column)] for column in
                           selected_columns})
                ])
            assert all(label in dataframe_2.index for label in ['1', '3'])
        else:
            assert False, "Missing test for the behaviour {}".format(behaviour)


@pytest.mark.parametrize("queue_name", [
    "TEST_3"
])
@pytest.mark.parametrize("columns, selected_columns", [
    (['A', 'B', 'C', 'D'], ['A', 'B']),
    (MultiIndex.from_tuples(list(zip(*[['A', 'A', 'B', 'B'], ['1', '2', '1', '2']])),
                            names=['first', 'second']), [('A', '1'), ('B', '2')])
])
def test_sequential_2(queue_name, columns, selected_columns):
    @managing(queue_name=queue_name)
    @adding(queue_items_creation_function=create_queue_items,
            other_args={"selected_columns": selected_columns}, queue_name=queue_name)
    def sequential_add_rows(dataframe: DataFrame,
                            indexes: List[str],
                            columns_dicts: List[dict]) -> List[Tuple[str, Dict]]:
        assert len(indexes) == len(columns_dicts)
        result = list()
        for index, columns_dict in zip(indexes, columns_dicts):
            result.append(add_row(dataframe, index, columns_dict))
        return result

    dataframe = DataFrame(columns=columns)
    assign_dataframe(dataframe, 5, selected_columns, queue_name)
    assert dataframe.empty
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque()

    columns_dict_row_1 = {column: str(uuid4()) for column in columns}
    sequential_add_rows(dataframe, ["1"], [columns_dict_row_1])
    assert len(dataframe) == 1
    result_row_1 = ("1", {selected_column: columns_dict_row_1[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([result_row_1])

    columns_dict_row_2 = {column: str(uuid4()) for column in columns}
    columns_dict_row_3 = {column: str(uuid4()) for column in columns}
    columns_dict_row_4 = {column: str(uuid4()) for column in columns}
    sequential_add_rows(dataframe,
                        ["2", "3", "4"],
                        [
                            columns_dict_row_2,
                            columns_dict_row_3,
                            columns_dict_row_4
                        ]
                        )
    assert len(dataframe) == 4
    result_row_2 = ("2", {selected_column: columns_dict_row_2[selected_column]
                          for selected_column in selected_columns})
    result_row_3 = ("3", {selected_column: columns_dict_row_3[selected_column]
                          for selected_column in selected_columns})
    result_row_4 = ("4", {selected_column: columns_dict_row_4[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([
        result_row_1,
        result_row_2,
        result_row_3,
        result_row_4
    ])

    columns_dict_row_5 = {column: str(uuid4()) for column in columns}
    columns_dict_row_6 = {column: str(uuid4()) for column in columns}
    columns_dict_row_7 = {column: str(uuid4()) for column in columns}
    sequential_add_rows(dataframe,
                        ["5", "6", "7"],
                        [
                            columns_dict_row_5,
                            columns_dict_row_6,
                            columns_dict_row_7
                        ]
                        )
    assert len(dataframe) == 5
    result_row_5 = ("5", {selected_column: columns_dict_row_5[selected_column]
                          for selected_column in selected_columns})
    result_row_6 = ("6", {selected_column: columns_dict_row_6[selected_column]
                          for selected_column in selected_columns})
    result_row_7 = ("7", {selected_column: columns_dict_row_7[selected_column]
                          for selected_column in selected_columns})
    assert QueuesHandler()._QueuesHandler__queues[queue_name] == deque([
        result_row_3,
        result_row_4,
        result_row_5,
        result_row_6,
        result_row_7
    ])


@pytest.mark.parametrize("queue_name, rows_nb, columns, dataframe_max_size", [
    ("TEST_4", 5000, ['A', 'B', 'C', 'D'], 5)
])
def test_massive_managing(queue_name, rows_nb, columns, dataframe_max_size):
    @managing(queue_name=queue_name)
    def manage() -> NoReturn:
        pass

    data = numpy.array(numpy.random.rand(rows_nb, len(columns)))
    index = numpy.array(numpy.arange(rows_nb))
    dataframe = DataFrame(data, index=index, columns=columns)

    assert len(dataframe) == rows_nb
    assign_dataframe(dataframe, dataframe_max_size, columns, queue_name)
    start = time.time()
    manage()
    end = time.time()
    assert len(dataframe) == dataframe_max_size

    print("\n{} managing execution time : {} s".format(queue_name, end-start))


@pytest.mark.parametrize("queue_name, execution_nb, columns, dataframe_max_size, chunk_size", [
    ("TEST_5", 5000, ['A', 'B', 'C', 'D'], 5, 1),
    ("TEST_6", 5000, ['A', 'B', 'C', 'D'], 5, 5),
    ("TEST_7", 5000, ['A', 'B', 'C', 'D'], 5, 10),
    ("TEST_8", 5000, ['A', 'B', 'C', 'D'], 5, 100),
    ("TEST_9", 5000, ['A', 'B', 'C', 'D'], 5, 1000)
])
def test_massive_managing_2(queue_name, execution_nb, columns, dataframe_max_size, chunk_size):
    dataframe = DataFrame(columns=columns)
    assign_dataframe(dataframe, dataframe_max_size, columns, queue_name)

    @managing(queue_name=queue_name)
    @adding(queue_name=queue_name)
    def add_rows(rows_nb: int) -> List[Tuple[str, Dict]]:
        result = list()
        for _ in range(rows_nb):
            index = numpy.random.rand(1)[0]
            columns_dict = dict()
            for column in columns:
                columns_dict[column] = numpy.random.rand(1)[0]
            dataframe.at[index] = Series(data=columns_dict)
            result.append((index, columns_dict))
        return result

    assert dataframe.empty
    start = time.time()
    for i in range(int(execution_nb/chunk_size)):
        add_rows(chunk_size)
        assert len(dataframe) == chunk_size*(i+1) if chunk_size*(i+1) <= dataframe_max_size \
            else dataframe_max_size
    end = time.time()
    assert len(dataframe) == dataframe_max_size

    print("\n{} managing execution time : {} s".format(queue_name, end-start))
