import logging

from dfqueue import *
from pandas import DataFrame, Series


def create_queue_item(result: tuple):
    return result[0], {"B": result[1]["B"]}


@scheduling()
@adding(queue_item_creation_function=create_queue_item)
def add_row(dataframe: DataFrame, index: str, columns_dict: dict):
    dataframe.loc[index] = Series(data=columns_dict)
    return index, columns_dict


logging.getLogger().setLevel("DEBUG")


dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
assign_dataframe(dataframe, 3)

add_row(dataframe, "1", {'A': 1.0, 'B': 2.0, 'C': 3.0, 'D': 4.0})
add_row(dataframe, "2", {'A': 5.0, 'B': 6.0, 'C': 7.0, 'D': 8.0})
add_row(dataframe, "3", {'A': 9.0, 'B': 10.0, 'C': 11.0, 'D': 12.0})
add_row(dataframe, "4", {'A': 13.0, 'B': 14.0, 'C': 15.0, 'D': 16.0})
add_row(dataframe, "5", {'A': 17.0, 'B': 18.0, 'C': 19.0, 'D': 20.0})
add_row(dataframe, "6", {'A': 21.0, 'B': 22.0, 'C': 23.0, 'D': 24.0})