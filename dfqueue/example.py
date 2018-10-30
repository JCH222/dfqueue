import logging

from dfqueue import *
from pandas import DataFrame, Series
from typing import Tuple, Dict, NoReturn


def create_queue_item(result: tuple) -> Tuple[str, Dict]:
    return result[0], {"B": result[1]["B"]}


@scheduling()
@adding(queue_item_creation_function=create_queue_item)
def add_row(dataframe: DataFrame, index: str, columns_dict: dict) -> Tuple[str, Dict]:
    dataframe.at[index] = Series(data=columns_dict)
    return index, columns_dict


@adding(queue_item_creation_function=create_queue_item)
def change_row_value(dataframe: DataFrame, index: str, new_columns_dict: dict) -> Tuple[str, Dict]:
    dataframe.at[index] = Series(data=new_columns_dict)
    return index, new_columns_dict


def remove_row(dataframe: DataFrame, index: str) -> NoReturn:
    dataframe.drop([index], inplace=True)


logging.getLogger().setLevel("DEBUG")


dataframe = DataFrame(columns=['A', 'B', 'C', 'D'])
assign_dataframe(dataframe, 3, ['B'])

add_row(dataframe, "1", {'A': 1.0, 'B': 2.0, 'C': 3.0, 'D': 4.0})
add_row(dataframe, "2", {'A': 5.0, 'B': 6.0, 'C': 7.0, 'D': 8.0})
change_row_value(dataframe, "2", {'A': 5.0, 'B': 60.0, 'C': 7.0, 'D': 8.0})
add_row(dataframe, "3", {'A': 9.0, 'B': 10.0, 'C': 11.0, 'D': 12.0})
add_row(dataframe, "4", {'A': 13.0, 'B': 14.0, 'C': 15.0, 'D': 16.0})
add_row(dataframe, "5", {'A': 17.0, 'B': 18.0, 'C': 19.0, 'D': 20.0})
add_row(dataframe, "6", {'A': 21.0, 'B': 22.0, 'C': 23.0, 'D': 24.0})
remove_row(dataframe, "6")
remove_row(dataframe, "5")
add_row(dataframe, "7", {'A': 25.0, 'B': 26.0, 'C': 27.0, 'D': 28.0})
