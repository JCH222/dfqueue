from pandas import DataFrame, Series
from typing import Tuple, Dict, NoReturn, List, Any


def create_queue_item(result: tuple, selected_columns: List[Any]) -> Tuple[str, Dict]:
    return [(result[0], {column: result[1][column] for column in selected_columns})]


def create_queue_items(results: List[tuple], selected_columns: List[Any]) -> List[Tuple[str, Dict]]:
    return [(result[0], {column: result[1][column] for column in selected_columns}) for result in results]


def add_row(dataframe: DataFrame, index: str, columns_dict: dict) -> Tuple[str, Dict]:
    dataframe.at[index] = Series(data=columns_dict)
    return index, columns_dict


def change_row_value(dataframe: DataFrame, index: str, new_columns_dict: dict) -> Tuple[str, Dict]:
    dataframe.at[index] = Series(data=new_columns_dict)
    return index, new_columns_dict


def remove_row(dataframe: DataFrame, index: str) -> NoReturn:
    dataframe.drop([index], inplace=True)
