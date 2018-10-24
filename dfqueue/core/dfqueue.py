import logging

from uuid import uuid4
from collections import deque
from enum import Enum
from pandas import DataFrame, Series
from typing import Union, Callable, Tuple, Any, NoReturn, Dict


class QueueHandlerItem(Enum):
    QUEUE = 0
    DATAFRAME = 1
    MAX_SIZE = 2


class QueuesHandler:
    # Singleton
    class __QueuesHandler:
        def __init__(self):
            # Define the default queue's name
            self.__default_queue_name = str(uuid4())
            # Define the default queue
            self.__queues = {self.__default_queue_name: deque()}
            self.__assigned_dataframes = {self.__default_queue_name: None}
            self.__assigned_dataframe_max_sizes = {self.__default_queue_name: 1000000}

        @property
        def default_queue_name(self) -> str:
            return self.__default_queue_name

        def __getitem__(self, queue_name: str) -> Dict[QueueHandlerItem, Any]:
            try:
                return {QueueHandlerItem.QUEUE: self.__queues[queue_name],
                        QueueHandlerItem.DATAFRAME: self.__assigned_dataframes[queue_name],
                        QueueHandlerItem.MAX_SIZE: self.__assigned_dataframe_max_sizes[queue_name]}
            except KeyError:
                raise KeyError("The queue '{}' doesn't exist".format(queue_name))

        def __setitem__(self, queue_name: str, items: dict) -> NoReturn:
            assert len(items) == len(QueueHandlerItem)
            assert all([True if item in items else False for item in QueueHandlerItem])
            self.__queues[queue_name] = deque(items[QueueHandlerItem.QUEUE])
            assert isinstance(items[QueueHandlerItem.DATAFRAME], DataFrame) or items[QueueHandlerItem.DATAFRAME] is None
            self.__assigned_dataframes[queue_name] = items[QueueHandlerItem.DATAFRAME]
            assert isinstance(items[QueueHandlerItem.MAX_SIZE], int)
            self.__assigned_dataframe_max_sizes[queue_name] = items[QueueHandlerItem.MAX_SIZE]

    __instance = None

    def __init__(self):
        if not QueuesHandler.__instance:
            QueuesHandler.__instance = QueuesHandler.__QueuesHandler()

    def __getattr__(self, item) -> Any:
        return getattr(self.__instance, item)

    def __getitem__(self, queue_name: str) -> Dict[QueueHandlerItem, Any]:
        return self.__instance[queue_name]

    def __setitem__(self, queue_name: str, items: Dict[QueueHandlerItem, Any]) -> NoReturn:
        self.__instance[queue_name] = items


def __create_logging_message(message: str) -> str:
    line_decorator = '| '
    lines = message.split('\n')
    edge_length = max([len(line) for line in lines]) + len(line_decorator)
    edge = ''.join(['-' for _ in range(edge_length)])
    decorated_message = '\n' + edge + '\n'

    for line in lines:
        decorated_line = line_decorator + line
        decorated_message += decorated_line + ''.join([' ' for _ in range(edge_length - len(decorated_line))]) + '\n'
    decorated_message += edge
    return decorated_message


def adding(queue_item_creation_function: Callable[[Any], Tuple[str, Dict]] = None, queue_name: Union[str, None] = None) -> Callable:
    def decorator(decorated_function: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            handler = QueuesHandler()
            real_queue_name = handler.default_queue_name if queue_name is None else queue_name
            queue_data = handler[real_queue_name]
            assert isinstance(queue_data[QueueHandlerItem.DATAFRAME], DataFrame), "The dataframe of the queue '{}' is not assigned".format(real_queue_name)
            result = decorated_function(*args, **kwargs)
            new_result = result if queue_item_creation_function is None else queue_item_creation_function(result)

            # Check result's format
            assert isinstance(new_result, (list, tuple)) and len(new_result) == 2, "The new queue's item must be a list or a tuple with length of 2"
            assert isinstance(new_result[1], dict), "The second element of the new queue's item must be a dictionary"
            assigned_dataframe_columns = list(queue_data[QueueHandlerItem.DATAFRAME])
            assert all([True if key in assigned_dataframe_columns else False for key in new_result[1]]), "Columns in the second element of the new queue's item must be in the assigned dataframe : {}".format(list(result[1].keys()))

            queue_data[QueueHandlerItem.QUEUE].append(new_result)
            logging.debug(__create_logging_message("New item added in the queue '{}' : {}\n"
                                                   "Size of the queue : {}\n"
                                                   "Size of the assigned dataframe : {}\n"
                                                   "Max size of the assigned dataframe : {}".format(real_queue_name,
                                                                                                    new_result,
                                                                                                    len(queue_data[QueueHandlerItem.QUEUE]),
                                                                                                    len(queue_data[QueueHandlerItem.DATAFRAME]),
                                                                                                    queue_data[QueueHandlerItem.MAX_SIZE])))

            return result
        return wrapper
    return decorator


def scheduling(queue_name: Union[str, None] = None) -> Callable:
    def decorator(decorated_function: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            handler = QueuesHandler()
            real_queue_name = handler.default_queue_name if queue_name is None else queue_name
            queue_data = handler[real_queue_name]
            assert isinstance(queue_data[QueueHandlerItem.DATAFRAME], DataFrame), "The dataframe of the queue '{}' is not assigned".format(real_queue_name)
            result = decorated_function(*args, **kwargs)
            queue = queue_data[QueueHandlerItem.QUEUE]
            dataframe = queue_data[QueueHandlerItem.DATAFRAME]
            max_size = queue_data[QueueHandlerItem.MAX_SIZE]
            while dataframe.index.size > max_size and len(queue) > 0:
                queue_item = queue.popleft()
                if queue_item[0] in dataframe.index:
                    dataframe_columns = dataframe.loc[queue_item[0], list(queue_item[1].keys())]
                    queue_item_columns = Series(data=queue_item[1], name='Queue Item')
                    if all(queue_item_columns == dataframe_columns) is True:
                        dataframe.drop([queue_item[0]], inplace=True)
                        logging.debug(__create_logging_message("Item removed from the queue '{}' : {}\n"""
                                                               "Size of the queue : {}\n"
                                                               "Size of the assigned dataframe : {}\n"
                                                               "Max size of the assigned dataframe : {}".format(real_queue_name,
                                                                                                                queue_item,
                                                                                                                len(queue),
                                                                                                                len(dataframe),
                                                                                                                max_size)))
                        
            return result
        return wrapper
    return decorator


def assign_dataframe(dataframe: Union[DataFrame, None], max_size: int, queue_name: Union[str, None] = None) -> NoReturn:
    handler = QueuesHandler()
    real_queue_name = handler.default_queue_name if queue_name is None else queue_name
    # Reset the dedicated queue
    handler[real_queue_name] = {QueueHandlerItem.QUEUE: [], QueueHandlerItem.DATAFRAME: dataframe,  QueueHandlerItem.MAX_SIZE: max_size}
