import logging

from uuid import uuid4
from collections import deque
from enum import Enum
from pandas import DataFrame, Series
from typing import Union, Callable, Tuple, Any, NoReturn, Dict, Iterable
from functools import wraps
from threading import Lock


class QueueHandlerItem(Enum):
    """
        Items contained in the QueuesHandler's instance.

        QUEUE : queue
        DATAFRAME : dataframe assigned to the queue
        MAX_SIZE : assigned dataframe's max size
    """

    QUEUE = 0
    DATAFRAME = 1
    MAX_SIZE = 2


class QueuesHandler:
    """
        SINGLETON
    """

    # Singleton
    class __QueuesHandler:
        """
            Dataframe's queues handler.

            Queues define the deleting priority of rows in the assigned dataframes when the dataframe's max sizes are
            reached.

            Queues contain row's labels and columns's values (i.e Queue items) for the scheduling.
        """
        
        def __init__(self):
            # Define the default queue's name
            self.__default_queue_name = str(uuid4())
            # Define the default queue
            self.__queues = {self.__default_queue_name: deque()}
            self.__assigned_dataframes = {self.__default_queue_name: None}
            self.__assigned_dataframe_max_sizes = {self.__default_queue_name: 1000000}
            self.__assigned_locks = {self.__default_queue_name: Lock()}

        @property
        def default_queue_name(self) -> str:
            return self.__default_queue_name

        def get_assigned_lock(self, queue_name: str) -> Lock:
            try:
                return self.__assigned_locks[queue_name]
            except KeyError:
                raise KeyError("The queue '{}' doesn't exist".format(queue_name))

        def assign_lock(self, queue_name: str, assigned_dataframe: DataFrame) -> NoReturn:
            queue_names = [selected_queue_name for (selected_queue_name, selected_dataframe) in self.__assigned_dataframes.items() if id(selected_dataframe) == id(assigned_dataframe)]
            is_lock_found = False
            if len(queue_names) > 0:
                for selected_queue_name, selected_lock in self.__assigned_locks.items():
                    if selected_queue_name in queue_names:
                        self.__assigned_locks[queue_name] = self.__assigned_locks[selected_queue_name]
                        is_lock_found = True
                        break
                if is_lock_found is False:
                    self.__assigned_locks[queue_name] = Lock()
            else:
                self.__assigned_locks[queue_name] = Lock()

        def __getitem__(self, queue_name: str) -> Dict[QueueHandlerItem, Any]:
            try:
                return {QueueHandlerItem.QUEUE: self.__queues[queue_name],
                        QueueHandlerItem.DATAFRAME: self.__assigned_dataframes[queue_name],
                        QueueHandlerItem.MAX_SIZE: self.__assigned_dataframe_max_sizes[queue_name]}
            except KeyError:
                raise KeyError("The queue '{}' doesn't exist".format(queue_name))

        def __setitem__(self, queue_name: str, items: dict) -> NoReturn:
            assert len(items) == len(QueueHandlerItem), "Queue handler item(s) is(are) missing in the dictionary"
            assert all([item in items for item in QueueHandlerItem]), "Items in the dictionary are not queue handler item"
            self.__queues[queue_name] = deque(items[QueueHandlerItem.QUEUE])
            assert isinstance(items[QueueHandlerItem.DATAFRAME], DataFrame) or items[QueueHandlerItem.DATAFRAME] is None, "Dataframe is not a Dataframe object or None"
            self.__assigned_dataframes[queue_name] = items[QueueHandlerItem.DATAFRAME]
            assert isinstance(items[QueueHandlerItem.MAX_SIZE], int), "Max size is not an integer"
            self.__assigned_dataframe_max_sizes[queue_name] = items[QueueHandlerItem.MAX_SIZE]

    __instance = None

    __doc__ += __QueuesHandler.__doc__

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
    """
        Generate a logging message with a predefined format.

        :param message: raw logging message
        :type message: str

        :return: formatted message
        :rtype: str
    """

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


def adding(queue_item_creation_function: Callable[..., Tuple[Any, Dict]] = None, queue_name: Union[str, None] = None, other_args: Union[None, Dict[str, Any]] = None) -> Callable:
    """
        Add a new item in a queue of the QueueHandler's instance.

        Item added in the queue will be the result of the decorated function or the result of the queue item creation
        function if it is not None.

        The format of the queue's item has to be Tuple[Any, Dict]:
        - The first element is the selected index's label in the assigned dataframe
        - The second element is a dictionary with the selected columns (and the values) for the scheduling of the
        assigned dataframe

        :param queue_item_creation_function: queue item creation function from the result of the decorated function
        :type queue_item_creation_function: Callable[[Any], Tuple[Any, Dict]]

        :param queue_name: name of the selected queue
        :type queue_name: Union[str, None]

        :param other_args: additional args for the queue item creation function
        :type other_args: Union[None, Dict[str, Any]]

        :return: Decorated function
        :rtype: Callable
    """

    def decorator(decorated_function: Callable) -> Callable:
        @wraps(decorated_function)
        def wrapper(*args, **kwargs) -> Any:
            handler = QueuesHandler()
            real_queue_name = handler.default_queue_name if queue_name is None else queue_name
            queue_data = handler[real_queue_name]
            assert isinstance(queue_data[QueueHandlerItem.DATAFRAME], DataFrame), "The dataframe of the queue '{}' is not assigned".format(real_queue_name)
            result = decorated_function(*args, **kwargs)
            new_result = result if queue_item_creation_function is None else queue_item_creation_function(result) if other_args is None else queue_item_creation_function(result, **other_args)

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
    """
        Remove rows in the dataframe's queue when the dataframe's max size is reached.

        If a row's label is present in the queue but the column's values don't match, the queue's item will be ignored.

        :param queue_name: Name of the queue for the scheduling
        :type queue_name: Union[str, None]

        :return: Decorated function
        :rtype: Callable
    """

    def decorator(decorated_function: Callable) -> Callable:
        @wraps(decorated_function)
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


def synchronized(queue_name: Union[str, None] = None) -> Callable:
    """
        Acquire the queue's Lock object before the decorated function calling. The Lock object will be released at the
        end of the decorated function calling.

        The same Lock object will be shared if several queues have the same assigned datframe.

        :param queue_name: Name of the queue for the synchronization
        :type queue_name: Union[str, None]

        :return: Decorated function
        :rtype: Callable
    """

    def decorator(decorated_function: Callable) -> Callable:
        @wraps(decorated_function)
        def wrapper(*args, **kwargs) -> Any:
            # noinspection PyProtectedMember
            lock = QueuesHandler._QueuesHandler__instance.get_assigned_lock(queue_name)
            lock.acquire()
            try:
                return decorated_function(*args, **kwargs)
            finally:
                lock.release()

        return wrapper
    return decorator


def assign_dataframe(dataframe: Union[DataFrame, None], max_size: int, selected_columns: Iterable[Any], queue_name: Union[str, None] = None) -> NoReturn:
    """
        Assign a dataframe to a QueueHandler's queue and reset the queue.

        Items in the assigned dataframe will be added in the reseted queue according to the columns's names in the
        'selected_columns' parameter.

        :param dataframe: New assigned dataframe
        :type dataframe: Union[DataFrame, None]

        :param max_size: Max size of the assigned dataframe for the scheduling
        :type max_size: int

        :param selected_columns: Names of the dataframe's columns used for the initial queue's items creation
        :type selected_columns: Iterable[Any]

        :param queue_name: Name of the selected queue
        :type queue_name: Union[str, None]
    """

    assert all([selected_column in dataframe.columns for selected_column in selected_columns] if dataframe is not None else [True]), "Selected columns don't exist in the dataframe"
    handler = QueuesHandler()
    real_queue_name = handler.default_queue_name if queue_name is None else queue_name
    # Reset the dedicated queue
    reseted_queue = dataframe.apply(lambda row: (row.name, {selected_column: row[selected_column] for selected_column in selected_columns}), axis=1) if dataframe is not None else []
    handler[real_queue_name] = {QueueHandlerItem.QUEUE: reseted_queue if len(reseted_queue) else [], QueueHandlerItem.DATAFRAME: dataframe,  QueueHandlerItem.MAX_SIZE: max_size}
    # noinspection PyProtectedMember
    QueuesHandler._QueuesHandler__instance.assign_lock(queue_name, dataframe)
    logging.debug(__create_logging_message("New dataframe assigned to the queue '{}'\n"
                                           "Size of the queue : {}\n"
                                           "Size of the assigned dataframe : {}\n"
                                           "Max size of the assigned dataframe : {}".format(real_queue_name,
                                                                                            len(reseted_queue),
                                                                                            len(dataframe) if dataframe is not None else None,
                                                                                            max_size)))
