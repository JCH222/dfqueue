# coding: utf8

import logging

from uuid import uuid4
from collections import deque
from itertools import islice
from enum import Enum
from typing import Union, Callable, Tuple, Any, NoReturn, Dict, Iterable, List
from functools import wraps
from threading import Lock
from itertools import compress
from pandas import DataFrame
from collections import Counter


__all__ = ['adding', 'managing', 'synchronized', 'assign_dataframe', 'list_queue_names',
           'get_info_provider', 'QueueBehaviour']


class QueueHandlerItem(Enum):
    """
        Items contained in the QueuesHandler's instance.

        QUEUE : queue
        COUNTER : number of occurences of queue items with the same row label and selected columns
        DATAFRAME : dataframe assigned to the queue
        MAX_SIZE : assigned dataframe's max size
        BEHAVIOUR : queue managing behaviour
    """

    QUEUE = 0
    COUNTER = 1
    DATAFRAME = 2
    MAX_SIZE = 3
    BEHAVIOUR = 4


class QueueBehaviour(Enum):
    """
        Behaviour of the queue during the managing process.

        LAST_ITEM : only the last item in the queue for each group of items
        (same row label and same selected columns) is used during the managing process
        ALL_ITEMS : all items in the queue for each group of items
        (same row label and same selected columns) is used during the managing process
    """

    LAST_ITEM = 0
    ALL_ITEMS = 1


class QueuesHandler:
    """
        SINGLETON
    """

    # Singleton
    class __QueuesHandler:
        """
            Dataframe's queues handler.

            Queues define the deleting priority of rows in the assigned dataframes when the
            dataframe's max sizes are
            reached.

            Queues contain row's labels and columns's values (i.e Queue items) for the managing.
        """
        def __init__(self):
            # Define the default queue's name
            self.__default_queue_name = str(uuid4())
            # Define the default queue
            self.__queues = {self.__default_queue_name: deque()}
            self.__counters = {self.__default_queue_name: dict()}
            self.__assigned_dataframes = {self.__default_queue_name: None}
            self.__assigned_dataframe_max_sizes = {self.__default_queue_name: 1000000}
            self.__assigned_locks = {self.__default_queue_name: Lock()}
            self.__queue_behaviour = {self.__default_queue_name: QueueBehaviour.LAST_ITEM}

        @property
        def default_queue_name(self) -> str:
            return self.__default_queue_name

        def get_assigned_lock(self, queue_name: str) -> Lock:
            assert queue_name in self.__assigned_locks, \
                "The queue '{}' doesn't exist".format(queue_name)
            return self.__assigned_locks[queue_name]

        def list_queue_names(self) -> Tuple[str]:
            return tuple(self.__queues.keys())

        def assign_lock(self, queue_name: str, assigned_dataframe: DataFrame) -> NoReturn:
            queue_names = [selected_queue_name for (selected_queue_name, selected_dataframe) in
                           self.__assigned_dataframes.items() if
                           id(selected_dataframe) == id(assigned_dataframe)]
            is_lock_found = False
            if queue_names:
                for selected_queue_name in self.__assigned_locks:
                    if selected_queue_name in queue_names:
                        self.__assigned_locks[queue_name] = \
                            self.__assigned_locks[selected_queue_name]
                        is_lock_found = True
                        break
                if is_lock_found is False:
                    self.__assigned_locks[queue_name] = Lock()
            else:
                self.__assigned_locks[queue_name] = Lock()

        def __getitem__(self, queue_name: str) -> Dict[QueueHandlerItem, Any]:
            assert queue_name in self.__queues, \
                "The queue '{}' doesn't exist".format(queue_name)
            assert queue_name in self.__counters, \
                "The counter for the queue '{}' doesn't exist".format(queue_name)
            assert queue_name in self.__assigned_dataframes, \
                "The assigned dataframe for the queue '{}' doesn't exist".format(queue_name)
            assert queue_name in self.__assigned_dataframe_max_sizes, \
                "The assigned dataframe's max size for " \
                "the queue '{}' doesn't exist".format(queue_name)
            assert queue_name in self.__queue_behaviour,\
                "The behaviour for the queue '{}' doesn't exist".format(queue_name)

            return {QueueHandlerItem.QUEUE: self.__queues[queue_name],
                    QueueHandlerItem.COUNTER: self.__counters[queue_name],
                    QueueHandlerItem.DATAFRAME: self.__assigned_dataframes[queue_name],
                    QueueHandlerItem.MAX_SIZE: self.__assigned_dataframe_max_sizes[queue_name],
                    QueueHandlerItem.BEHAVIOUR: self.__queue_behaviour[queue_name]}

        def __setitem__(self, queue_name: str, items: dict) -> NoReturn:
            assert len(items) == len(QueueHandlerItem), \
                "Queue handler item(s) is(are) missing in the dictionary"
            assert all([item in items for item in QueueHandlerItem]), \
                "Items in the dictionary are not queue handler item"
            self.__queues[queue_name] = deque(items[QueueHandlerItem.QUEUE])
            assert isinstance(items[QueueHandlerItem.COUNTER],
                              dict) and all([isinstance(counter, Counter) for counter
                                             in items[QueueHandlerItem.COUNTER].values()])
            self.__counters[queue_name] = items[QueueHandlerItem.COUNTER]
            assert isinstance(items[QueueHandlerItem.DATAFRAME], DataFrame) or \
                   items[QueueHandlerItem.DATAFRAME] is None, \
                "Dataframe is not a Dataframe object or None"
            self.__assigned_dataframes[queue_name] = items[QueueHandlerItem.DATAFRAME]
            assert isinstance(items[QueueHandlerItem.MAX_SIZE], int), "Max size is not an integer"
            self.__assigned_dataframe_max_sizes[queue_name] = items[QueueHandlerItem.MAX_SIZE]
            assert isinstance(items[QueueHandlerItem.BEHAVIOUR], QueueBehaviour), \
                "Behaviour is not a QueueBehaviour object"
            self.__queue_behaviour[queue_name] = items[QueueHandlerItem.BEHAVIOUR]

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
        decorated_message += \
            decorated_line + ''.join([' ' for _ in range(edge_length - len(decorated_line))]) + '\n'
    decorated_message += edge
    return decorated_message


def adding(queue_items_creation_function: Callable[..., List[Tuple[Any, Dict]]] = None,
           queue_name: Union[str, None] = None,
           other_args: Union[None, Dict[str, Any]] = None) -> Callable:
    """
        Add new items in a queue of the QueueHandler's instance.

        Items added in the queue will be the result of the decorated function or the result of the
        queue item creation
        function if it is not None.

        The format of the queue's item has to be Tuple[Any, Dict]:
        - The first element is the selected index's label in the assigned dataframe
        - The second element is a dictionary with the selected columns (and the values) for the
        managing of the
        assigned dataframe

        :param queue_items_creation_function: queue items creation function used with the result of
        the decorated function
        :type queue_items_creation_function: Callable[[Any], List[Tuple[Any, Dict]]]

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
            assert isinstance(queue_data[QueueHandlerItem.DATAFRAME], DataFrame), \
                "The dataframe of the queue '{}' is not assigned".format(real_queue_name)
            result = decorated_function(*args, **kwargs)
            if queue_items_creation_function is None:
                new_result = result
            elif other_args is None:
                new_result = queue_items_creation_function(result)
            else:
                new_result = queue_items_creation_function(result, **other_args)

            if __debug__:
                # Check result's format
                assigned_dataframe_columns = list(queue_data[QueueHandlerItem.DATAFRAME])
                assert isinstance(new_result, (list, tuple)), \
                    "Queue's items must be contained in a list or a tuple object"
                for index, item in enumerate(new_result):
                    assert isinstance(item, (list, tuple)) and len(item) == 2, \
                        "Item {} : The new queue's item must be a list or a " \
                        "tuple with length of 2".format(index)
                    assert isinstance(item[1], dict), \
                        "Item {} : The second element of the new queue's " \
                        "item must be a dictionary".format(index)
                    for key in item[1]:
                        assert key in assigned_dataframe_columns, \
                            "Item {} : Column {} in the second element of the new queue's " \
                            "item is not in the assigned dataframe : " \
                            "{}".format(list(result[1].keys()), key, index)

            for item in new_result:
                queue_data[QueueHandlerItem.QUEUE].append(item)
                counter = queue_data[QueueHandlerItem.COUNTER]
                if item[0] not in counter:
                    counter[item[0]] = Counter()
                counter[item[0]][frozenset(item[1].keys())] += 1

                if __debug__:
                    logging.debug(
                        __create_logging_message("New item added in the queue '{}' : {}\n"
                                                 "Size of the queue : {}\n"
                                                 "Size of the assigned dataframe : {}\n"
                                                 "Max size of the assigned "
                                                 "dataframe : {}".
                                                 format(real_queue_name,
                                                        item,
                                                        len(queue_data[QueueHandlerItem.QUEUE]),
                                                        len(queue_data[QueueHandlerItem.DATAFRAME]),
                                                        queue_data[QueueHandlerItem.MAX_SIZE])))

            return result
        return wrapper
    return decorator


def managing(queue_name: Union[str, None] = None) -> Callable:
    """
        Remove rows in the dataframe's queue when the dataframe's max size is reached.

        If a row's label is present in the queue but the column's values don't match, the queue's
        item will be ignored.

        :param queue_name: Name of the queue for the managing
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
            assert isinstance(queue_data[QueueHandlerItem.DATAFRAME], DataFrame), \
                "The dataframe of the queue '{}' is not assigned".format(real_queue_name)
            result = decorated_function(*args, **kwargs)
            queue = queue_data[QueueHandlerItem.QUEUE]
            counter = queue_data[QueueHandlerItem.COUNTER]
            dataframe = queue_data[QueueHandlerItem.DATAFRAME]
            max_size = queue_data[QueueHandlerItem.MAX_SIZE]
            behaviour = queue_data[QueueHandlerItem.BEHAVIOUR]

            def get_items_nb() -> int:
                queue_size = len(queue)
                diff = dataframe.index.size - max_size
                return queue_size if diff > queue_size else diff

            def pop_left_queue(pop_nb: int) -> List[dict]:
                items = list()
                for _ in range(pop_nb):
                    item = queue.popleft()
                    key = frozenset(item[1].keys())
                    if behaviour == QueueBehaviour.LAST_ITEM:
                        if counter[item[0]][key] == 1:
                            items.append(item)
                        elif counter[item[0]][key] <= 0:
                            pass
                    elif behaviour == QueueBehaviour.ALL_ITEMS:
                        items.append(item)
                    else:
                        pass

                    counter[item[0]][key] -= 1
                return dict(items)

            items_nb = get_items_nb()
            while items_nb > 0 and queue:
                queue_items = pop_left_queue(items_nb)
                selected_labels = list(compress(dataframe.index,
                                                dataframe.index.isin(queue_items.keys())))

                selected_checking_values_list = list()
                selected_columns = set()
                for selected_label in selected_labels:
                    selected_checking_values = queue_items[selected_label]
                    selected_checking_values_list.append(selected_checking_values)
                    selected_columns.update(selected_checking_values.keys())
                selected_columns = list(selected_columns)

                original_dataframe = dataframe.loc[selected_labels, selected_columns]
                queue_items_dataframe = DataFrame(data=selected_checking_values_list,
                                                  index=selected_labels)
                comparison_result = original_dataframe == queue_items_dataframe.reindex(
                    columns=original_dataframe.columns)
                new_selected_labels = list(filter(None,
                                                  comparison_result.apply(lambda row:
                                                                          row.name if all(row)
                                                                          else None, axis=1)))
                dataframe.drop(new_selected_labels, inplace=True)
                if __debug__:
                    logging.debug(
                        __create_logging_message("Item removed from the queue '{}' : {}\n"
                                                 "Size of the queue : {}\n"
                                                 "Size of the assigned dataframe : {}\n"
                                                 "Max size of the assigned dataframe : {}".
                                                 format(real_queue_name,
                                                        "\n".join(
                                                            [str((label, values))
                                                             for label, values in
                                                             queue_items.items()]),
                                                        len(queue),
                                                        len(dataframe),
                                                        max_size)))
                items_nb = get_items_nb()

            return result
        return wrapper
    return decorator


def synchronized(queue_name: Union[str, None] = None) -> Callable:
    """
        Acquire the queue's Lock object before the decorated function calling. The Lock object
        will be released at the
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


def assign_dataframe(dataframe: Union[DataFrame, None],
                     max_size: int,
                     selected_columns: Iterable[Any],
                     queue_name: Union[str, None] = None,
                     queue_behaviour: QueueBehaviour = QueueBehaviour.LAST_ITEM) -> NoReturn:
    """
        Assign a dataframe to a QueueHandler's queue and reset the queue.

        Items in the assigned dataframe will be added in the reseted queue according to the
        columns's names in the
        'selected_columns' parameter.

        :param dataframe: New assigned dataframe
        :type dataframe: Union[DataFrame, None]

        :param max_size: Max size of the assigned dataframe for the managing
        :type max_size: int

        :param selected_columns: Names of the dataframe's columns used for the initial queue's
        items creation
        :type selected_columns: Iterable[Any]

        :param queue_name: Name of the selected queue
        :type queue_name: Union[str, None]

        :param queue_behaviour: behaviour of the queue during the managing process
        :type queue_behaviour: QueueBehaviour
    """

    if __debug__ and dataframe is not None:
        columns = dataframe.columns
        for selected_column in selected_columns:
            assert selected_column in columns, \
                "Selected columns {} doesn't exist in the dataframe".format(selected_column)

    def get_values(r, cs, counter):
        values = dict()
        for c in cs:
            values[c] = r[c]
        if r.name not in counter:
            counter[r.name] = Counter()
        counter[r.name][frozenset(cs)] += 1
        return values

    handler = QueuesHandler()
    real_queue_name = handler.default_queue_name if queue_name is None else queue_name
    # Reset the dedicated queue
    if dataframe is not None:
        if not dataframe.empty:
            reseted_counter = {}
            reseted_queue = dataframe.apply(lambda row:
                                            (row.name,
                                             get_values(row, selected_columns, reseted_counter)),
                                            axis=1,
                                            result_type='reduce')
        else:
            reseted_counter = {}
            reseted_queue = []
    else:
        reseted_counter = {}
        reseted_queue = []

    handler[real_queue_name] = {QueueHandlerItem.QUEUE: reseted_queue,
                                QueueHandlerItem.COUNTER: reseted_counter,
                                QueueHandlerItem.DATAFRAME: dataframe,
                                QueueHandlerItem.MAX_SIZE: max_size,
                                QueueHandlerItem.BEHAVIOUR: queue_behaviour}
    # noinspection PyProtectedMember
    QueuesHandler._QueuesHandler__instance.assign_lock(queue_name, dataframe)
    if __debug__:
        logging.debug(
            __create_logging_message("New dataframe assigned to the queue '{}'\n"
                                     "Size of the queue : {}\n"
                                     "Size of the assigned dataframe : {}\n"
                                     "Max size of the assigned dataframe : {}".
                                     format(real_queue_name,
                                            len(reseted_queue),
                                            len(dataframe) if dataframe is not None else None,
                                            max_size)))


def list_queue_names() -> Tuple[str]:
    """
        List all current queue names.

        :return: Current queue names
        :rtype: Tuple[str]
    """

    # noinspection PyProtectedMember
    return QueuesHandler()._QueuesHandler__instance.list_queue_names()


class QueueInfoProvider:
    """
        It provides access to specific queue data in read only mode.
    """
    class QueueWrapper:
        """
            Queue wrapper provides access to a specific queue in read only mode.

            It may be manipulated as a list:
            - brackets with int type
            - brackets with slice type
            - len function
            - iteration
            - containing
            - equality
        """
        def __init__(self, queue_name: str):
            self.__queue_handler = QueuesHandler()
            self.__queue_name = queue_name

        def __getitem__(self, item):
            if isinstance(item, int):
                return self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE][item]
            if isinstance(item, slice):
                queue = self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE]
                return tuple(islice(queue, item.start, item.stop, item.step))
            raise ValueError("Item type {} not allowed "
                             "(only int or slice)".format(type(item).__name__))

        def __len__(self):
            return len(self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE])

        def __iter__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE].__iter__()

        def __next__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE].__next__()

        def __str__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE].__str__()

        def __repr__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE].__repr__()

        def __contains__(self, item):
            return (self.__queue_handler[self.__queue_name]
                    [QueueHandlerItem.QUEUE].__contains__(item))

        def __eq__(self, other):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.QUEUE].__eq__(other)

    class CounterWrapper:
        """
            Counter wrapper provides access to a specific counter in read only mode.

            It may be manipulated as a dict:
            - brackets with key
            - len function
            - iteration
            - containing
            - equality
            - keys method
            - values method
            - items method
        """
        def __init__(self, queue_name: str):
            self.__queue_handler = QueuesHandler()
            self.__queue_name = queue_name

        def __getitem__(self, item):
            return (self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER]
                    [item].copy())

        def __len__(self):
            return len(self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER])

        def __iter__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].__iter__()

        def __next__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].__next__()

        def __str__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].__str__()

        def __repr__(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].__repr__()

        def __contains__(self, item):
            return (self.__queue_handler[self.__queue_name]
                    [QueueHandlerItem.COUNTER].__contains__(item))

        def __eq__(self, other):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].__eq__(other)

        def keys(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].keys()

        def values(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].values()

        def items(self):
            return self.__queue_handler[self.__queue_name][QueueHandlerItem.COUNTER].items()

    def __init__(self, queue_name: Union[str, None] = None):
        if __debug__ and queue_name is not None:
            assert queue_name in list_queue_names(), \
                "The queue '{}' doesn't exist".format(queue_name)

        self.__handler = QueuesHandler()
        if queue_name is None:
            queue_name = self.__handler.default_queue_name

        self.__queue_name = queue_name
        if queue_name == self.__handler.default_queue_name:
            self.__is_default_queue = True
        else:
            self.__is_default_queue = False

    @property
    def queue_name(self) -> str:
        return self.__queue_name

    @property
    def is_default_queue(self) -> bool:
        return self.__is_default_queue

    @property
    def assigned_dataframe(self) -> DataFrame:
        return self.__handler[self.__queue_name][QueueHandlerItem.DATAFRAME]

    @property
    def max_size(self) -> int:
        return self.__handler[self.__queue_name][QueueHandlerItem.MAX_SIZE]

    @property
    def queue(self) -> QueueWrapper:
        return QueueInfoProvider.QueueWrapper(self.__queue_name)

    @property
    def counter(self) -> CounterWrapper:
        return QueueInfoProvider.CounterWrapper(self.__queue_name)

    def __repr__(self):
        return "{} : {}".format(type(self).__name__, self.__queue_name)


def get_info_provider(queue_name: Union[str, None] = None) -> QueueInfoProvider:
    """
        Generate an information provider to a specific queue.

        :param queue_name: Selected queue name
        :type queue_name: Union[str, None]

        :return: Queue information provider
        :rtype: QueueInfoProvider
    """
    return QueueInfoProvider(queue_name=queue_name)
