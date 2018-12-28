DfQueue
=======

What is it?
-----------

DfQueue is a collection of functions for dataframes's rows managing. Deletion priority of rows are defined by their positions in the dedicated queue.

DfQueue can be split into three distinct parts:
- The assignation of a specific queue for each selected dataframe (*assign_dataframe* function)
- The adding of items (related to a dataframe row) in the queues  (*adding* decorator)
- The managing of the dataframes according to items in the related queues (*managing* decorator)

[![pypi][pypi-image]][pypi-url]

[pypi-image]: https://img.shields.io/pypi/v/dfqueue.svg?style=flat
[pypi-url]: https://pypi.org/project/dfqueue

Installation
------------

    pip install dfqueue

How does it work?
-----------------

DfQueue instantiates a *QueuesHandler* singleton containing all dataframe queues and their parameters. It can't be directly accessed
but the *assign_dataframe* function can reset a specific queue and modify its parameters.

A queues has three parameters:
- The dataframe assigned to the queue
- The maximum allowed size of the assigned dataframe. If the size of the assigned dataframe is greater than this parameter, the managing functions will remove the excess rows during their next calls
- The behaviour of the queue during the managing process

Items in the queues are size 2 tuples *(A, B)* containing:
- *A* : The label of the related row. Each queue item represents a row in the assigned dataframe. If the label doesn't exist, the item will be removed and ignored during the next managing function call
- *B* : A dictionary containing columns names of the assigned dataframe and their values used for the checking during the managing process. If the columns values in the item doesn't correpond to the columns values in the assigned dataframe, the item will be removed and ignored during the next managing function call

Queue evolution example:

    # Initial situation
    ----------------------------------------------------------------------------------------
    
    # Assigned dataframe (max size : 4)         # Queue
    +-------+-----------+-----------+           +----------------------------+
    |       | COLUMN A  | COLUMN B  |           |           EMPTY            |
    +=======+===========+===========+           +----------------------------+
    |             EMPTY             |
    +-------------------------------+  


    # Rows adding with only <COLUMN A> as checking column
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    +-------+-----------+-----------+           +----------------------------+
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 }) |
    +=======+===========+===========+           +----------------------------+
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 }) |
    +-------+-----------+-----------+           +----------------------------+
    | ROW 2 |     2     |     3     |
    +-------+-----------+-----------+
    
    
    # Rows adding with <COLUMN A> and <COLUMN B> as checking columns
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    +-------+-----------+-----------+           +------------------------------------------+
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 })               |
    +=======+===========+===========+           +------------------------------------------+
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 })               |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 2 |     2     |     3     |           | ( ROW 3, { COLUMN A : 4, COLUMN B : 5 }) |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 3 |     4     |     5     |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 4 |     6     |     7     |
    +-------+-----------+-----------+
    
    
    # Rows adding values with only <COLUMN A> as checking column
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    +-------+-----------+-----------+           +------------------------------------------+
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 })               |
    +=======+===========+===========+           +------------------------------------------+
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 })               |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 2 |    200    |    300    |           | ( ROW 3, { COLUMN A : 4, COLUMN B : 5 }) |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 3 |     4     |     5     |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 4 |     6     |     7     |           | ( ROW 2, { COLUMN A : 200 })             |       
    +-------+-----------+-----------+           +------------------------------------------+
    
    
    # Rows adding with only <COLUMN A> as checking column
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    +-------+-----------+-----------+           +------------------------------------------+
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 })               |
    +=======+===========+===========+           +------------------------------------------+
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 })               |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 2 |    200    |    300    |           | ( ROW 3, { COLUMN A : 4, COLUMN B : 5 }) |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 3 |     4     |     5     |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 4 |     6     |     7     |           | ( ROW 2, { COLUMN A : 200 })             |       
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 5 |     8     |     9     |           | ( ROW 5, { COLUMN A : 8 })               |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 6 |     2     |     3     |           | ( ROW 6, { COLUMN A : 2 })               |
    +-------+-----------+-----------+           +------------------------------------------+
    
    Max size of the dataframe id reached. It's time to call the managing function...
    
    
    # Managing dataframe
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    +-------+-----------+-----------+           +------------------------------------------+
    |       | COLUMN A  | COLUMN B  |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    +=======+===========+===========+           +------------------------------------------+
    | ROW 2 |    200    |    300    |           | ( ROW 2, { COLUMN A : 200 })             |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 4 |     6     |     7     |           | ( ROW 5, { COLUMN A : 8 })               |       
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 5 |     8     |     9     |           | ( ROW 6, { COLUMN A : 2 })               |
    +-------+-----------+-----------+           +------------------------------------------+
    | ROW 6 |     2     |     3     |
    +-------+-----------+-----------+
    
    If the selected behaviour is ALL_ITEMS : <( ROW 2, { COLUMN A : 2 })> was ignored because the <COLUMN A> value doesn't correpond.
    If the selected behaviour is LAST_ITEM : <( ROW 2, { COLUMN A : 2 })> was ignored because this is not the last item of < ROW 2 > with only < COLUMN A > as checking column.
    

Example !
---------

An game room has 6 clients but only 3 arcades and the manager wants to add a challenge for the players. He creates the following rules:
- A player is replaced by another client every 10 min (i.e., a session).
- The replaced player is one has not won additional levels between this session and the previous one (if none or several are selected, the first in the list  will be chosen).
- If a player reaches the 5th level, he will be a replaced player.

Initialization:

```python
from pandas import DataFrame, Series
from random import randint
from dfqueue import assign_dataframe, managing, adding, QueueBehaviour

arcades_nb = 3
max_level = 5
checking_columns = ['REMAINING_LEVELS']
arcade_room = DataFrame(columns=checking_columns)

clients = [
'BOB',
'JACK',
'TOM',
'DONALD',
'ARNOLD',
'WILLIAM'
]
```
    
Dataframe assignation:

```python
assign_dataframe(arcade_room, arcades_nb, checking_columns, queue_behaviour=QueueBehaviour.ALL_ITEMS)
```
    
Adding and managing function creation:

```python
@managing()
@adding()
def new_session(new_players_nb=1):
    current_players = list()

    if not arcade_room.empty:
        # Udapte the current players
        for label in arcade_room.index:
            remaining_levels = arcade_room.at[label, 'REMAINING_LEVELS']
            # Select a random value between 0 and the previous remaining levels
            new_remaining_levels = randint(0, remaining_levels)
            if new_remaining_levels != remaining_levels:
                if new_remaining_levels != 0:
                    # Udapte the row in the dataframe
                    arcade_room.at[label, 'REMAINING_LEVELS'] = new_remaining_levels
                    # Add an item in the queue 
                    current_players.append((label, {'REMAINING_LEVELS': new_remaining_levels}))
                else:
                    arcade_room.drop([label], inplace=True)

    # Add the new players in the queue and the dataframe
    for _ in range(new_players_nb):
        new_player = (clients.pop(0), {'REMAINING_LEVELS': max_level})
        arcade_room.at[new_player[0]] = Series(data=new_player[1])
        current_players.append(new_player)

    # The results of functions decorated with the @adding decorator have to return a list of queue items (see documentation) 
    return current_players
```
        
The first session :
  
```python
# Add directly 3 players because the arcade room is empty
new_session(3)
```
    
Result:
    
    # arcade_room (max size : 3)                    # Queue
    +---------------+-------------------+           +----------------------------------+
    |               |  REMAINING_LEVEL  |           | ( BOB,  { REMAINING_LEVEL : 5 }) |
    +===============+===================+           +----------------------------------+
    |      BOB      |         5         |           | ( JACK, { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +----------------------------------+
    |      JACK     |         5         |           | ( TOM,  { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +----------------------------------+
    |      TOM      |         5         |
    +---------------+-------------------+
    
The second session :

```python
# Only 1 new player this time
new_session()
```
    
Adding process:
    
    # arcade_room (max size : 3)                    # Queue
    +---------------+-------------------+           +------------------------------------+
    |               |  REMAINING_LEVEL  |           | ( BOB,    { REMAINING_LEVEL : 5 }) |
    +===============+===================+           +------------------------------------+
    |      BOB      |         3         |           | ( JACK,   { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +------------------------------------+
    |      JACK     |         4         |           | ( TOM,    { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +------------------------------------+
    |      TOM      |         5         |           | ( BOB,    { REMAINING_LEVEL : 3 }) |
    +---------------+-------------------+           +------------------------------------+
    |     DONALD    |         5         |           | ( JACK,   { REMAINING_LEVEL : 4 }) |
    +---------------+-------------------+           +------------------------------------+
                                                    | ( DONALD, { REMAINING_LEVEL : 5 }) |
                                                    +------------------------------------+
                                                    
    A Tom's item is not added in the queue because he didn't gain levels.
    
Managing process (and final result):
    
    # arcade_room (max size : 3)                    # Queue
    +---------------+-------------------+           +------------------------------------+
    |               |  REMAINING_LEVEL  |           | ( BOB,    { REMAINING_LEVEL : 3 }) |
    +===============+===================+           +------------------------------------+
    |      BOB      |         3         |           | ( JACK,   { REMAINING_LEVEL : 4 }) |
    +---------------+-------------------+           +------------------------------------+
    |      JACK     |         4         |           | ( DONALD, { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +------------------------------------+
    |     DONALD    |         5         |
    +---------------+-------------------+

The third session : 

```python
# Only 1 new player this time
new_session()
```
    
Adding process:
    
    # arcade_room (max size : 3)                    # Queue
    +---------------+-------------------+           +------------------------------------+
    |               |  REMAINING_LEVEL  |           | ( BOB,    { REMAINING_LEVEL : 3 }) |
    +===============+===================+           +------------------------------------+
    |      BOB      |         3         |           | ( JACK,   { REMAINING_LEVEL : 4 }) |
    +---------------+-------------------+           +------------------------------------+
    |      JACK     |         2         |           | ( DONALD, { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +------------------------------------+
    |     ARNOLD    |         5         |           | ( JACK,   { REMAINING_LEVEL : 2 }) |
    +---------------+-------------------+           +------------------------------------+
                                                    | ( ARNOLD, { REMAINING_LEVEL : 5 }) |
                                                    +------------------------------------+
                                                    
    A Bob's item is not added in the queue because he didn't gain levels.
    Donald is replaced because he gained 5 levels.
    
Managing process didn't do anything because the max size of the dataframe was not reached.

The last session :
    
```python
# Only 1 new player this time
new_session()
```
    
Adding process:
    
    # arcade_room (max size : 3)                    # Queue
    +---------------+-------------------+           +-------------------------------------+
    |               |  REMAINING_LEVEL  |           | ( BOB,     { REMAINING_LEVEL : 3 }) |
    +===============+===================+           +-------------------------------------+
    |      BOB      |         1         |           | ( JACK,    { REMAINING_LEVEL : 4 }) |
    +---------------+-------------------+           +-------------------------------------+
    |      JACK     |         1         |           | ( DONALD,  { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +-------------------------------------+
    |     ARNOLD    |         3         |           | ( JACK,    { REMAINING_LEVEL : 2 }) |
    +---------------+-------------------+           +-------------------------------------+
    |     WILLIAM   |         5         |           | ( ARNOLD,  { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +-------------------------------------+
                                                    | ( BOB,     { REMAINING_LEVEL : 1 }) |
                                                    +-------------------------------------+
                                                    | ( JACK,    { REMAINING_LEVEL : 1 }) |
                                                    +-------------------------------------+
                                                    | ( ARNOLD,  { REMAINING_LEVEL : 3 }) |
                                                    +-------------------------------------+
                                                    | ( WILLIAM, { REMAINING_LEVEL : 5 }) |
                                                    +-------------------------------------+
                                                    
Managing process (and final result):
    
    # arcade_room (max size : 3)                    # Queue
    +---------------+-------------------+           +-------------------------------------+
    |               |  REMAINING_LEVEL  |           | ( JACK,    { REMAINING_LEVEL : 1 }) |
    +===============+===================+           +-------------------------------------+
    |      JACK     |         1         |           | ( ARNOLD,  { REMAINING_LEVEL : 3 }) |
    +---------------+-------------------+           +-------------------------------------+
    |     ARNOLD    |         3         |           | ( WILLIAM, { REMAINING_LEVEL : 5 }) |
    +---------------+-------------------+           +-------------------------------------+
    |     WILLIAM   |         5         |           
    +---------------+-------------------+


Notes
-----

- DfQueue doesn't support dataframes with rows multiindexes.
- One managing process with multiple removed rows is faster than multiple managing processes with only one removed row.
- Pandas 0.23.4 or greater is supported.
