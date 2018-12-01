DfQueue
=======

What is it?
-----------

DfQueue is a collection of functions for dataframes's rows managing. Deletion priority of rows are defined by their positions in the dedicated queue.

DfQueue can be split into three distinct parts:
- The assignation of a specific queue for each selected dataframe (*assign_dataframe* function)
- The adding of items (related to a dataframe row) in the queues  (*adding* decorator)
- The managing of the dataframes according to items in the related queues (*managing* decorator)

How does it work?
-----------------

DfQueue instantiates a *QueuesHandler* singleton containing all dataframe queues and their parameters. It can't be directly accessed
but the *assign_dataframe* function can reset a specific queue and modify its parameters.

A queues has two parameters:
- The dataframe assigned to the queue
- The maximum allowed size of the assigned dataframe. If the size of the assigned dataframe is greater than this parameter, the managing functions will remove the excess rows during their next calls

Items in the queues are size 2 tuples *(A, B)* containing:
- *A* : The label of the related row. Each queue item represents a row in the assigned dataframe. If the label doesn't exist, the item will be removed and ignored during the next managing function call
- *B* : A dictionary containing columns names of the assigned dataframe and their values used for the checking during the managing process. If the columns values in the item doesn't correpond to the columns values in the assigned dataframe, the item will be removed and ignored during the next managing function call

Queue representation example:

    # Initial situation
    ----------------------------------------------------------------------------------------
    
    # Assigned dataframe (max size : 4)         # Queue
    ---------------------------------           ------------------------------
    |       | COLUMN A  | COLUMN B  |           |           EMPTY            |
    ---------------------------------           ------------------------------
    |             EMPTY             |
    ---------------------------------  


    # Adding rows with only <COLUMN A> as checking column
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    ---------------------------------           ------------------------------
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 }) |
    ---------------------------------           ------------------------------
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 }) |
    ---------------------------------           ------------------------------
    | ROW 2 |     2     |     3     |
    ---------------------------------
    
    
    # Adding rows with <COLUMN A> and <COLUMN B> as checking columns
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    ---------------------------------           --------------------------------------------
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 })               |
    ---------------------------------           --------------------------------------------
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 })               |
    ---------------------------------           --------------------------------------------
    | ROW 2 |     2     |     3     |           | ( ROW 3, { COLUMN A : 4, COLUMN B : 5 }) |
    ---------------------------------           --------------------------------------------
    | ROW 3 |     4     |     5     |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    ---------------------------------           --------------------------------------------
    | ROW 4 |     6     |     7     |
    ---------------------------------
    
    
    # Changing rows values with only <COLUMN B> as checking column
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    ---------------------------------           --------------------------------------------
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 })               |
    ---------------------------------           --------------------------------------------
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 })               |
    ---------------------------------           --------------------------------------------
    | ROW 2 |    200    |    300    |           | ( ROW 3, { COLUMN A : 4, COLUMN B : 5 }) |
    ---------------------------------           --------------------------------------------
    | ROW 3 |     4     |     5     |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    ---------------------------------           --------------------------------------------
    | ROW 4 |     6     |     7     |           | ( ROW 2, { COLUMN B : 300 })             |       
    ---------------------------------           --------------------------------------------
    
    
    # Adding rows with only <COLUMN A> as checking column
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    ---------------------------------           --------------------------------------------
    |       | COLUMN A  | COLUMN B  |           | ( ROW 1, { COLUMN A : 0 })               |
    ---------------------------------           --------------------------------------------
    | ROW 1 |     0     |     1     |           | ( ROW 2, { COLUMN A : 2 })               |
    ---------------------------------           --------------------------------------------
    | ROW 2 |    200    |    300    |           | ( ROW 3, { COLUMN A : 4, COLUMN B : 5 }) |
    ---------------------------------           --------------------------------------------
    | ROW 3 |     4     |     5     |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    ---------------------------------           --------------------------------------------
    | ROW 4 |     6     |     7     |           | ( ROW 2, { COLUMN B : 300 })             |       
    ---------------------------------           --------------------------------------------
    | ROW 5 |     8     |     9     |           | ( ROW 5, { COLUMN A : 8 })               |
    ---------------------------------           --------------------------------------------
    | ROW 6 |     2     |     3     |           | ( ROW 6, { COLUMN A : 2 })               |
    ---------------------------------           --------------------------------------------
    
    Max size of the dataframe id reached. It's time to call the managing function...
    
    
    # Managing dataframe
    ----------------------------------------------------------------------------------------
    
    Assigned dataframe (max size : 4)           # Queue
    ---------------------------------           --------------------------------------------
    |       | COLUMN A  | COLUMN B  |           | ( ROW 4, { COLUMN A : 6, COLUMN B : 7 }) |
    ---------------------------------           --------------------------------------------
    | ROW 2 |    200    |    300    |           | ( ROW 2, { COLUMN B : 300 })             |
    ---------------------------------           --------------------------------------------
    | ROW 4 |     6     |     7     |           | ( ROW 5, { COLUMN A : 8 })               |       
    ---------------------------------           --------------------------------------------
    | ROW 5 |     8     |     9     |           | ( ROW 6, { COLUMN A : 2 })               |
    ---------------------------------           --------------------------------------------
    | ROW 6 |     2     |     3     |
    ---------------------------------
    
    <( ROW 2, { COLUMN A : 2 })> was ignored because the <COLUMN A> value doesn't correpond.
