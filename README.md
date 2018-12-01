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
