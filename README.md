DfQueue
=======

What is it?
-----------

DfQueue is a collection of functions for dataframes's rows managing. Deletion priority of rows are defined by their positions in the dedicated queue.

DfQueue can be split into three distinct parts:
- The assignation of a specific queue for each selected dataframe
- The adding of items (related to a dataframe row) in the queues
- The managing of the dataframes according to items in the related queues
