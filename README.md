**Simple Database Implementation**

**Ted Moskovitz, 2018**

An implementation of a simple database. To test, simply run any .py file in the test directory. The implementation assumes that three tables have been created to hold the database metadata, csvtables, csvindexes, and csvcolumns. Their specifications are included in the 'sql' folder. The included data files are from the 2017 Lahman baseball database. 

**Dependencies:**

- python 3.4 or greater

- pymysql

**Core files**

- CSVCatalog.py: constructs a simple database catalog to store metadata, much like INFORMATION_SCHEMA in MySQL. 

- CSVTable.py: implements simple table operations for the database, with the focus on an efficient equi-join. To optimize the join operation, the class leverages indexing and select pushdown. It also switches the scan and probe tables based which carries a more selective index for the column on which the two tables are being joined. 

