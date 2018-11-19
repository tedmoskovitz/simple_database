import pymysql
import csv
import logging
import json 
from DataTableExceptions import DataTableException


def templateToInsertClause(resource, t):
    '''
    Convert from a dictionary template to an insert statement
    :param t: dictionary template:
    :return: 'insert' clause
    '''
    s = "INSERT INTO `{}` ".format(resource)
    kstr = "("
    vstr = "("
    for (k,v) in t.items():
        kstr +=  k + ", "
        vstr += "\'" + v + "\', "
    kstr = kstr[:-2] + ")"
    vstr = vstr[:-2] + ")"
    return s + kstr + " VALUES " + vstr + ";"

def templateToWhereClause(t):
    '''
    Convert from a dictionary template to an SQL 'where' clause
    :param t: dictionary template for query
    :return: 'where' clause (string)
    '''
    s = ""
    for (k,v) in t.items():
        if s != "":
            s += " and "
        s += k + "='" + v + "'"

    if s != "":
        s = "where " + s;

    return s

class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """
        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        if column_type not in self.column_types:
            raise ValueError("Invalid column type.")
        self.column_name = column_name
        self.column_type = column_type 
        self.not_null = not_null

    def __str__(self):
        return "column name: {}, column type: {}, not_null: {}".format(self.column_name,
            self.column_type, self.not_null)

    def to_json(self):
        """
        :return: A JSON object, not a string, representing the column and it's properties.
        """
        col = {}
        col["column_name"] = self.column_name
        col["column_type"] = self.column_type
        col["not_null"] = self.not_null
        return col
        
class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type, columns):
        """
        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        :param column: column name 
        """
        if index_type not in self.index_types:
            raise ValueError('Invalid index type.')
        self.index_name = index_name
        self.index_type = index_type
        self.columns = columns

    def to_json(self):
        """
        :return: A JSON object, not a string, representing the index and its properties.
        """     
        idx = {}
        idx["index_name"] = self.index_name
        idx["columns"] = self.columns
        idx["kind"] = self.index_type
        return idx 

class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None, load=False):
        """
        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        if load: 
            table_info, column_info, index_info = self.load_table_definition(cnx, t_name)
            self.cnx = cnx
            self.csv_f = csv_f
            self.t_name = t_name
            self.column_definitions = []
            self.column_names = []
            self.index_definitions = []
            self.index_names = []

            # extract column info
            for cdict in column_info:
                not_null = True
                ctype = "number"
                if cdict["not_null"] == "NO": not_null = False; 
                if cdict["column_type"] == "text" or cdict["column_type"].startswith("varchar"): ctype = "text";
                new_col = ColumnDefinition(cdict["column_name"], ctype, not_null)
                self.column_definitions.append(new_col)
                self.column_names.append(new_col.column_name)

            # extract index info
            indexes = {}
            for idict in index_info:
                key_name = "PRIMARY" if idict["index_name"] == "PRI" else idict["index_name"]
                if key_name in list(indexes.keys()): 
                    indexes[key_name]["columns"].append((idict["column_name"], idict["ordinal_pos"]))
                else: 
                    indexes[key_name] = {}
                    indexes[key_name]["columns"] = [(idict["column_name"], idict["ordinal_pos"])]
                    indexes[key_name]["index_type"] = idict["index_type"]

            # build index definitions
            for idx_name in list(indexes.keys()):
                # sort columns by ordinal position
                indexes[idx_name]["columns"].sort(key=lambda x: x[1])
                cols = [c[0] for c in indexes[idx_name]["columns"]]
                new_index = IndexDefinition(idx_name, indexes[idx_name]["index_type"], cols)
                self.index_definitions.append(new_index)
                self.index_names.append(idx_name)

        else:
            self.t_name = t_name 
            self.csv_f = csv_f 
            self.cnx = cnx 
            # inspect csv file
            with open(self.csv_f, newline='') as f:
                reader = csv.reader(f)
                csv_cols = next(reader) 
            self.column_definitions = column_definitions if column_definitions is not None else []
            self.column_names = []
            if column_definitions is not None and self.cnx is not None:
                for col in self.column_definitions: 
                    self.add_column_definition(col, new=False); 
                self.column_names = [col.column_name for col in self.column_definitions]

            # verify column names
            if any([c not in csv_cols for c in self.column_names]):
                raise ValueError("Column definition is invalid.")

            self.index_definitions = index_definitions if index_definitions is not None else []
            self.index_names = []
            if index_definitions is not None:
                for idx in self.index_definitions and self.cnx is not None: 
                    self.define_index(idx.index_name, idx.index_type, idx.columns, new=False)
                self.index_names = [idx.index_name for idx in self.index_definitions]
            
    def __str__(self):
        pass

    @classmethod
    def load_table_definition(self, cnx, table_name):
        """
        :param cls: the class object for the method
        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        cursor = cnx.cursor()
        template = {"t_name":table_name}
        # table info
        table_q = "select * from csvtables " + templateToWhereClause(template) + ";"
        cursor.execute(table_q)
        table_info = cursor.fetchall()

        # column info 
        col_q = "select * from csvcolumns " + templateToWhereClause(template) + ";"
        cursor.execute(col_q)
        column_info = cursor.fetchall()

        # index info
        idx_q = "select * from csvindexes " + templateToWhereClause(template) + ";"
        cursor.execute(idx_q)
        index_info = cursor.fetchall() 

        cnx.commit() 
        return table_info, column_info, index_info

    def add_column_definition(self, c, new=True):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :param new: if new column is being added after creation of table
        :return: None
        """
        if c.column_name in self.column_names:
            raise ValueError("Attempted to add a duplicate column.")

        # need to check file still****
        not_null = "YES" if c.not_null else "NO"
        template = {"column_name":c.column_name, "column_type":c.column_type,
                    "not_null":not_null, "t_name":self.t_name}
        q = templateToInsertClause("csvcolumns", template)
        cursor = self.cnx.cursor()
        cursor.execute(q)
        self.cnx.commit() 
        if new:
            self.column_definitions.append(c)
            self.column_names.append(c.column_name)

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        for col in self.column_definitions:
            if col.column_name == c:
                cursor = self.cnx.cursor()
                template = {"column_name":c, "t_name":self.t_name}
                w = templateToWhereClause(template)
                # must also drop any indexes using this column
                q_indexes = "select index_name from csvindexes " + w + ";"
                cursor.execute(q_indexes)
                indexes = cursor.fetchall()
                for idx in indexes: self.drop_index(idx);
                q_col = "delete from csvcolumns " + w + ";"
                cursor.execute(q_indexes)
                cursor.execute(q_col)
                self.cnx.commit()
                self.column_definitions.remove(col)
                self.column_names.remove(col.column_name)
                break 

    def to_json(self):
        """
        :return: A JSON representation of the table and it's elements.
        """
        info = {}
        info["definition"] = {"name":self.t_name, "path":self.csv_f}
        info["columns"] = [] if self.column_definitions is None else [col.to_json() for col in self.column_definitions]
        info["indexes"] = {} 
        if self.index_definitions is not None:
            for idx in self.index_definitions:
                info["indexes"][idx.index_name] = idx.to_json()
        return info #json.dumps(info)

    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values (names) in order.
        :return:
        """
        # if any column is not in table, throw an exception 
        if any([c not in self.column_names for c in columns]):
            raise ValueError("Invalid column name for primary key.")
        self.primary_key = []
        for c in self.column_definitions:
            if c.column_name in columns:
                self.primary_key.append(c)

        if self.index_definitions is not None:
            for idx in self.index_definitions:
                if idx.index_type == "PRIMARY":
                    self.drop_index("PRIMARY")

        self.define_index("PRIMARY", "PRIMARY", columns)

    def define_index(self, index_name, kind, columns, new=True):
        """
        Define or replace an index definition.
        :param index_name: Index name, must be unique within a table.
        :param column: Valid column.
        :param kind: One of the valid index types.
        :param new: if new index is being added after creation of table
        :return:
        """
        if index_name in self.index_names: 
            raise ValueError("Duplicate index name.")
        if any([c not in self.column_names for c in columns]):
            raise ValueError("Attempted to create index on invalid column.")
        new_idx = IndexDefinition(index_name, index_type=kind, columns=columns)

        # add to db
        cursor = self.cnx.cursor()
        for i,c in enumerate(columns):
            template = {"index_name":index_name, "index_type":kind,
                        "column_name":c, "t_name":self.t_name, "ordinal_pos":str(i+1)}
            q = templateToInsertClause("csvindexes", template)
            cursor.execute(q)
        self.cnx.commit() 
        
        if new:
            self.index_definitions.append(new_idx)
            self.index_names.append(index_name)

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        for idx in self.index_definitions:
            if idx.index_name == index_name:
                cursor = self.cnx.cursor()
                for i_name in idx.columns:
                    # drop from sql
                    template = {"index_name":index_name, "column_name":i_name, "t_name":self.t_name}
                    w = templateToWhereClause(template)
                    q = "delete from csvindexes " + w + ";"
                    cursor.execute(q)
                self.cnx.commit()
                self.index_definitions.remove(idx)
                self.index_names.remove(index_name)
                break

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        return self.to_json()


class CSVCatalog:

    def __init__(self, dbhost="localhost", dbport=3306, 
                 dbname="CSVCatalog", dbuser="dbuser", dbpw="dbuser", debug_mode=None):
        self.cnx = pymysql.connect(host=dbhost,
                              user=dbuser,
                              password=dbpw,
                              db=dbname,
                              port=dbport,
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)
        table_query = "select * from csvtables;"
        cursor = self.cnx.cursor()
        cursor.execute(table_query)
        table_dicts = cursor.fetchall()
        self.cnx.commit()

        self.tables = []
        for tdict in table_dicts:
            self.tables.append(TableDefinition(t_name=tdict["t_name"], csv_f=tdict["csv_f"],
                cnx=self.cnx, load=True))

    def __str__(self):
        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        idx_definitions = None
        if primary_key_columns is not None:
            idx_definitions = IndexDefinition("PRIMARY", "PRIMARY", primary_key_columns)
        
        table_names = [table.t_name for table in self.tables]
        if table_name in table_names:
            raise ValueError("Table with name \'{}\' already exists in catalog.".format(table_name))

        # add table to db
        cursor = self.cnx.cursor()
        template = {"t_name":table_name, "csv_f":file_name}
        q = templateToInsertClause("csvtables", template)
        cursor.execute(q)
        self.cnx.commit()

        table = TableDefinition(t_name=table_name, csv_f=file_name,
            column_definitions=column_definitions, index_definitions=idx_definitions, 
            cnx=self.cnx, load=False)

        self.tables.append(table)
        return table 

    def drop_table(self, table_name):
        # remove from db
        cursor = self.cnx.cursor()
        template = {"t_name":table_name}
        w = templateToWhereClause(template)
        q_columns = "delete from csvcolumns " + w + ";"
        q_indexes = "delete from csvindexes " + w + ";"
        q_table = "delete from csvtables " + w + ";"
        cursor.execute("set FOREIGN_KEY_CHECKS=0;")
        cursor.execute(q_indexes)
        cursor.execute(q_columns)
        cursor.execute(q_table)
        cursor.execute("set FOREIGN_KEY_CHECKS=1;")
        self.cnx.commit()
        for t in self.tables:
            if t.t_name == table_name:
                self.tables.remove(t)
                break 

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table. (load = True)
        :return:
        """
        #table_names = [table.t_name for table in self.tables]
        if table_name not in [t.t_name for t in self.tables]:
            raise ValueError("Requested table does not exist.")

        for table in self.tables:
            if table.t_name == table_name:
                return table  
