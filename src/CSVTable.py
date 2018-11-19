import csv  # Python package for reading and writing CSV files.
import DataTableExceptions
import CSVCatalog


import json

max_rows_to_print = 10


class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True, rows=None, description=None):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog. You have to implement  the called methods below.
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = []
            self.__load__()  # Load rows from the CSV file.

            # Build indexes defined in the metadata. We do not implement insert(), update() or delete().
            # So we can build indexes on load.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"
            if rows: self.__rows__ = rows; 
            self.idxs = {}
            self.__description__ = description
            if self.__description__ is not None:
                self.__build_indexes__
            #self.__build_indexes__()

    def table_from_rows(self, t_name, rows):
        """
        construct table t_name from given rows 
        """
        return CSVTable(t_name, load=False, rows=rows)

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        table = self.__catalog__.get_table(self.__table_name__)
        self.__description__ = table.describe_table()

    def get_description(self):
        return self.__description__

    def name(self):
        return self.__table_name__

    def __add_row__(self, r):
        """
        adds r to the list of rows in the table
        """
        self.__rows__.append(r)

    def __get_file_name__(self):
        return "../data/{}.csv".format(self.__table_name__)

    # Load from a file and creates the table and data.
    def __load__(self):

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __get_column_names__(self):
        return [col["column_name"] for col in self.__description__["columns"]]

    def get_row_list(self):
        return self.__rows__

    def __str__(self):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        s = "Name: " + self.__table_name__  + "\nFile: " + self.__get_file_name__() + \
            "\nRow count: " + str(len(self.__rows__)) + "\n" + json.dumps(self.__description__, indent=2) + \
            "\nSample rows:" 
        if len(self.__rows__) >= 10:
            for i in range(5):
                s += "\n" + str(self.__rows__[i])
            s += "\n..."
            for i in range(-5,0):
                s += "\n" + str(self.__rows__[i])
        else:
            for i in range(len(self.__rows__)):
                s += "\n" + str(self.__rows__[i])
        return s + "\n"

    def __build_index__(self, columns):
        """
        build an index for a given set of coumns 
        """
        idx_dict = {}
        for i,row in enumerate(self.__rows__):
            # generate index name
            col_vals = [str(row[c]) for c in columns]
            key = '_'.join(col_vals)
            if key in idx_dict: idx_dict[key].append(i);
            else: idx_dict[key] = [i]; 
        return idx_dict

    def __build_indexes__(self):
        indexes = self.__description__["indexes"]
        self.idxs = {} # dict mapping index name to the implemented indexes
        for idx in list(indexes.keys()):
            index = self.__build_index__(indexes[idx]["columns"])
            self.idxs[idx] = index 

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.
        Best is defined as the most selective index, i.e. the one with the most distinct index entries.
        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: list of query cols
        :return: Index or None
        """
        # check each index to see if it's compatible with the template
        most_selective = None
        max_selectivity = -1
        for idx_name in list(self.idxs.keys()):
            idx_cols = self.__description__["indexes"][idx_name]["columns"]
            if all([ic in tmp for ic in idx_cols]):
                # match
                idx_selectivity = len(self.__rows__) / len(list(self.idxs[idx_name].keys()))
                if idx_selectivity > max_selectivity:
                    max_selectivity = idx_selectivity
                    most_selective = idx_name
        return most_selective, max_selectivity

    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:

            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        if self.__rows__ is not None:
            # extract values from template in correct order
            t_idx_name = ""
            for col in self.__description__["indexes"][idx]["columns"]:
                t_idx_name += str(t[col]) + "_"
            t_idx_name = t_idx_name[:-1]
            # get row numbers using index
            result_row_idxs = self.idxs[idx][t_idx_name]
            result = [self.__rows__[i] for i in result_row_idxs]
            result = self.project(result, fields)
        else:
            result = None

        return result 

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        if t is not None: access_index, max_selectivity = self.__get_access_path__(list(t.keys()));
        else: access_index = None; 

        if access_index is None: 
            return self.__find_by_template_scan__(t, fields=fields, limit=None, offset=None)
        else:
            result = self.__find_by_template_index__(t, access_index, fields, limit, offset)
            return result

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )

    def get_on_template(self, row, on_fields):
        tmp = {}
        for f in on_fields:
            tmp[f] = row[f]
        return tmp

    def execute_slow_join(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """
        left_r = self
        left_rows = self.__rows__
        right_rows = right_r.get_row_list()
        result_rows = []

        left_rows_processed = 0
        for lr in left_rows:
            on_template = self.get_on_template(lr, on_fields)
            for rr in right_rows:
                if self.matches_template(rr, on_template):
                    new_r = {**lr, **rr}
                    result_rows.append(new_r)
            left_rows_processed += 1
            if left_rows_processed % 10 == 0 and left_rows_processed > 0:
                print ("Processed {}/{} left rows...".format(left_rows_processed, len(left_rows)))

        join_result = self.table_from_rows("JOIN(" + left_r.name()  + "," + right_r.name() + ")", result_rows)
        result = join_result.find_by_template(where_template, fields=project_fields)
        return result 

    def execute_smart_join(self, right_r, on_fields, where_template=None, \
        project_fields=None, idx_selectivities=(-1,-1)):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """
        # set table with maximally selective index to probe table
        # check left table for relevant indexes
        s_max, r_max = idx_selectivities
        if s_max > r_max:
            left_r = right_r
            right_r = self
            selected_l = left_r.find_by_template(where_template, fields=project_fields) # optimization
            selected_l = left_r.table_from_rows("LEFTSELECTED", selected_l)
            left_rows = selected_l.get_row_list()
            right_rows = self.get_row_list()
        else:
            left_r = self
            selected_l = self.find_by_template(where_template, fields=project_fields) # optimization = select before join
            selected_l = self.table_from_rows("LEFTSELECTED", selected_l)
            left_rows = selected_l.get_row_list()
            right_rows = right_r.get_row_list()
        result_rows = []

        left_rows_processed = 0
        for lr in left_rows:
            on_template = self.get_on_template(lr, on_fields)
            # use index to find matches
            matching_rows = right_r.find_by_template(on_template)
            result_rows += [{**lr, **rr} for rr in matching_rows]
            left_rows_processed += 1
            if left_rows_processed % 10000 == 0 and left_rows_processed > 0:
                print ("Processed {}/{} left rows...".format(left_rows_processed, len(left_rows)))

        join_result = self.table_from_rows("JOIN(" + left_r.name() + "," + \
            right_r.name() + ")", result_rows)
        #result = join_result.find_by_template(where_template, fields=project_fields)
        return join_result 

    def join(self, right_r, on_fields, where_template=None, project_fields=None, optimize=True):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """
        # If no optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        #
        # if either table has indexes or there is a 'where' condition, do optimized join
        _, s_max = self.__get_access_path__(on_fields)
        _, r_max = right_r.__get_access_path__(on_fields)
        if optimize and ((s_max > 0 or rr_max > 0) or where_template is not None):
            result = self.execute_smart_join(right_r, on_fields, where_template=where_template, \
                project_fields=project_fields, idx_selectivities=(s_max,r_max))
        else:
            result = self.execute_slow_join(right_r, on_fields, where_template=where_template, project_fields=project_fields)
        #return self.table_from_rows("JOIN(" + self.name() + "," + right_r.name() + ")", result)
        return result
        
