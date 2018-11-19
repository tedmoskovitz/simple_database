import sys
sys.path.append("../src/")
import CSVCatalog
import CSVTable

import time
import json

data_dir = "../data/"

def cleanup():
    """
    Deletes previously created information to enable re-running tests.
    :return: None
    """
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people", force_drop=True)
    cat.drop_table("batting", force_drop=True)
    cat.drop_table("teams", force_drop=True)

def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")

def test_join_optimizable_4(optimize=False):
    """
    test join with multiple where conditions and projection
    :return:
    """
    cleanup()
    print_test_separator("Starting test_optimizable_4, optimize = " + str(optimize))

    cat = CSVCatalog.CSVCatalog()
    cds = []

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCity", "text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCountry", "text"))
    cds.append(CSVCatalog.ColumnDefinition("throws", column_type="text"))

    t = cat.create_table(
        "people",
        data_dir + "People.csv",
        cds)
    t.define_index("pid_idx", "INDEX", ['playerID'])
    print("People table metadata = \n", json.dumps(t.describe_table(), indent=2))

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("H", "number", True))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number"))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))

    t = cat.create_table(
        "batting",
        data_dir + "Batting.csv",
        cds)
    print("Batting table metadata = \n", json.dumps(t.describe_table(), indent=2))
    t.define_index("pid_idx", "INDEX", ['playerID'])

    people_tbl = CSVTable.CSVTable("people")
    batting_tbl = CSVTable.CSVTable("batting")

    print("Loaded people table = \n", people_tbl)
    print("Loaded batting table = \n", batting_tbl)

    start_time = time.time()

    tmp = {"playerID": "willite01", "yearID": "1955"}
    join_result = people_tbl.join(batting_tbl,['playerID'], where_template=tmp, \
        project_fields=["playerID", "yearID", "H", "AB"], optimize=optimize)

    end_time = time.time()

    print("Result = \n", join_result)
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_4")


test_join_optimizable_4(optimize=True)
