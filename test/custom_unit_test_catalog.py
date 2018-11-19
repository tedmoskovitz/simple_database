import sys
sys.path.append("../src/")
import CSVCatalog

import time
import json

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


def test_create_table_6_fail():
    """
    tries to retrieve a loaded table that doesn't exist
    :return:
    """
    print_test_separator("Starting test_create_table_6_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))


    t = cat.create_table("batting",
                         "../data/Batting.csv",
                         cds)
    try:
        t = cat.get_table("people")
        print("People table", json.dumps(t.describe_table(), indent=2))
        print_test_separator("FAILURES test_create_table_6_fail")
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_create_table_6_fail should fail.")

def test_create_table_7_fail():
    """
    try to create an index on a nonexistant column
    :return:
    """
    print_test_separator("Starting test_create_table_7_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))


    t = cat.create_table("batting",
                         "../data/Batting.csv",
                         cds)

    try:
        t.define_index("teamID_yearID_idx", "INDEX", ["teamID", "yearID", "canary"])
        print("Batting table", json.dumps(t.describe_table(), indent=2))
        print_test_separator("FAILURES test_create_table_7_fail")
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_create_table_7_fail should fail.")


test_create_table_6_fail()
test_create_table_7_fail()