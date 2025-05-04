import pytest
import duckdb
from decimal import Decimal
from datetime import datetime

@pytest.fixture(scope="module")
def tpch_database_connection(device):
    # Setup

    con = duckdb.connect(config={"allow_unsigned_extensions": "true", "memory_limit": "75MB", "threads": 1})
    con.load_extension("nvmefs")
    con.load_extension("tpch")

    con.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{device.get_generic_device_path()}',
                        fdp_plhdls       '{7}',
                        backend          'io_uring_cmd'
                    );""")

    con.execute("ATTACH DATABASE 'nvmefs:///tpch.db' AS test (READ_WRITE);")
    con.execute("USE test;")

    con.execute("CALL dbgen(sf=1);")

    yield con

    # Teardown the database and data
    con.close()

def test_tpch_1_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 1
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], columns[1], Decimal(columns[2]), Decimal(columns[3]), Decimal(columns[4]), Decimal(columns[5]), float(columns[6]), float(columns[7]), float(columns[8]), int(columns[9])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_2_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 2
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(Decimal(columns[0]), columns[1], columns[2], int(columns[3]), columns[4], columns[5], columns[6], columns[7]) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_3_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 3
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(int(columns[0]), Decimal(columns[1]), datetime.strptime(columns[2], '%Y-%m-%d').date(), int(columns[3])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_4_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 4
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], int(columns[1])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_5_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 5
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], Decimal(columns[1])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_6_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 6
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(Decimal(columns[0]), ) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_7_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 7
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], columns[1], int(columns[2]), Decimal(columns[3])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_8_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 8
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(int(columns[0]), float(columns[1])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_9_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 9
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], int(columns[1]), Decimal(columns[2])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_10_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 10
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(int(columns[0]), columns[1], Decimal(columns[2]), Decimal(columns[3]), columns[4], columns[5], columns[6], columns[7]) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_11_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 11
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(int(columns[0]), Decimal(columns[1])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_12_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 12
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], int(columns[1]), int(columns[2])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_13_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 13
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(int(columns[0]), int(columns[1])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_14_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 14
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(float(columns[0]), ) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_15_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 15
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(int(columns[0]), columns[1], columns[2], columns[3], Decimal(columns[4])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_16_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 16
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], columns[1], int(columns[2]), int(columns[3])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_17_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 17
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(float(columns[0]),) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_18_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 18
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], int(columns[1]), int(columns[2]), datetime.strptime(columns[3], '%Y-%m-%d').date(), Decimal(columns[4]), Decimal(columns[5])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_19_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 19
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(Decimal(columns[0]),) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_20_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 20
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], columns[1]) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results

def test_tpch_21_spill(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    query = 21
    connection = tpch_database_connection
    result = connection.execute(f"SELECT answer FROM tpch_answers() WHERE query_nr = {query} AND scale_factor = 1;").fetchall()
    result_rows = result[0][0].splitlines()[1:]

    expected_query_results = [(columns[0], int(columns[1])) for columns in 
                               [line.split("|") for line in result_rows]]


    query_result = connection.execute(f"PRAGMA tpch({query});").fetchall()

    assert query_result == expected_query_results