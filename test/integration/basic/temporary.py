import pytest
import duckdb

@pytest.fixture(scope="module")
def tpch_database_connection(device, extension_path):
    # Setup
    duckdb.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{device.device_path}',
                        fdp_plhdls       '{7}',
                    );""")

    con = duckdb.connect("nvmefs:///database.db", config={"allow_unsigned_extensions": "true", "memory_limit": "500MB"})
    con.load_extension(extension_path)
    con.load_extension("tpch")

    con.execute("CALL dbgen(sf=1);")

    yield con

    # Teardown the database and data
    con.close()
    device.deallocate(1)

def test_large_query_and_spilling_to_disk(tpch_database_connection):
    """
    Tests that the database using nvmefs can handle large queries and spills to disk
    """

    connection = tpch_database_connection
    expected_query7_result = connection.execute("FROM tpch_answers() WHERE query_nr = 7 AND scale_factor = 1;").fethcall()

    query7_result = connection.execute("PRAGMA tpch(7);").fetchall()

    assert query7_result == expected_query7_result
