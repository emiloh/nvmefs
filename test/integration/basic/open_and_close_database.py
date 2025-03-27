import pytest
import duckdb

def test_create_database_close_and_open(device, extension_path):
    """
    This tests that we can create a database with a schema and table, close it, and open it again.
    The reason is that the database persist the data after it is closed, therefor we should be 
    able to open it again and read the same data.   
    """
    duckdb.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{device.device_path}',
                        fdp_plhdls       '{7}',
                    );""")

    con = duckdb.connect("nvmefs:///test.db", config={"allow_unsigned_extensions": "true"})
    con.load_extension(extension_path)

    con.execute("CREATE SCHEMA public;")
    con.execute("CREATE TABLE public.numbers (a INTEGER);")
    con.execute("INSERT INTO numbers VALUES (1), (2), (3);")
    result = con.execute("SELECT * FROM numbers;").fetchall()
    assert result == [(1), (2), (3)]

    con.close()

    con = duckdb.connect("nvmefs:///test.db", config={"allow_unsigned_extensions": "true"})
    con.load_extension(extension_path)

    result = con.execute("SELECT * FROM numbers;").fetchall()
    assert result == [(1), (2), (3)]
    con.close()
    device.deallocate(1)