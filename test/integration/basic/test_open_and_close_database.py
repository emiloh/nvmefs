import duckdb

def test_create_database_close_and_open(device):
    """
    This tests that we can create a database with a schema and table, close it, and open it again.
    The reason is that the database persist the data after it is closed, therefore we should be 
    able to open it again and read the same data.   
    """

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension("nvmefs")
    con.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{device.get_generic_device_path()}',
                        fdp_plhdls       '{7}',
                        backend         'io_uring_cmd'
                    );""")
    
    con.execute("ATTACH DATABASE 'nvmefs:///test.db' AS test (READ_WRITE);")

    con.execute("CREATE SCHEMA test.public;")
    con.execute("CREATE TABLE test.public.numbers (a INTEGER);")
    con.execute("INSERT INTO test.public.numbers VALUES (1), (2), (3);")
    result = con.execute("SELECT * FROM test.public.numbers;").fetchall()
    assert result == [(1,), (2,), (3,)]

    con.close()

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension("nvmefs")

    con.execute("ATTACH DATABASE 'nvmefs:///test.db' AS test (READ_WRITE);")

    result = con.execute("SELECT * FROM test.public.numbers;").fetchall()
    assert result == [(1,), (2,), (3,)]
    con.close()