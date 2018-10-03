from . import small_csv, database, table
from pythena import Client


def test_show_databases(small_csv):
    """
    Test that we can inspect databases
    """
    query = 'show databases'
    result = Client().athena_query(query)
    print(type(result))
    assert sum(result[0] == database) == 1


def test_show_tables(small_csv):
    """
    Test that we can inspect list of tables in our test database
    """
    query = 'show tables in {}'.format(database)
    result = Client().athena_query(query)
    print(result)
    print(type(result))
    assert result[0][0] == table


def test_select_constant(small_csv):
    """
    Tests that we can select a constant value (i.e. 1) - no data dependency
    """
    query = 'select 1'
    result = Client().athena_query(query)
    print(result)
    print(type(result))
    assert result['_col0'][0] == 1


def test_select_data(small_csv):
    """
    Tests that we can select some data with the client
    """
    query = 'select * from {}.{}'.format(database, table)
    result = Client().athena_query(query)
    print(result)
    print(type(result))
    assert len(result) == 3

