from . import bucket, database, small_csv, table
from pythena import Client


def test_show_databases(small_csv):
    """
    Test that we can inspect databases
    """
    query = 'show databases'
    result = Client(results='pythena-int').execute(query)
    print(type(result))


def test_show_tables(small_csv):
    """
    Test that we can inspect list of tables in our test database
    """
    query = 'show tables in {}'.format(database)
    result = Client(results='pythena-int').execute(query)
    print(result)
    print(type(result))


def test_select_constant(small_csv):
    """
    Tests that we can select a constant value (i.e. 1) - no data dependency
    """
    query = 'select 1'
    result = Client(results='pythena-int').execute(query)
    print(result)
    print(type(result))


def test_select_data(small_csv):
    """
    Tests that we can select some data with the client
    """
    query = 'select * from {}.{}'.format(database, table)
    result = Client(results='pythena-int').execute(query)
    print(result)
    print(type(result))
