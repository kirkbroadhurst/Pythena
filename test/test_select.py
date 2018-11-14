from . import bucket, database, small_csv, table
from pythena import Client


def test_show_databases(small_csv):
    """
    Test that we can inspect databases
    """
    query = 'show databases'
    result = Client(results='pythena-int').athena_query(query)
    print(type(result))
    assert sum(result[0] == database) == 1


def test_show_tables(small_csv):
    """
    Test that we can inspect list of tables in our test database
    """
    query = 'show tables in {}'.format(database)
    result = Client(results='pythena-int').athena_query(query)
    print(result)
    print(type(result))
    assert result[0][0] == table


def test_select_constant(small_csv):
    """
    Tests that we can select a constant value (i.e. 1) - no data dependency
    """
    query = 'select 1'
    result = Client(results='pythena-int').athena_query(query)
    print(result)
    print(type(result))
    assert result['_col0'][0] == 1


def test_select_data(small_csv):
    """
    Tests that we can select some data with the client
    """
    query = 'select * from {}.{}'.format(database, table)
    result = Client(results='pythena-int').athena_query(query)
    print(result)
    print(type(result))
    assert len(result) == 3


def test_select_single_row(small_csv):
    """
    Tests that a where clause helps and works
    """
    query = 'select * from {}.{} where id = 1'.format(database, table)
    result = Client(results='pythena-int').athena_query(query)
    assert len(result) == 1
    assert result['message'][0] == 'something great!'
