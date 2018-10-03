from . import small_csv, database, table
from pythena import Client


def test_show_tables(small_csv):
    """
    Tests that we can select some data with the client
    """
    query = 'show tables in {}'.format(database)
    result = Client.athena_query(query)
    print(result)


def test_show_databases(small_csv):
    """
    Tests that we can select some data with the client
    """
    query = 'show databases'
    result = Client.athena_query(query)
    print(result)


def test_select(small_csv):
    """
    Tests that we can select some data with the client
    """
    query = 'select * from {}.{}'.format(database, table)
    result = Client.athena_query(query)
    print(result)
    #client = pythena.Client()
    #result = client.sql('select top 100 * from mytable')


def test_select_1():
    """
    Tests that we can select the value 1; i.e. no data dependency
    """
    query = 'select 1'
    result = Client.athena_query(query)
    print(result)

