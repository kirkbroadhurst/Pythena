from pythena import Client

def test_is_select_query():
    """ Tests for select queries """
    client = Client()
    assert client._is_select_query('select 1')
    assert client._is_select_query('  SELECT 1')
    assert client._is_select_query('SELECT `insert`, `update`, from nothing')


def test_is_not_select_query():
    """ Tests for not select queries """
    client = Client()
    assert not client._is_select_query('update something set a = b')
    assert not client._is_select_query('create table something awful')
    assert not client._is_select_query('show databases')


def test_parse_simple_select():
    """ Test that a very simple select statement can be parsed """
    client = Client()
    columns = client._get_column_names('select a, b, c from table')
    assert columns == ['a', 'b', 'c']


def test_parse_create():
    """ Test that a create query doesn't return columns """
    client = Client()
    columns = client._get_column_names('create table abc (id int) ')
    assert columns is None
