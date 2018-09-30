from . import athena_query
#import pythena


def test_select():
    """
    Tests that we can select some data with the client
    """
    result = athena_query('select count(*) from `pythena-test`.unquoted')
    print(result)
    #client = pythena.Client()
    #result = client.sql('select top 100 * from mytable')

