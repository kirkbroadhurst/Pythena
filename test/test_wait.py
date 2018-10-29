"""
Test that the waiting /  polling process works as expected
"""

import pytest_mock
from pythena import Client


def test_wait(mocker):
    """ 
    Assert that for some good result, the wait function succeeds
    """
    client = Client()
    mocker.patch.object(client.client, 'get_query_execution')

    response = {'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}}

    client.client.get_query_execution.return_value = response

    result = client.wait_for_results('abc123')
    assert result
