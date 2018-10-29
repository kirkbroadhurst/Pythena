"""
Test that the waiting /  polling process works as expected
"""

import pytest_mock
from pythena import Client


def test_wait(mocker):
    client = Client()
    mocker.patch.object(client.client, 'get_query_execution')

    response = {'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}}

    client.client.get_query_execution.return_value = response
    print(client.client.get_query_execution())
    pass

