import boto3
import logging
import os
import pytest
import sys
import time
from urllib.parse import urlparse

from pythena import Client


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

database = 'pythena_test'
table = 'unquoted'

bucket = 'pythena-int'
path = 'unquoted'
key = '{}/data'.format(path)

s3 = boto3.resource('s3')

cur_dir = os.path.split(os.path.abspath(__file__))[0]
sys.path.append(cur_dir)


@pytest.fixture(scope='session')
def small_csv():
    """
    Tests that we can select some data with the client
    """
    cleanup()
    print('starting setup')

    # put the file onto s3
    datafile = os.path.join(cur_dir,
                            'data/small_unquoted.csv')
    s3.Bucket(bucket).upload_file(datafile, key)
    print('uploaded file')

    # create the table in Athena
    query = '''CREATE EXTERNAL TABLE `{db}`.`{table}`
        (`id` int, `date` date, `float` float, `message` string)
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
        LOCATION 's3://{bucket}/{path}/'
        tblproperties ("skip.header.line.count"="1")'''.format(
            db=database, table=table, bucket=bucket, path=path)

    Client.athena_query('create database if not exists {}'.format(database))
    print('created database')

    Client.athena_query(query)
    print('created table')

    yield
    cleanup()


def cleanup():
    Client.athena_query('drop database if exists {} cascade'.format(database))
    print('dropped database')

    boto3.client('s3').delete_object(Bucket=bucket, Key=key)
    print('deleted object')

