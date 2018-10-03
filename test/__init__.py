import boto3
import logging
import os
import pytest
import time
from urllib.parse import urlparse


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


@pytest.fixture(scope='session')
def small_csv():
    """
    Tests that we can select some data with the client
    """
    cleanup()
    print('starting setup')

    # put the file onto s3
    datafile = os.path.join(os.path.split(os.path.abspath(__file__))[0],
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

    athena_query('create database if not exists {}'.format(database))
    print('created database')

    athena_query(query)
    print('created table')

    yield
    cleanup()


def cleanup():
    athena_query('drop database if exists {} cascade'.format(database))
    print('dropped database')

    boto3.client('s3').delete_object(Bucket=bucket, Key=key)
    print('deleted object')


def athena_query(sql):
    """
    Send a query to Athena. Return the results as a string
    """
    logger.debug('running query {}'.format(sql))
    result_config = {'OutputLocation': 's3://{}/outputlocation'.format(bucket),
                     'EncryptionConfiguration': { 'EncryptionOption': 'SSE_S3' }
                    }

    client = boto3.client('athena')
    result = client.start_query_execution(QueryString=sql,
                                          ResultConfiguration=result_config)

    query_id = result['QueryExecutionId']
    logger.debug('query submitted with id {}'.format(query_id))
    execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

    while (execution['Status']['State'] not in ['SUCCEEDED', 'FAILED']):
        logger.debug('status: {}'.format(execution['Status']['State']))
        time.sleep(1)
        execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

    logger.debug('query completed')
    path = execution['ResultConfiguration']['OutputLocation']
    key = path[path.find(bucket) + len(bucket) + 1:]

    logger.debug('results at {}'.format(path))
    results = s3.Object(bucket, key).get()['Body'].read().decode('utf-8')
    return results
