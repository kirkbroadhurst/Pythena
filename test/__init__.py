import boto3
import pytest
import time
from urllib.parse import urlparse


database = 'pythena-test'
table = 'unquoted'

bucket = 'pythena-int'
path = 'unquoted'

s3 = boto3.resource('s3')


@pytest.fixture(scope='session')
def small_csv():
    """
    Tests that we can select some data with the client
    """
    # put the file onto s3
    key = '{}/data'.format(path)
    s3.Bucket(bucket).upload_file('data/small_unquoted.csv', key)

    # create the table in Athena
    query = '''CREATE EXTERNAL TABLE `{db}`.`{table}`
        (`id` int, `date` date, `float` float, `message` string)
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            ESCAPED BY '\\'
            LINES TERMINATED BY '\n'
        LOCATION 's3://{bucket}/{path}/'
        tblproperties ("skip.header.line.count"="1")'''.format(
            db=database, table=table, bucket=bucket, path=path)

    athena_query('create database if not exists {}'.format(database))
    athena_query(query)
    yield

    athena_query('drop table `{}`.`{}`'.format(database, table))
    boto3.client('s3').delete_object(Bucket=bucket, Key=key)


def athena_query(sql):
    """
    Send a query to Athena. Return the results as a string
    """

    result_config = {'OutputLocation': 's3://{}/outputlocation'.format(bucket),
                     'EncryptionConfiguration': { 'EncryptionOption': 'SSE_S3' }
                    }

    client = boto3.client('athena')
    result = client.start_query_execution(QueryString=sql,
                                          ResultConfiguration=result_config)

    query_id = result['QueryExecutionId']
    execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

    while (execution['Status']['State'] != 'SUCCESS'):
        time.sleep(1000)
        execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

    path = execution['ResultConfiguration']['OutputLocation']
    key = path[path.find(bucket) + len(bucket) + 1:]

    results = s3.Object(bucket, key).get()['Body'].read().decode('utf-8')
    return results
