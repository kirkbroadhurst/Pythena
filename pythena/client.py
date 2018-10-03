import boto3
import logging
import time


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

bucket = 'pythena-int'

s3 = boto3.resource('s3')


class Client():
    """
    Main class for pythena use
    """
    def __init__(self, region, results):
        self.region = region
        self.results = results


    def execute(self, query, **kwargs):
        """
        execute a query on athena
        :query: SQL query to execute
        """
        client = boto3.client('athena', region_name=self.region)
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': self.results,
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }, **kwargs)
        query_id = response['QueryExecutionId']
        return response


    def athena_query(sql):
        """
        Send a query to Athena. Return the results as a string
        :sql: SQL query to execute
        """
        logger.debug('running query {}'.format(sql))
        result_config = {
            'OutputLocation': 's3://{}/outputlocation'.format(bucket),
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
