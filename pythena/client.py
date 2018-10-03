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
    def __init__(self, results = None, region = None):
        """
        :results: s3 location to store results / output
            - if empty must be set in PYTHENA_OUTPUTLOCATION variable
        :region: AWS region; if empty will use default
        """
        self.region = region
        self.results = results or os.getenv('PYTHENA_OUTPUTLOCATION')


    def execute(self, query, **kwargs):
        """
        execute a query on athena
        :query: SQL query to execute
        """

        if region != '':
            client = boto3.client('athena', region_name=self.region)
        else:
            client = boto3.client('athena')

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


    def athena_query(query):
        """
        Send a query to Athena. Return the results as a string
        :query: SQL query to execute
        """
        logger.debug('running query {}'.format(query))
        result_config = {
            'OutputLocation': 's3://{}/outputlocation'.format(bucket),
            'EncryptionConfiguration': { 'EncryptionOption': 'SSE_S3' }
        }

        client = boto3.client('athena')
        result = client.start_query_execution(QueryString=query,
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
