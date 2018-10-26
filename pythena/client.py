import boto3
import io
import logging
import os
import pandas as pd
import re
import time


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

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

        if self.region != '':
            client = boto3.client('athena', region_name=self.region)
        else:
            client = boto3.client('athena')

        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': 's3://{}/'.format(self.results),
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }, **kwargs)
        query_id = response['QueryExecutionId']
        return query_id


    def athena_query(self, query):
        """
        Send a query to Athena. Return the results as a string
        :query: SQL query to execute
        """

        # if query is a select query use select values as column names
        columns = self._get_column_names(query)

        logger.debug('running query {}'.format(query))
        result_config = {
            'OutputLocation': 's3://{}/outputlocation'.format(self.results),
            'EncryptionConfiguration': { 'EncryptionOption': 'SSE_S3' }
        }

        client = boto3.client('athena')

        query_id = self.execute(query)
        logger.debug('query submitted with id {}'.format(query_id))
        execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

        while (execution['Status']['State'] not in ['SUCCEEDED', 'FAILED']):
            logger.debug('status: {}'.format(execution['Status']['State']))
            time.sleep(1)
            execution = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']

        logger.debug('query completed')
        path = execution['ResultConfiguration']['OutputLocation']
        key = path[path.find(self.results) + len(self.results) + 1:]

        logger.debug('results at {}'.format(path))
        results = s3.Object(self.results, key).get()['Body'].read()
        logger.debug(results)

        if any(results):
            if self._is_select_query(query):
                return pd.read_csv(io.BytesIO(results))
            return pd.read_csv(io.BytesIO(results), header=None)
        return None


    def _get_column_names(self, query):
        """
        Parses a SQL statement to return column names
        """
        if not self._is_select_query(query):
            return None

        q = query.strip().upper()
        column_section = query[q.find('SELECT')+len('SELECT'):q.find('FROM')]
        return list(filter(None, re.split('[, ]', column_section)))


    def _is_select_query(self, query):
        """
        Check if the query is a select query
        """
        return query.split()[0].upper() == 'SELECT'
