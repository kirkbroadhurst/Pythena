import boto3

class Client():
    def __init__(self, region, results):
        self.region = region
        self.results = results

    def execute(self, query, **kwargs):
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



