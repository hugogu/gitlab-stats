import boto3
from datetime import datetime
from metrics_store import MetricsStore, MetricsProcessor


class TimestreamMetricsStore(MetricsStore):
    def __init__(self, region, aws_access_key, aws_access_secret):
        self.write_client = boto3.client('timestream-write',
                                         region_name=region,
                                         aws_access_key_id=aws_access_key,
                                         aws_secret_access_key=aws_access_secret)
        self.query_client = boto3.client('timestream-query',
                                         region_name=region,
                                         aws_access_key_id=aws_access_key,
                                         aws_secret_access_key=aws_access_secret)

    def create_table(self, database, table, s3_bucket=None):
        try:
            self.write_client.describe_table(DatabaseName=database, TableName=table)
            print(f"Table {table} already exists in database {database}.")
        except self.write_client.exceptions.ResourceNotFoundException:
            print(f"Table {table} does not exist. Creating table...")
            self.write_client.create_table(
                DatabaseName=database,
                TableName=table,
                RetentionProperties={
                    'MemoryStoreRetentionPeriodInHours': 24,
                    'MagneticStoreRetentionPeriodInDays': 3650
                },
                MagneticStoreWriteProperties={
                    'EnableMagneticStoreWrites': True,
                    'MagneticStoreRejectedDataLocation': {
                        'S3Configuration': {
                            'BucketName': s3_bucket
                        }
                    }
                }
            )
            print(f"Table {table} created successfully in database {database}.")

    def write_records(self, records, database, table):
        for trunk in [records[x:x + 100] for x in range(0, len(records), 100)]:
            print(f"Writing {len(trunk)} metrics.")
            try:
                self.write_client.write_records(
                    DatabaseName=database,
                    TableName=table,
                    Records=trunk
                )
            except Exception as e:
                print(f"\t\tFailed to write records to Amazon Timestream: {str(e)}")

    def query(self, query_string, data_extractor, default_value):
        try:
            response = self.query_client.query(QueryString=query_string)
            if response['Rows']:
                return data_extractor(response['Rows'])
            else:
                return default_value
        except Exception as e:
            print(f"Exception while running query: {query_string}", e)
            return default_value

    def close(self):
        self.write_client.close()
        self.query_client.close()

class TimestreamMetricsProcessor(MetricsProcessor):
    def load_latest_commit(self, args, project_name):
        query = f"SELECT project, MAX(time) as max_time FROM \"{args.database}\".\"{args.table}\" where project = '{project_name}' AND measure_name = 'id' GROUP BY project"
        data_extractor = lambda rows: {row['Data'][0]['ScalarValue']: datetime.strptime(row['Data'][1]['ScalarValue'][:-3], '%Y-%m-%d %H:%M:%S.%f') for row in rows}
        return self.store.query(query, data_extractor, {})

    def get_all_commits_id(self, args, project_name):
        query = f"SELECT measure_value::varchar FROM \"{args.database}\".\"{args.table}\" where project = '{project_name}' AND measure_name = 'id'"
        data_extractor = lambda rows: [row['Data'][0]['ScalarValue'] for row in rows]
        return set(self.store.query(query, data_extractor, []))
