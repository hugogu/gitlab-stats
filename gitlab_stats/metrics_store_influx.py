from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

from metrics_store import MetricsStore, MetricsProcessor


class InfluxDBMetricsStore(MetricsStore):
    def __init__(self, url, token, org):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def create_table(self, database, table, s3_bucket=None):
        # InfluxDB does not require explicit table creation
        pass

    def write_records(self, records, database, table):
        points = []
        for record in records:
            point = Point(record['MeasureName']) \
                .tag("project", record['Dimensions'][0]['Value']) \
                .tag("group", record['Dimensions'][1]['Value']) \
                .tag("author", record['Dimensions'][2]['Value']) \
                .tag("parents", record['Dimensions'][3]['Value']) \
                .field("value", record['MeasureValue']) \
                .time(record['Time'], WritePrecision.S)
            points.append(point)
        self.write_api.write(bucket=database, record=points)

    def query(self, query_string, data_extractor, default_value):
        try:
            tables = self.query_api.query(query_string)
            return tables
        except Exception as e:
            print(f"Exception while running query: {query_string}", e)
            return []

    def close(self):
        self.client.close()

class InfluxDBMetricsProcessor(MetricsProcessor):
    def load_latest_commit(self, args, project_name):
        query = f'from(bucket: "{args.database}") |> range(start: -10y) |> filter(fn: (r) => r._measurement == "id" and r.project == "{project_name}") |> last()'
        data_extractor = lambda tables: {record['project']: datetime.strptime(record['_time'], '%Y-%m-%dT%H:%M:%SZ') for table in tables for record in table.records}
        return self.store.query(query, data_extractor, {})

    def get_all_commits_id(self, args, project_name):
        query = f'from(bucket: "{args.database}") |> range(start: 0) |> filter(fn: (r) => r._measurement == "id" and r.project == "{project_name}") |> keep(columns: ["_value"])'
        data_extractor = lambda tables: [record['_value'] for table in tables for record in table.records]
        return set(self.store.query(query, data_extractor, []))
