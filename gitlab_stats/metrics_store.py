from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

class MetricsStore(ABC):
    @abstractmethod
    def create_table(self, database, table, s3_bucket=None):
        pass

    @abstractmethod
    def write_records(self, records, database, table):
        pass

    @abstractmethod
    def query(self, query_string, data_extractor, default_value):
        pass

    @abstractmethod
    def close(self):
        pass


class MetricsProcessor(ABC):
    def __init__(self, store):
        self.store = store

    @abstractmethod
    def load_latest_commit(self, args, project_name):
        pass

    @abstractmethod
    def get_all_commits_id(self, args, project_name):
        pass

    def process_commit(self, commit, project):
        commit_id = commit['id']
        commit_details = project.commits.get(commit_id)
        group, _ = project.name_with_namespace.split(' / ')
        time = datetime.strptime(commit['created_at'], '%Y-%m-%dT%H:%M:%S.%f%z')
        author = commit['author_name']

        # Amazon Timestream Table has a 10 years limit (which is configurable), you can't insert data older than 10 years.
        if time <= (datetime.now(timezone.utc) - timedelta(days=365 * 10)):
            return

        dimensions = [
            {'Name': 'project', 'Value': project.name},
            {'Name': 'group', 'Value': group},
            {'Name': 'author', 'Value': author},
            {'Name': 'parents', 'Value': str(len(commit['parent_ids']))}
        ]

        yield {
            'Dimensions': dimensions,
            'MeasureName': 'additions',
            'MeasureValue': str(commit_details.stats.get('additions', 0)),
            'Time': str(int(round(time.timestamp()))),
            'TimeUnit': 'SECONDS'
        }
        yield {
            'Dimensions': dimensions,
            'MeasureName': 'deletions',
            'MeasureValue': str(commit_details.stats.get('deletions', 0)),
            'Time': str(int(round(time.timestamp()))),
            'TimeUnit': 'SECONDS'
        }
        yield {
            'Dimensions': dimensions,
            'MeasureName': 'id',
            'MeasureValue': str(commit_id),
            'MeasureValueType': 'VARCHAR',
            'Time': str(int(round(time.timestamp()))),
            'TimeUnit': 'SECONDS'
        }
        if commit['message'] and 1 <= len(commit['message']) <= 2048:
            yield {
                'Dimensions': dimensions,
                'MeasureName': 'message',
                'MeasureValue': commit['message'],
                'MeasureValueType': 'VARCHAR',
                'Time': str(int(round(time.timestamp()))),
                'TimeUnit': 'SECONDS'
            }
