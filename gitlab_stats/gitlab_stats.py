import argparse

from concurrent import futures
from datetime import datetime
from metrics_fetcher_github import GitHubMetricsFetcher
from metrics_fetcher_gitlab import GitLabMetricsFetcher
from metrics_store_ts import TimestreamMetricsStore, TimestreamMetricsProcessor
from metrics_store_influx import InfluxDBMetricsStore, InfluxDBMetricsProcessor


def parse_arguments():
    parser = argparse.ArgumentParser(description='GitLab to Timestream')
    parser.add_argument('-k', '--access-key', required=True, help='GitLab Access Token')
    parser.add_argument('-u', '--gitlab-url', required=False, help='GitLab URL')
    parser.add_argument('-r', '--region', required=False, help='AWS Region', default="us-east-1")
    parser.add_argument('-d', '--database', required=False, help='Timestream Database')
    parser.add_argument('-c', '--s3-bucket', required=True, help='S3 Bucket for Magnetic Store Writes')
    parser.add_argument('-a', '--aws-access-key', required=True, help='Amazon Timestream Access Key')
    parser.add_argument('-s', '--aws-access-secret', required=True, help='Amazon Timestream Access Secret')
    parser.add_argument('-t', '--table', required=False, help='Timestream Table', default="gitlab-history")
    parser.add_argument('-b', '--all-branch', required=False, help='Capture all branches', default=False)
    parser.add_argument('-l', '--reload', required=False, help='Timestream Table', default=False)
    parser.add_argument('-p', '--project', required=False, help='A specific Project to parse')
    parser.add_argument('--store-type', required=False, choices=['timestream', 'influxdb'], help='Type of metrics store', default = "timestream")
    parser.add_argument('--influxdb-url', required=False, help='InfluxDB URL')
    parser.add_argument('--influxdb-token', required=False, help='InfluxDB Token')
    parser.add_argument('--influxdb-org', required=False, help='InfluxDB Organization')
    parser.add_argument('--site-type', required=False, choices=['gitlab', 'github'], help='Type of site to fetch metrics from', default='gitlab')

    return parser.parse_args()

class Context:
    def __init__(self, args):
        self.args = args
        if args.site_type == 'gitlab':
            self.metrics_fetcher = GitLabMetricsFetcher(args.gitlab_url, args.access_key)
        elif args.site_type == 'github':
            self.metrics_fetcher = GitHubMetricsFetcher(args.access_key)
        else:
            raise ValueError("Unsupported site type")

        if args.store_type == 'timestream':
            self.metrics_store = TimestreamMetricsStore(args.region, args.aws_access_key, args.aws_access_secret)
            self.metrics_processor = TimestreamMetricsProcessor(self.metrics_store)
        elif args.store_type == 'influxdb':
            self.metrics_store = InfluxDBMetricsStore(args.influxdb_url, args.influxdb_token, args.influxdb_org)
            self.metrics_processor = InfluxDBMetricsProcessor(self.metrics_store)
        else:
            raise ValueError("Unsupported store type")

        self.metrics_store.create_table(args.database, args.table, args.s3_bucket)

    def __exit__(self, exc_type, exc_val, exc_tb):
       self.metrics_store.close()


def generate_project_records(context, project, default_branch):
    processed_commits = context.metrics_processor.load_latest_commit(context.args, project.name)
    if not context.args.reload and project.name in processed_commits:
        since = processed_commits.get(project.name, 0)
    else:
        since = datetime.strptime('2000-01-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')
    commits = context.metrics_fetcher.fetch_commits(project, since, context.args.all_branch)
    existing_commits = context.metrics_processor.get_all_commits_id(context.args, project.name)
    print(f"Retrieved {len(commits)} commits in project: {project.name} since ${since}")

    for commit in commits:
        if context.args.reload or commit.sha not in existing_commits:
            existing_commits.add(commit.sha)
            yield from context.metrics_processor.process_commit(commit, project)


def main():
    args = parse_arguments()
    context = Context(args)
    context.metrics_store.create_table(args.database, args.table, args.s3_bucket)

    projects = [p for p in context.metrics_fetcher.fetch_projects()
                if args.project is None or p.name == args.project]
    print(f"Loaded {len(projects)} projects from gitlab")

    def process_project(project):
        records = [r for r in generate_project_records(context, project, "master")]
        print(f"Processing {len(records)} new records in project: {project.name}")
        context.metrics_store.write_records(records, args.database, args.table)

    # MultiThreading
    # futures.ThreadPoolExecutor().map(process_project, projects)

    # Single Thread
    # Process projects
    for project in projects:
        process_project(project)

if __name__ == '__main__':
    main()

