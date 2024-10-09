import argparse
import boto3
import json
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser(description='Capture employee team changes and write to Timestream')
    parser.add_argument('-f', '--file', required=True, help='Path to the JSON file containing employee data')
    parser.add_argument('-r', '--region', required=False, help='AWS Region', default="us-west-2")
    parser.add_argument('-d', '--database', required=True, help='Timestream Database')
    parser.add_argument('-t', '--table', required=True, help='Timestream Table')
    parser.add_argument('-a', '--aws-access-key', required=True, help='Amazon Timestream Access Key')
    parser.add_argument('-s', '--aws-access-secret', required=True, help='Amazon Timestream Access Secret')
    return parser.parse_args()


def build_timestream_client(args, name):
    return boto3.client(name,
                        region_name=args.region,
                        aws_access_key_id=args.aws_access_key,
                        aws_secret_access_key=args.aws_access_secret)


def create_timestream_table(args, client):
    try:
        client.describe_table(DatabaseName=args.database, TableName=args.table)
        print(f"Table {args.table} already exists in database {args.database}.")
    except client.exceptions.ResourceNotFoundException:
        print(f"Table {args.table} does not exist. Creating table...")
        try:
            client.create_table(
                DatabaseName=args.database,
                TableName=args.table,
                RetentionProperties={
                    'MemoryStoreRetentionPeriodInHours': 24,
                    'MagneticStoreRetentionPeriodInDays': 3650
                }
            )
            print(f"Table {args.table} created successfully in database {args.database}.")
        except Exception as e:
            print(f"Failed to create table {args.table}: {str(e)}")

def write_team_changes_to_timestream(args, client, team_changes):
    records = []
    for change in team_changes:
        time = datetime.strptime(change['timestamp'], '%Y-%m-%dT%H:%M:%S')
        record = {
            'Dimensions': [
                {'Name': 'employee_id', 'Value': change['employee_id']},
                {'Name': 'old_team', 'Value': change['old_team']},
                {'Name': 'new_team', 'Value': change['new_team']}
            ],
            'MeasureName': 'team_change',
            'MeasureValue': '1',
            'MeasureValueType': 'BIGINT',
            'Time': str(int(round(time.timestamp()))),
            'TimeUnit': 'SECONDS'
        }
        records.append(record)

    try:
        client.write_records(
            DatabaseName=args.database,
            TableName=args.table,
            Records=records
        )
        print(f"Successfully wrote {len(records)} records to Timestream.")
    except Exception as e:
        print(f"Failed to write records to Amazon Timestream: {str(e)}")

def main():
    args = parse_arguments()

    with open(args.file, 'r') as f:
        team_changes = json.load(f)

    client = build_timestream_client(args, 'timestream-write')

    create_timestream_table(args, client)

    write_team_changes_to_timestream(args, client, team_changes)

    client.close()

if __name__ == '__main__':
    main()
