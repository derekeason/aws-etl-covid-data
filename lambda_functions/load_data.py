import csv
import boto3


def lambda_handler(event, context):
    region = REGION
    record_list = []

    try:
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb', region_name=region)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        csv_file = s3.get_object(Bucket=bucket, Key=key)
        record_list = csv_file['Body'].read().decode('utf-8').split('\n')

        csv_reader = csv.reader(record_list, delimiter=',', quotechar='"')
        next(csv_reader)    # skips the header

        for row in csv_reader:

            date = str(row[0])
            cases = row[1]
            deaths = row[2]
            recovered = row[3]

            add_to_db = dynamodb.put_item(
                TableName='TABLE_NAME',
                Item={
                    'date': {'S': date},
                    'cases': {'N': cases},
                    'deaths': {'N': deaths},
                    'recovered': {'N': recovered}
                })

    except Exception as e:
        print(e)
    return 'Table loaded successfully'
