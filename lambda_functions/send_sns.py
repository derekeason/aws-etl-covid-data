import json
import logging
import boto3


def lambda_handler(event, context):

    txt = ""
    myeventID = ""
    m = ""

    for record in event['Records']:
        txt = txt + record['eventName']

    for record in event['Records']:
        myeventID = record['eventID']

    n = str(len(event['Records']))
    k = record['dynamodb']['Keys']  # ['date']
    m = f'Successfully processed {n} records. Keys: {k}.'
    message = {"message": m}

    client = boto3.client('sns')
    response = client.publish(
        TargetArn='SNS TOPIC ARN',
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )

    s3 = boto3.resource('s3')
    s3bucket = 'BUCKETNAME'
    fname = 'FOLDER/STREAMFILENAME' + str(myeventID) + '.json'
    s3object = s3.Object(s3bucket, fname)
    s3object.put(
        Body=(bytes(json.dumps(event).encode('UTF-8')))
    )

    return {
        'statusCode': 200,
        'body': json.dumps('-' + txt + '-')
    }
