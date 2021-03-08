import boto3
import re
import requests
import json
from requests_aws4auth import AWS4Auth

region = 'us-east-1' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://<xyz>.us-east-1.es.amazonaws.com'

url = host + '/' + '_bulk'

headers = { "Content-Type": "application/x-ndjson" }

s3 = boto3.client('s3')


# Lambda execution starts here
def handler(event, context):
    es_bulk_payload = ""
    for e in event['Records']:
        records = json.loads(e["body"])
        
        for record in records["Records"]:
        # Get the bucket name and key for the new file
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

        # Get, read, and split the file into lines
            obj = s3.get_object(Bucket=bucket,Key=key)
            body = obj['Body'].read().decode("utf-8")
            #print(obj)
            #print(body)
            
        # Extracting S3 object header data to ingest directly into ES
            #metadata = s3.head_object(Bucket=bucket, Key=key)
         
            payload = json.loads(body)
            
            es_payload = list()
            es_payload.append('{"index" : { "_index" : "<index-name>"} }')
            
            es_payload.append(json.JSONEncoder().encode(payload["summary"]))
            #es_payload.append(metadata["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-summary"])
            
            es_bulk_payload += "\n".join(es_payload) + "\n"
    #print(es_bulk_payload)
            
    r = requests.post(url, auth=awsauth, data=es_bulk_payload, headers=headers)
    #payload = list()
    print(r.text)