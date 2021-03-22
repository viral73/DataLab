import boto3
import json
import urllib.parse

from textwrap import wrap
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

s3 = boto3.resource('s3')

region = 'us-west-2'

#Connect to es
def connectES():
    service = 'es'
    host = 'vpc-data-lab-xyz.us-west-2.es.amazonaws.com'
    
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    try:
        es = Elasticsearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth = awsauth, use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection)
        return es
    except Exception as E:
        print("Unable to connect to {0}")
        print(E)
        exit(3)

def lambda_handler(event, context):
    #Connect to comprehend
    comprehend = boto3.client(service_name='comprehend', region_name=region)

    #Connect to es
    es = connectES()

    #Get message from SQS
    for sqs_record in event['Records']:

        #Get S3 bucket and key
        s3_record = json.loads(sqs_record['body'])
        s3_body = s3_record['Records'][0]
       
        bucket = s3_body['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(s3_body['s3']['object']['key'], encoding='utf-8')

        #Get object from S3
        obj = s3.Object(bucket, key)
        obj_dict = json.load(obj.get()['Body'])
        obj_dict.pop('_version_', None)
        obj_body = obj_dict['BODY']
        obj_body_modified = ' '.join(obj_body.split())
        obj_dict['BODY'] = obj_body_modified
        
        #Chunk size limit 5k bytes, current body size >5kb 
        chunks = wrap(obj_body, 4500)
        keyphrases = set()
        entities = {}
        
        for chunk in chunks:
            #Get key phrases from Comprehend
            keyphrase_response = comprehend.detect_key_phrases(Text=chunk, LanguageCode='en')
            KeyPhraseList=keyphrase_response.get("KeyPhrases")
            for s in KeyPhraseList:
                keyphrases.add(s.get("Text"))
                  
            
            #Get entities from Comprehend
            detect_entity= comprehend.detect_entities(Text=chunk, LanguageCode='en')
            EntityList=detect_entity.get("Entities")
            for s in EntityList:
                entities.update([(s.get("Type").strip('\t\n\r'),s.get("Text").strip('\t\n\r'))])
        
        searchdata={'KeyPhrases':list(keyphrases),'Entity':entities}
        obj_dict.update(searchdata)
        
        #Put S3 object into ES
        es_result = es.index(index='tornado', body=obj_dict, doc_type='_doc')
        print(es_result)

