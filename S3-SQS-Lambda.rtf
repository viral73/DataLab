{\rtf1\ansi\ansicpg1252\cocoartf1671\cocoasubrtf600
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww17240\viewh12160\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf0 \expnd0\expndtw0\kerning0
import boto3\
import json\
import urllib.parse\
\
from textwrap import wrap\
from requests_aws4auth import AWS4Auth\
from elasticsearch import Elasticsearch, RequestsHttpConnection\
\
s3 = boto3.resource('s3')\
\
region = 'us-west-2'\
\
#Connect to es\
def connectES():\
\'a0 \'a0 service = 'es'\
\'a0 \'a0 host = 'vpc-data-lab-xyz.us-west-2.es.amazonaws.com'\
\'a0 \'a0 \
\'a0 \'a0 credentials = boto3.Session().get_credentials()\
\'a0 \'a0 awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)\
\'a0 \'a0 try:\
\'a0 \'a0 \'a0 \'a0 es = Elasticsearch(\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 hosts=[\{'host': host, 'port': 443\}],\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 http_auth = awsauth, use_ssl=True,\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 verify_certs=True,\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 connection_class=RequestsHttpConnection)\
\'a0 \'a0 \'a0 \'a0 return es\
\'a0 \'a0 except Exception as E:\
\'a0 \'a0 \'a0 \'a0 print("Unable to connect to \{0\}")\
\'a0 \'a0 \'a0 \'a0 print(E)\
\'a0 \'a0 \'a0 \'a0 exit(3)\
\
def lambda_handler(event, context):\
\'a0 \'a0 #Connect to comprehend\
\'a0 \'a0 comprehend = boto3.client(service_name='comprehend', region_name=region)\
\
\'a0 \'a0 #Connect to es\
\'a0 \'a0 es = connectES()\
\
\'a0 \'a0 #Get message from SQS\
\'a0 \'a0 for sqs_record in event['Records']:\
\
\'a0 \'a0 \'a0 \'a0 #Get S3 bucket and key\
\'a0 \'a0 \'a0 \'a0 s3_record = json.loads(sqs_record['body'])\
\'a0 \'a0 \'a0 \'a0 s3_body = s3_record['Records'][0]\
\'a0 \'a0 \'a0 \'a0\
\'a0 \'a0 \'a0 \'a0 bucket = s3_body['s3']['bucket']['name']\
\'a0 \'a0 \'a0 \'a0 key = urllib.parse.unquote_plus(s3_body['s3']['object']['key'], encoding='utf-8')\
\
\'a0 \'a0 \'a0 \'a0 #Get object from S3\
\'a0 \'a0 \'a0 \'a0 obj = s3.Object(bucket, key)\
\'a0 \'a0 \'a0 \'a0 obj_dict = json.load(obj.get()['Body'])\
\'a0 \'a0 \'a0 \'a0 obj_dict.pop('_version_', None)\
\'a0 \'a0 \'a0 \'a0 obj_body = obj_dict['BODY']\
\'a0 \'a0 \'a0 \'a0 obj_body_modified = ' '.join(obj_body.split())\
\'a0 \'a0 \'a0 \'a0 obj_dict['BODY'] = obj_body_modified\
\'a0 \'a0 \'a0 \'a0 \
\'a0 \'a0 \'a0 \'a0 #Chunk size limit 5k bytes, current body size >5kb \
\'a0 \'a0 \'a0 \'a0 chunks = wrap(obj_body, 4500)\
\'a0 \'a0 \'a0 \'a0 keyphrases = set()\
\'a0 \'a0 \'a0 \'a0 entities = \{\}\
\'a0 \'a0 \'a0 \'a0 \
\'a0 \'a0 \'a0 \'a0 for chunk in chunks:\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 #Get key phrases from Comprehend\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 keyphrase_response = comprehend.detect_key_phrases(Text=chunk, LanguageCode='en')\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 KeyPhraseList=keyphrase_response.get("KeyPhrases")\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 for s in KeyPhraseList:\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 keyphrases.add(s.get("Text"))\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 #Get entities from Comprehend\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 detect_entity= comprehend.detect_entities(Text=chunk, LanguageCode='en')\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 EntityList=detect_entity.get("Entities")\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 for s in EntityList:\
\'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 \'a0 entities.update([(s.get("Type").strip('\\t\\n\\r'),s.get("Text").strip('\\t\\n\\r'))])\
\'a0 \'a0 \'a0 \'a0 \
\'a0 \'a0 \'a0 \'a0 searchdata=\{'KeyPhrases':list(keyphrases),'Entity':entities\}\
\'a0 \'a0 \'a0 \'a0 obj_dict.update(searchdata)\
\'a0 \'a0 \'a0 \'a0 \
\'a0 \'a0 \'a0 \'a0 #Put S3 object into ES\
\'a0 \'a0 \'a0 \'a0 es_result = es.index(index='tornado', body=obj_dict, doc_type='_doc')\
\'a0 \'a0 \'a0 \'a0 print(es_result)\
\
}