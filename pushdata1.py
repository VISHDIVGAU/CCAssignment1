import json
with open('yelp-data.json') as json_file:
    dumpdata= json.load(json_file)['businesses']
'''field={"timestamp":""}
for data in dumpdata:
    data.update(field)'''
    #print(data['id'], data['location'], data['coordinates'], data['review_count'], data['rating'], data['location']['zip_code'], data['timestamp'])
#print(dumpdata[0])

for data in dumpdata:
    l=[]
    for j in data['categories']:
        l.append(j['alias'])
    record={"name":data['name'], "cuisine":l}
    y= json.dumps(record, indent=4)
#print(y)
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from decimal import *
import time
import pandas as pd
import os
'''key_file=pd.read_csv('/home/divyagupta/cloudcomputing/Administrator_accessKeys.csv')
access_key_id= key_file['Access key ID']
secret_access_key= key_file['Secret access key']'''
credentials = boto3.Session(aws_access_key_id = 'AKIAQXCNDHWYIJQ5MYTZ', aws_secret_access_key= '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').get_credentials()
region= 'us-east-1'
service= 'es'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
host= 'search-search-restaurant-m2jiw2a2ntzgzzctr55yxl6glq.us-east-1.es.amazonaws.com'
print("printing authen")
print(awsauth)
es= OpenSearch(
        hosts= [{'host': host, 'port': 443}],
        http_auth= awsauth,
        use_ssl= True,
        verify_certs= True,
        connection_class= RequestsHttpConnection
        )
print("printing opensearch object")
print(es)
for data in dumpdata:
    l=[]
    for j in data['categories']:
        l.append(j['alias'])
    record= { "name": data['name'], "cuisine": l}
    y= json.dumps(record, indent=4)
    es.index(index="search-restaurants", doc_type= "_doc", id= data['id'], body=y)
print(succes)

