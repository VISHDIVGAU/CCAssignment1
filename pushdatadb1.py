import json
import boto3
import time
from decimal import Decimal
dumpdata=[]
with open('yelp-data.json') as json_file:
    dumpdata= json.load(json_file)['businesses']

dynamodb= boto3.resource('dynamodb',region_name='us-east-1',aws_access_key_id = 'AKIAQXCNDHWYIJQ5MYTZ', aws_secret_access_key='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')
table=dynamodb.Table('yelp-restaurants')
for data in dumpdata:
    l=[]
    for j in data['categories']:
        l.append(j['alias'])
    tableEntry= {
            "id":data['id'],
            "name": data['name'],
            "address": "".join(data['location']['display_address']),
            "coordinates": str(data['coordinates']['latitude'])+","+str(data['coordinates']['longitude']),
            "review_count": data['review_count'],
            "rating": Decimal(data['rating']),
            "zipcode": data['location']['zip_code'],
            "insertedAtTimestamp": str(time.asctime(time.localtime(time.time())))
            }
    response= table.put_item(Item= tableEntry)
print("sucess")
