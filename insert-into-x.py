import boto3
import csv
import json
import pandas as pd
import numpy as np
import datetime
import uuid
import logging, os
import twint
import nest_asyncio
import pandas as pd
import datetime
from elasticsearch import Elasticsearch
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
translate = boto3.client("translate")
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
es=Elasticsearch(['https://search-twitter-c4idtc5xplyaujgttxrwhyoede.us-west-2.es.amazonaws.com/'], http_auth=('<username_changeme>', '<password_changeme>'))
now = datetime.datetime.now()
local_filename="twitter_"+str(int(datetime.datetime.timestamp(now)))+".csv"

def isNaN(str):
    str != str

def to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

def get_tweets(key_words, filename, since_date):
    c = twint.Config()
    #c.Search = "neo4j OR \graph database\ OR \graph databases \ OR graphdb OR graphconnect OR @neoquestions OR @Neo4jDE OR @Neo4jFr OR neotechnology"
    #c.Search = "memory OR \flash memory\ OR \sk hynix\ OR sandisk OR intel OR micron OR microship OR samsung OR kioxia OR marvell OR \on semiconductor\ OR Infineon OR \flash memory\ OR nand"
    #c.Search = "cisco OR \Juniper Networks\ OR \hpe aruba\ OR huawei  OR \arista networks\ OR netgear OR vmware OR \extreme networks\ OR dell"
    c.Search = key_words
    #c.Store_json = True
    c.Store_csv = True
    #c.Custom[\user\] = [\\, \tweet\, \user_id\, \username\, \hashtags\, \mentions\]g
    c.User_full = True
    c.Output = filename
    c.Since = since_date
    c.Hide_output = True
    twint.run.Search(c)
    return True

def make_json(csvFilePath, jsonFilePath):
    # create a dictionary
    data = {}

    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # Convert each row into a dictionary
        # and add it to data
        for rows in csvReader:
            # Assuming a column named 'No' to
            # be the primary key
            key = rows['id']
            data[key] = rows

    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))


'''
# Read from S3 for csv file
bucketname = 'public-datasets-multimodality'
filename='json-data/year=2021/month=05/day=12/'
fileObjds = s3.list_objects(Bucket=bucketname, Prefix='json-data/year=2021/')
fileObj = s3.get_object(Bucket=bucketname, Key='json-data/year=2021/month=05/day=14/hour=22twitter-1-2021-05-14-22-13-42-9a50f05f-5376-4cf6-ba4d-1a4e15a65c12')
file_content = fileObj["Body"].read().decode('utf-8').splitlines(True)
reader = csv.reader(file_content, delimiter='|')
'''



# Get tweets
try:
    get_tweets("cisco", local_filename, "2021-05-17")

except Exception as e:
    logging.error(e)

# Get from a local CSV
file_content = open(local_filename)
col_list = ["conversation_id","created_at","timezone","place","tweet","language"]
data = pd.read_csv(file_content, usecols=col_list)
#reader = csv.reader(file_content, delimiter=',', quotechar='|')
result_file = str(uuid.uuid4())
row_list = [[]]
table = dynamodb.Table('tweetSentiment')
sentiment_list=['ar', 'hi', 'ko', 'zh-TW', 'ja', 'zh', 'de', 'pt', 'it', 'fr', 'es']
translatee_list=['af','sq','am','ar','hy','az','bn','bs','bg','ca','zh','zh-TW','hr','cs','da','fa-AF','nl','et','fa','Tagalog','fi','fr','fr-CA','ka','de','el','gu','ht','ha','he','hi','hu','is','id','it','ja','kn','kk','ko','lv','lt','mk','ms','ml','mt','mn','no','fa','ps','pl','pt','ro','ru','sr','si','sk','sl','so','es','es-MX)','sw','sv','tl','ta','te','th','tr','uk','ur','uz','vi','cy']
#for row in reader:
for i in range(len(data)):
    language=data['language'] [i]
    # Translte
    if language in translatee_list and len(data['tweet'][i])>1:
         try:
         	translate_output = translate.translate_text(Text=data['tweet'][i], SourceLanguageCode=language, TargetLanguageCode="en")['TranslatedText']
         except:
         	pass
    else:
         translate_output = data['tweet'][i]

    # Sentiment from Comprehend
    #language='en' if language not in sentiment_list else 'en'
    if language not in sentiment_list:
         language='en'
    if len(data['tweet'][i]) > 1:
         #Comprehend limit text input at 5000
         try:
         	sentiment_output=comprehend.detect_sentiment(Text=(data['tweet'][i][:4999]) if len(data['tweet'][i])>4999 else data['tweet'][i], LanguageCode=language)['Sentiment']
         except:
         	pass
    else:
         sentiment_output=''

    # Entities
    try:
    	nerr=json.dumps(comprehend.detect_entities(Text=translate_output[:4999] if len(translate_output)>4999 else translate_output, LanguageCode='en'))
    except:
    	pass
    ners=json.loads(nerr)
    entities=''
    for nn in ners['Entities']:
    	entities += nn['Text']+' '

    # Insert into Dynamodb
    try:
        table.put_item(
            Item={
                'Created_at': data['created_at'][i],
                'ID': to_str(data['conversation_id'][i]),
                'Location': data['place'][i] if isNaN(data['place'][i]) else '-',
                'Tweet': translate_output,
                'Sentiment': sentiment_output,
                'Entities': entities
                }
            )
    except:
        pass

    # Update ElasticSearch

    str1 = data['created_at'][i].split(' ')
    c_time = datetime.datetime.fromisoformat(str1[0] + 'T' + str1[1])
    Item2 = {
        'Created_at': c_time,
        'ID': to_str(data['conversation_id'][i]),
        'Location': data['place'][i] if isNaN(data['place'][i]) else '',
        'Tweet': translate_output,
        'Sentiment': sentiment_output,
        'Entities': entities
    }
    try:
        res = es.index(index="sentiment", doc_type='_doc', id=to_str(data['conversation_id'][i]), body=Item2, request_timeout=45)
    except:
        print(res['result'])
        pass

    # Publish a json into aws-athena-streaming bucket
    row_list.append([to_str(data['conversation_id'][i]), c_time, data['place'][i] if isNaN(data['place'][i]) else '', sentiment_output, entities,translate_output])

with open(result_file+'.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['id', 'created_at', 'location', 'sentiment', 'entities', 'tweet'])
    writer.writerows(row_list)

try:
    # make_json(result_file+'.csv', result_file+'.json')
    #os.remove(result_file + '.json')
    with open(result_file+'.json', 'w', newline='') as file:
        with open(result_file+'.csv', 'r') as data:
            for line in csv.DictReader(data):
                file.write(str(line) + '\n')
except ClientError as e:
    logging.error(e)

# Upload to S3
try:
     response = s3.upload_file(result_file+'.json', 'aws-athena-buffering', result_file+'.json')
except ClientError as e:
    logging.error(e)
try:
    os.remove(result_file+'.csv')
    os.remove(result_file + '.json')
except OSError:
    pass
