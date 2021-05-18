import boto3
import csv
import json
from elasticsearch import Elasticsearch
import datetime

s3 = boto3.client("s3")
translate = boto3.client("translate")
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
es=Elasticsearch(['https://search-twitter-c4idtc5xplyaujgttxrwhyoede.us-west-2.es.amazonaws.com/'], http_auth=('mltest', 'take2tEZ!'))
    

bucketname = 'public-datasets-multimodality'
filename='json-data/year=2021/month=05/day=12/'
fileObjds = s3.list_objects(Bucket=bucketname, Prefix='json-data/year=2021/')
fileObj = s3.get_object(Bucket=bucketname, Key='json-data/year=2021/month=05/day=15/hour=21twitter-1-2021-05-15-21-30-55-940c8719-297e-4eb5-bb97-87881de50ceb')

file_content = fileObj["Body"].read().decode('utf-8').splitlines(True)


reader = csv.reader(file_content, delimiter='|')
table = dynamodb.Table('tweetSentiment')
sentiment_list=['ar', 'hi', 'ko', 'zh-TW', 'ja', 'zh', 'de', 'pt', 'it', 'fr', 'es']
translatee_list=['af','sq','am','ar','hy','az','bn','bs','bg','ca','zh','zh-TW','hr','cs','da','fa-AF','nl','et','fa','Tagalog','fi','fr','fr-CA','ka','de','el','gu','ht','ha','he','hi','hu','is','id','it','ja','kn','kk','ko','lv','lt','mk','ms','ml','mt','mn','no','fa','ps','pl','pt','ro','ru','sr','si','sk','sl','so','es','es-MX)','sw','sv','tl','ta','te','th','tr','uk','ur','uz','vi','cy']
for row in reader:
    if len(row)<4:
         continue
    language=row[5] if len(row)==6 else 'en'
    # Translte
    if language in translatee_list and len(row[4])>1:
         try:
         	translate_output = translate.translate_text(Text=row[4], SourceLanguageCode=language, TargetLanguageCode="en")['TranslatedText']
         except:
         	pass
    else:
         translate_output = row[4]

    # Sentiment from Comprehend
    #language='en' if language not in sentiment_list else 'en'
    if language not in sentiment_list:
         language='en'
    if len(row[4]) > 1: 
         #Comprehend limit text input at 5000
         try:
         	sentiment_output=comprehend.detect_sentiment(Text=(row[4][:4999]) if len(row[4])>4999 else row[4], LanguageCode=language)['Sentiment']
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

    # Insert into Dynamodbi
    str1 = row[1].split(' ')
    c_time = datetime.datetime.fromisoformat(str1[0]+'T'+str1[1])
    try:
        table.put_item(
            Item={
                'Created_at': row[1],
                'ID': row[0],
                'Location': row[3],
                'Tweet': translate_output,
                'Sentiment': sentiment_output,
                'Entities': entities
                }
        )   
    except:
        print('Dynamodb insert failed on '+ row[0])
        pass
    
    # Update ElastocSearch
    Item2={
        'Created_at': c_time,
        'ID': row[0],
        'Location': row[3],
        'Tweet': translate_output,
        'Sentiment': sentiment_output,
        'Entities': entities
    }
    try: 
        res = es.index(index="sentiment", doc_type='_doc', id=row[0], body=Item2, request_timeout=45)
    except:
        print(res['result'])
        pass
