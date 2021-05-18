import twint
import json
import nest_asyncio
import boto3
import pandas as pd
from datetime import datetime

now = datetime.now()
local_filename="twitter_"+str(int(datetime.timestamp(now)))+".csv"

def main(key_words, filename, since_date):
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

'''
bucket="public-datasets-multimodality"
s3=boto3.cliner("s3")
s3.upload_file(local_filename, bucket, "json-data/".ocal_filename)
'''

if __name__ == '__main__':
  main("cisco", local_filename, "2021-05-17")
  '''
  col_list = ["conversation_id","created_at","timezone","place","tweet","language"]
  df = pd.read_csv(local_filename, usecols=col_list)
  df.to_csv(local_filename, index=False)
  '''
