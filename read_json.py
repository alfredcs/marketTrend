import json
import argparse
  
parser = argparse.ArgumentParser()
parser.add_argument('-I', '--input-file', required=True, help='The input file path ex) ./online_retail.json')

options=parser.parse_args()
# Opening JSON file
print(options.input_file)
f = open(options.input_file)
  
# returns JSON object as 
# a dictionary
data = json.load(f)
  
# Iterating through the json
# list
for i in data['id']:
    print(i)
  
# Closing file
f.close()
