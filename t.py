import json

data = []
with open('t.json') as f:
    for line in f:
        #data.append(json.loads(line))
        print(json.loads(line))
        print("-------")
