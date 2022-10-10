import sys

from elasticsearch import Elasticsearch

import pandas as pd
from constant import similarity_index_name

num_args = len(sys.argv)
if num_args < 2:
    exit("Error: Please provide tag name")

client = Elasticsearch(
    hosts=[{'host': "ec2-54-152-53-80.compute-1.amazonaws.com",
            'port': 9200}])

df = pd.read_csv("data/data_files/" + sys.argv[1] + "/rawInput/new.csv")
df = df[df["cp_tag"]=="Yes"]
missing_list = list()
style_present = dict()
for i, row in df.iterrows():
    style_present[row['md5']] = True

for i, row in df.iterrows():
    elasticquery={
        "query":{
            "match":{
                "md5hash":row['md5']
            }
        }
    }
    result = client.search(index = similarity_index_name, body =  elasticquery)
    try:
        family_id = result.get('hits').get('hits')[0].get('_source').get('familyid')
    except Exception as e:
        raise Exception("familyid not found for given md5: ", row['md5'], e)
        
    elasticquery={
        "query":{
            "match":{
                "familyid":family_id
            }
        },
        "_source":["stylename","md5hash"]
    }
    response= client.search(index="similarity_etl_auto1",body=elasticquery)

    if len(response['hits']['hits']) > 0:
        styles = response['hits']['hits'][0]['_source']['styles']
        for i in range(len(styles)):
            try:
                if styles[i]['md5hash'] not in style_present:
                    print(styles[i]['md5hash'], styles[i]['stylename'])
                    missing_list.append([styles[i]['md5hash'],
                                    '=Image("https://render.myfonts.net/fonts/font_rend.php?id=' +
                                    styles[i]['md5hash'] + \
                                    '&rt=The%20quick%20brown%20fox%20jumps%20over%20the%20lazy%20dog.&rs=200&fg=000000&bg=FFFFFF&w=3000")'])
            except:
                pass
df_missing = pd.DataFrame(missing_list, columns=['md5', 'image_url'])
df_missing=df_missing.drop_duplicates(subset=['md5'], keep='first')
df_missing.to_csv("data/data_files/"+sys.argv[1]+"/input/missing_styles.csv")