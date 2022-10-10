import os
from elasticsearch import Elasticsearch
import json
import es_management as es1
import getDataFromApi as gf
from constant import index_name, ELASTIC_SEARCH_HOST
es = Elasticsearch([ELASTIC_SEARCH_HOST])

result=[]
md5s = []
elasticquery={"query":{"match_all":{}},"size":10000}
res = es.search(index="similarity_etl_auto1",body=elasticquery)

if not os.path.exists("indexResult.json"):
    try:
        with open("indexResult.json","x") as f:
            print("File created successfully")
    except:
        print("File can not be created")
else:
    print("file already exists")   
for hit in res['hits']['hits']:
    with open('indexSchema.json', 'r') as destination_file:
        destination_object = json.load(destination_file)
        destination_parent_source = destination_object.get("_source")
        source = hit.get('_source')
        if (source['isdefault']).lower()=='yes':
            destination_parent_source['md5hash']=source.get('md5hash')
            destination_parent_source['familyid']=source.get('familyid')
            destination_object['_id'] = hit['_id']
            destination_parent_source['bestsellersrank']=source.get('bestsellersrank')
            styles = source.get('styles')[0]
            destination_parent_source['classification']=styles.get('classes')
            missing_data = gf.getDataFromApi([source.get('md5hash')])
            
            missing_mltags_data= missing_data.get("mltags")
            if missing_mltags_data:
                missing_mltags = missing_mltags_data.get(source['md5hash']) 
                if missing_mltags:
                    for tag in missing_mltags:
                        mltags = missing_mltags.get(str(tag))
                        confidence = mltags.get('confidence')
                        obj = {
                            str(tag): confidence,
                        }  
                        destination_parent_source['mltags'].append(obj)   
            
        result.append(destination_object)
    with open('indexResult.json','w')as file:
        json.dump(result,file)

obj = es1.EsManagement()
obj.populate_index( 'indexResult.json', index_name )


