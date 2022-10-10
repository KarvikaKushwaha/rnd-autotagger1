import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import deque
from constant import ELASTIC_SEARCH_HOST, ES_TIMEOUT

logging.basicConfig(filename = 'es_management.log', level = logging.INFO)

logger = logging.getLogger(__name__)
class EsManagement:
    def __init__(self):
        self.es_client = Elasticsearch(
            ELASTIC_SEARCH_HOST,timeout=ES_TIMEOUT
        )
    
    
    def __read_data(self,JSON_PATH:str) -> list:
        """
        Read data from path 
        :param path: The path to the JSON file
        """
        with open(JSON_PATH, 'r') as data_file:
            json_data = data_file.read()
        data = json.loads(json_data)
        return data
        


    def populate_index(self, path: str, index_name: str) -> None:
        """
        Populate an index from a JSON file.
        :param path: The path to the JSON file.
        :param index_name: Name of the index to which documents should be written.
        """
        
        try:
            data =  self.__read_data(path)
            for i in range(0, len(data)):
                if data[i]['_score']:
                    del data[i]['_score'] 
                data[i]['_index'] = index_name
    
            logger.info(f"Writing {len(data)} documents to ES index {index_name}")
            pb_gen = helpers.parallel_bulk(client=self.es_client, actions=data,thread_count = 5,queue_size=100)
            deque(pb_gen, maxlen=0)
            print(f"{index_name} Populated successfully")
            logger.info(f"{index_name} Populated successfully")
        except Exception as e:
            print("unable to populate index ")
            logging.exception(str(e))



