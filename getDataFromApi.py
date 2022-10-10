import numpy as np
import requests
import logging
from constant import SFDMS_API_URL, BATCH_SIZE_SFDMS    

def getDataFromApi(md5s):
    ''' 
    fetches data from sfdms api
    :param md5s: list of md5s
    '''
    logging.basicConfig(filename = 'getDataFromApi.log', level = logging.INFO)
    start_index = 0
    no_of_loops = np.ceil(len(md5s)/BATCH_SIZE_SFDMS)
    mltags={}
    
    try:
        for i in range(1,int(no_of_loops)+1):
            next_index = int((i * BATCH_SIZE_SFDMS) - 1)
            payload = ",".join(list(set(md5s[start_index:next_index])))
            
            url = SFDMS_API_URL + payload
            res = requests.get(url)
            x= res.json()
            results = x["result"]        
            for result in results[0:BATCH_SIZE_SFDMS]:
                try:
                    md5 = result["mdFive"]
                    mltgs = result["tags"]

                    mltags.update({md5 :mltgs})
                except Exception as e:
                    print("fatal...",result["mdFive"], e)

            start_index = next_index
    except Exception as e:
        print("Unable to fetch data from api")
        logging.exception(str(e))
    return {
    "mltags":mltags
}