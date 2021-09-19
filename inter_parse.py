import pandas as pd
import json
import pysolr
import math
import traceback
import logging
import sys
import datetime
from datetime import date


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def indextoSolr(shost, data, max_doc_num=1000):
    solr = pysolr.Solr(shost, always_commit=True)
    doclist = []
    try:
        # print(data)
        for doc in data:
            if len(doclist) > max_doc_num:
                solr.add(doclist)
                doclist.clear()
            else:
                doclist.append(doc)
        if len(doclist) > 0:
            solr.add(doclist)
            doclist.clear()

        logging.info('Feed Refreshed Succesfully')
    except Exception as e:
        logging.error(traceback.format_exc())

def feeddownload():
    try:
        data = pd.read_csv("https://feeds.datafeedwatch.com/20707/8098e7358e4e1da0cd6204913b8a576d1e75fc95.csv",delimiter="|", header = 0)
        logging.info('Feed exchange complete')
        return(data)
    except Exception as e:
        logging.error(traceback.format_exc())


#Initializations
columnnames=[]
final_data=[]
a={}
dict1=[]
# orgID_s = "53a829d8-51de-4a3c-83f6-48a6681c3e02"
orgID_s = "53a829d8-51de-4a3c-83f6-48a6681c3e02"
org_s = "intertoys"


# #ReadCSV
# data = pd.read_csv("./02102020.csv",delimiter="|", header = 0)
# # print(data)
count=0
count_final=0
pudate_dt = (datetime.datetime.now().isoformat())+"Z"

data = feeddownload()
for i, j in data.iterrows():
    a={}
    count+=1
    a["name_s"] = str(j[1])
    if (a["name_s"]!="nan" and str(j["url_key"])!="nan" and str(j["product_main_image_url"])!="nan" and str(j["price"])!="nan"):
        a["cannoical_url_s"] = {"set":j[2]}
        a["sku_s"] = {"set":str(j[0])}
        # print(str(j[0]))
        a["product_cannoical_url_s"] = {"set":j[2]}
        a["smallImage_s"] = {"set":j[12]}
        a["page_reachable_b"] = {"set":True}
        a["org_s"] = {"set":org_s}
        a["status_b"] = {"set":False}
        doc_id=str(orgID_s) +"_"+str(j[0])
        a["doc_id_s"] = doc_id
        a["namel_t"] = {"set":a["name_s"].lower()}
        a["description_t"] = {"set":j[7]}
        a["price_d"] = {"set":j[3]}
        a["namel_s"] = {"set":a["name_s"].lower()}
        a["client_os_type_s"] = {"set":"magento"}
        a["image_base_url_s"] = {"set":j[12]}
        a["event_type_s"] = {"set":"productinfo"}
        a["orgID_s"] = {"set":str(orgID_s)}
        a["visibility_b"] = {"set":True}
        a["product_id_i"] = {"set":j[0]}
        a["thumbImage_s"] = {"set":j[12]}
        a["typeid_s"] = {"set":"simple"}
        a["image_s"] = {"set":j[12]}
        a["name_t"] = {"set":j[1]}
        # print(j["instock"])
        if((math.isnan(float(j["instock"])))==False):
            a["stock_d"] = {"set":j["instock"]}
        else:
            a["stock_d"]={"set":0}
        if(str(j[4])!=" "):
            weight_proc = j[4].split(",")
            weight_proc = float(str(weight_proc[0])+"."+str(weight_proc[1]))
            a["weight_i"] = {"set":weight_proc}
        if((math.isnan(float(j["agefrom"])))==False):
            a["age_from_i"] = {"set":j["agefrom"]}
        else:
            a["age_from_i"] = {"set":0}
        if((math.isnan(float(j["ageto"])))==False):
            a["age_to_i"] = {"set":j["ageto"]}
        else:
            a["age_to_i"] = {"set":999}
        if((math.isnan(float(j["listprice"])))==False):
            a["discounted_price_d"] = {"set":j["listprice"]}
        else:
            a["discounted_price_d"] = {"set":0}
        a["release_date_s"] = {"set":str(j["releasedate"])}
        a["pupdated_at_dt"]= {"set":str(pudate_dt)}
        logging.debug(a["sku_s"])
        # x=json.dumps(a)
        dict1.append(a)
        # print(dict1["name_t"])
        # dict1[i]=a
        count_final+=1
    else:
        final_data.append(str(j[0]))
        count_unprocessed=len(final_data)
        
logging.info("Unprocessed due to Missing fields:"+str(count_unprocessed))
logging.info("Unprocessed due to Missing fields:"+str(final_data))
logging.info("Processed Count:"+str(count_final))
logging.info("Total Count:"+str(count))

# print(dict1)
# print(final_data)
indextoSolr("http://161.35.155.132:8983/solr/product_listings", dict1)

# print(dict1)
# print(count,count_final)
# print(final_data)



# with open('product_profile_inter_1.json', 'w') as outfile:
#     json.dump(dict1, outfile)



# with open('product_profile_inter_1.json') as json_file:
#     data = json.load(json_file)


# # print(data)

# for x in data:
#     print(x)

 
#curl -X POST -H 'Content-Type: application/json' 'http://161.35.155.132:8984/solr/product_profile/update?commit=true'  --data-binary @/media/vishnu/1TB/ecomtics1/Intertoys/product_profile_inter_1.json
