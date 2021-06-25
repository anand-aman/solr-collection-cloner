import json
import requests
from flask import Flask, request

from config import solr_host, solr_development_host

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/read_collection')
def read_collection():
    try:
        doc_count = request.args.get('count')
        collection_name = request.args.get('collection')
        start = request.args.get('start')
        if doc_count is None or collection_name is None:
            raise Exception("Invalid parameter value")
        if start is None:
            start = "0"

        print("Getting Response")
        solr_query = solr_host + collection_name + "/select?q=*%3A*&rows=" + str(doc_count) + "&start="+start
        print("Getting JSON")
        response = requests.get(solr_query).json()
        print("Storing documents in to collection_docs variable")
        collection_docs = response['response']['docs']

        print("Writing in File")
        file = open(collection_name + ".json", "w")
        for doc in collection_docs:
            if type(doc) is not dict:
                continue
            doc.pop('doc_index_dt')
            doc.pop('_version_')
            # doc.pop('id')
            try:
                file.write(doc.__str__() + "\n")
            except Exception as e:
                print(doc)
                print(e)
                continue

        file.close()
        print("Collection copy process completed")
    except Exception as e:
        print(e)
        return e.__str__()
    return response['responseHeader']


@app.route('/write_collection')
def write_collection():
    try:
        collection_name = request.args.get("collection")
        if collection_name is None:
            raise Exception("Invalid parameter value")

        print("Reading file")

        file = open(collection_name + ".json", 'r')
        lines = file.readlines()
        solr_host_update = solr_development_host + collection_name + "/update/json?commit=true"

        print("Pushing data into Solr")
        count = 0
        docs = []
        for line in lines:
            count += 1
            try:
                json_object = eval(line)  # To convert str to dict
                # response = requests.post(url=solr_host_update, json=json_object)
                # print(json_object)
                docs.append(json_object)
                if len(docs) == 500:
                    payload = json.dumps(docs)
                    response = requests.post(url=solr_host_update, data=payload)
                    print(count)
                    print(response)
                    docs.clear()
            except Exception as e:
                print(e.__str__())
                continue
        print(count)
        payload = json.dumps(docs)
        response = requests.post(url=solr_host_update, data=payload)
        print(response)
        file.close()

        return "Collection added successfully"
    except Exception as e:
        return e.__str__()


if __name__ == '__main__':
    app.run(debug=True)
