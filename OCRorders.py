from pymongo import MongoClient
from multiprocessing.pool import Pool
import requests
import csv
import os
import json

priorities = ['application', 'exhibit', 'interested part']
total_ocred = 0

def get_collection():
    client = MongoClient(os.environ['MONGODB_URI'])
    db = client['heroku_zsl3pl6l']
    return db.orders

def make_url(doc_number, doc_url):
    return 'http://35.165.174.118:8080/plss?docNo=' + doc_number + '&url=%27http://ogccweblink.state.co.us/' + doc_url + '%27'


def handler(d):
    print('Thread is fetching ' + d['doc'])
    collection = get_collection()
    def ocr(row):
        global total_ocred
        doc_url = row[2].replace("['", '').replace("']", '')
        doc_number = row[0]
        try:
            response = requests.get(make_url(doc_number, doc_url))
            d = json.loads(response.content.decode('utf-8'))
            if d['ocrFailed'] == 'true':
                print('Fetching ' + doc_number + ' - ' + doc_url + ' again')
                ocr(row)
            total_ocred += 1
            d['causeNum'] = row[0]
            d['orderNum'] = row[1]
            d['docLink'] = row[2].replace("['", '').replace("']", '')
            d['name'] = row[3].replace("['", '').replace("']", '')
            d['docDate'] = row[4]
            d['description'] = row[5]
            d['_id'] = d['docLink']
            collection.insert_one(d)
            print('Fetched ' + doc_number + ' - ' + doc_url + '. Total fetched: ' + str(total_ocred))
        except Exception as e:
            print(str(e) + ' - ' + doc_url)
    for row in d['rows']:
        ocr(row)


def ocr_all():
    def check_priority(name):
        for priority in priorities:
            if priority in name:
                return True
        return False
    if os.path.isfile('main_res.csv'):
        with open('main_res.csv', 'r', newline='') as file:
            reader = csv.reader(file, delimiter=',', quotechar='"')
            rows = [row for row in reader if check_priority(row[3])]
        grouped = []
        for docno in list(set([row[0] for row in rows])):
            d = {'doc' : docno, 'rows' : []}
            for row in rows:
                if row[0] == docno:
                    d['rows'].append(row)
            grouped.append(d)
        with Pool(2) as p:
            p.map(handler, grouped)
    print('Over')


if __name__ == '__main__':
    ocr_all()