from pymongo import MongoClient
from multiprocessing.pool import Pool
import requests
import csv
import os
import json

priorities = ['parties', 'application', 'exhibit']


def get_collection():  # MONGODB_URI
    client = MongoClient(os.environ['MONGODB_URI'])
    db = client['heroku_zsl3pl6l']
    return db.orders


def make_url(doc_number, doc_url):
    return 'http://35.165.174.118:8080/plss?docNo=' + doc_number + '&url=%27http://ogccweblink.state.co.us/' + doc_url + '%27'


def handler(d):
    print('Thread is fetching ' + d['cause'] + '-' + d['order'])
    collection = get_collection()

    def insert(res):
        if not res:
            return 0
        if 'error' in res:
            print('Error in res')
            return 0
        if res['normAddress']:
            print('Got something in ' + d['cause'] + '-' + d['order'])
            collection.insert_one(res)
            return 1
        return 0

    def ocr(row, times=1):
        doc_url = row[2].replace("['", '').replace("']", '')
        if doc_url == 'DownloadDocument.aspx?DocumentId=':
            return {'normAddress': []}
        doc_number = row[0]
        try:
            response = requests.get(make_url(doc_number, doc_url))
            d = json.loads(response.content.decode('utf-8'))
            d['causeNum'] = row[0]
            d['orderNum'] = row[1]
            d['docLink'] = row[2].replace("['", '').replace("']", '')
            d['name'] = row[3].replace("['", '').replace("']", '')
            d['docDate'] = row[4]
            d['description'] = row[5]
            d['docUrl'] = d['docLink']
            return d
        except requests.exceptions.ConnectionError as e:
            print(str(e))
            return None
        except Exception as e:
            if str(e) == 'ocrFailed':
                if times < 3:
                    return ocr(row, times=times + 1)
            print(str(e) + ' - ' + doc_url)

    last_scraped = None
    for row in d[priorities[0]]:
        res = ocr(row)
        if res and not last_scraped:
            last_scraped = res
        flag = insert(res)
        if flag:
            return

    for row in d[priorities[1]]:
        res = ocr(row)
        if res and not last_scraped:
            last_scraped = res
        flag = insert(res)
        if flag:
            return

    for row in d[priorities[2]]:
        res = ocr(row)
        if res and not last_scraped:
            last_scraped = res
        flag = insert(res)
        if flag:
            return

    if last_scraped:
        print('Inserting emtpy doc for ' + d['cause'] + '-' + d['order'])
        collection.insert(last_scraped)
        return

    print('Nothing fetched in ' + d['cause'] + '-' + d['order'])


def ocr_all():
    def check_priority(name):
        for priority in priorities:
            if priority in name:
                return True
        return False

    if os.path.isfile('main_res.csv'):
        collection = get_collection()
        scraped_docs = collection.find()
        scraped_orders = []
        for doc in scraped_docs:
            scraped_orders.append((doc['causeNum'], doc['orderNum']))
        with open('main_res.csv', 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',', quotechar='"')
            rows = [row for row in reader]
        grouped = []
        for order in [t for t in list(set([(row[0], row[1]) for row in rows])) if t not in scraped_orders]:
            d = {'cause': order[0], 'order': order[1]}
            doc_rows = [row for row in rows if row[0] == order[0] and row[1] == order[1]]
            d[priorities[0]] = [row for row in doc_rows if priorities[0] in row[3].lower()]
            d[priorities[1]] = [row for row in doc_rows if priorities[1] in row[3].lower()]
            d[priorities[2]] = [row for row in doc_rows if priorities[2] in row[3].lower()]
            grouped.append(d)
        with Pool(2) as p:
            p.map(handler, grouped)
        print(len(grouped))
    print('Over')


if __name__ == '__main__':
    ocr_all()
