import os
from os import path
import subprocess
import requests
import lxml
import csv
from zipfile import ZipFile
from urllib.request import urlopen


class MdbDistillator():
    def __init__(self):
        self.temp_dir = 'Temporary'

    def _download_and_extract_zip(self):
        'Returns rows of .csv file'
        tempfile_name = 'temp.zip'
        temp_zip = self.temp_dir + '/' + tempfile_name
        zipresp = urlopen(
            'http://cogcc.state.co.us/documents/data/downloads/spacingorders/CauseOrderTable_Download.zip')
        tempzip = open(temp_zip, "wb")
        tempzip.write(zipresp.read())
        tempzip.close()
        zf = ZipFile(temp_zip)
        zf.extractall(path=self.temp_dir)
        zf.close()
        os.remove(temp_zip)
        return list(filter(path.isfile, os.listdir(self.temp_dir)))[0]

    def get_rows(self):
        'Returns .csv file name'
        mdb_path = self._download_and_extract_zip()
        subprocess.call("mdb-export '%s' COGCC_Spacing_Download' > Temporary/result.csv" % mdb_path,
                        shell=True)
        csv_path = self.temp_dir + '/result.csv'
        try:
            with open(csv_path) as csvfile:
                return csv.reader(csvfile, delimiter=',', quotechar='"')[:]
        finally:
            os.remove(csv_path)

class Spider():
    def load_items(self, csv_rows):
        'Returns dicts, set of cause_nums'
        items = []
        for row in csv_rows:
            items.append({'section': row[1],
                          'twp': row[2],
                          'range': row[3],
                          'meridian': row[4],
                          'section_part': row[5],
                          'cause_num': row[6],
                          'order_num': row[7]})
        pairs = set([d['cause_num'] + '-' + d['order_num'] for d in items])
        return items, pairs

    def scrape(self, pairs):

        for pair in pairs:


class DbWorker():
    def __init__(self):
        password = ''
        with open('creds.txt', 'r') as f:
            password = f.readline()
        self.password = password