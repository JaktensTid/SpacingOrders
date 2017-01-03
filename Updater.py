import os
import subprocess
from lxml import html
import csv
from time import sleep
from zipfile import ZipFile
from urllib.request import urlopen
import asyncio
import aiohttp
from aiohttp import ClientSession
from selenium import webdriver
import time


class Pair(object):
    def __init__(self, cause_num, order_num, items=[], name=''):
        self.cause_num = cause_num
        self.order_num = order_num
        self.items = items
        self.name = name


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
        return [file for file in os.listdir(self.temp_dir)][0]

    def get_rows(self):
        'Returns .csv file name'
        rows = []
        mdb_path = self._download_and_extract_zip()
        csv_path = self.temp_dir + '/result.csv'
        try:
            subprocess.call(
                "mdb-export '%s' 'COGCC_Spacing_Download' > %s" % (self.temp_dir + '/' + mdb_path, csv_path),
                shell=True)

            with open(csv_path) as csvfile:
                rows = [row for row in csv.reader(csvfile, delimiter=',', quotechar='"')]
        finally:
            os.remove(self.temp_dir + '/' + mdb_path)
            os.remove(csv_path)
        return rows


class Spider():
    def __init__(self):
        self.url_sceleton = 'http://ogccweblink.state.co.us/results.aspx?classid=04&id=%s'

    def load_items(self, csv_rows, slice=0):
        'Returns dicts, pairs'
        items = []
        for row in csv_rows:
            items.append({'section': row[1],
                          'twp': row[2],
                          'range': row[3],
                          'meridian': row[4],
                          'section_part': row[5],
                          'cause_num': row[6],
                          'order_num': row[7]})
        # Get index - pair values
        pairs = set([(d['cause_num'], d['order_num']) for d in items])
        pairs = [Pair(pair[0], pair[1]) for pair in pairs]
        if slice:
            return pairs[:slice], items
        return pairs, items

    def _insert_tifs(self, response, pair, items=[], wd=None, page=1):
        document = html.fromstring(response)
        tables = document.xpath("//table[@id='WQResultGridView']")
        if len(tables) == 0:
            print('Tables len == 0 at ' + self.url_sceleton % pair.cause_num + '-' + pair.order_num + ' - page: ' + str(
                page))
            return
        table = tables[-1]
        tds = table.xpath(".//tr[position()>1 and not(@align)]/td[position()=3]")
        items += map(lambda element: (element.xpath(".//a/@href"), element.xpath(".//a/text()")), tds)
        pages = len(table.xpath(".//tr[@align='left']//a"))
        if pages:
            pages += 1
            if not wd:
                wd = webdriver.PhantomJS(os.path.join(os.path.dirname(__file__), 'bin/phantomjs'))
                wd.get(self.url_sceleton % pair.cause_num + '-' + pair.order_num)
            page += 1
            if page > pages:
                wd.close()
                return
            if page != 1:
                wd.execute_script("__doPostBack('WQResultGridView','Page$%s')" % page)
                sleep(2)
            self._insert_tifs(response=wd.page_source, items=items, pair=pair, wd=wd, page=page)
        else:
            return

    async def _fetch(self, pair, session):
        try:
            async with session.get(self.url_sceleton % pair.cause_num + '-' + pair.order_num) as response:
                response = await response.read()
                items = []
                self._insert_tifs(response=response, items=items, pair=pair)
                pair.items = items
                return pair
        except aiohttp.errors.ClientOSError:
            print('Error at: ' + self.url_sceleton % pair.cause_num + '-' + pair.order_num)
            return pair

    async def _bound_fetch(self, sem, pair, session):
        async with sem:
            return await self._fetch(pair, session)

    async def _run(self, pairs):
        tasks = []
        sem = asyncio.Semaphore(100)

        async with ClientSession() as session:
            for pair in pairs:
                task = asyncio.ensure_future(self._bound_fetch(sem, pair, session))
                tasks.append(task)

            results = asyncio.gather(*tasks)
            await results

            return results._result

    def scrape(self, pairs):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._run(pairs))
        pairs = loop.run_until_complete(future)
        return pairs


class DbWorker():
    def __init__(self):
        password = ''
        with open('creds.txt', 'r') as f:
            password = f.readline()
        self.password = password


def main():
    print('Spacing scraping began')
    distillator = MdbDistillator()
    rows = distillator.get_rows()[1:]
    spider = Spider()
    start = time.time()
    pairs, items = spider.load_items(rows, 50)
    pairs = spider.scrape(pairs)
    end = time.time()
    for item in items:
        for pair in pairs:
            if item['cause_num'] == pair.cause_num and item['order_num'] == pair.order_num:
                item['pair'] = pair
    print('Execution time: ' + str(end - start))
    with open('res.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(
            ['Cause num', 'Order num', 'Doc', 'Name'])
        for pair in pairs:
            for item in pair.items:
                spamwriter.writerow([pair.cause_num, pair.order_num, item[0], item[1]])


if __name__ == '__main__':
    main()
