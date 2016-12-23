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
        if slice:
            return list(pairs)[:slice]
        return list(pairs)

    def _insert_tifs(self, response, pair, hrefs=[], returned_email=False, wd=None, page=1):
        document = html.fromstring(response)
        tables = document.xpath("//table[@id='WQResultGridView']")
        if len(tables) == 0:
            print('Tables len == 0 at ' + self.url_sceleton % pair[0] + '-' + pair[1] + ' - page: ' + str(page))
            return
        table = tables[-1]
        hrefs += set(table.xpath(".//tr[position()>1 and not(@align)]//a[position()=1]/@href"))
        #if 'RETURNED EMAIL' in document.xpath('.//text()'):
        #    returned_email = True
        pages = len(table.xpath(".//tr[@align='left']//a"))
        if pages:
            pages += 1
            if not wd:
                wd = webdriver.PhantomJS(os.path.join(os.path.dirname(__file__), 'bin/phantomjs'))
                wd.get(self.url_sceleton % pair[0] + '-' + pair[1])
            page += 1
            if page > pages:
                wd.close()
                return
            if page != 1:
                wd.execute_script("__doPostBack('WQResultGridView','Page$%s')" % page)
                sleep(2)
            self._insert_tifs(wd.page_source, pair, hrefs, returned_email, wd, page)
        else:
            return

    async def _fetch(self, pair, session):
        try:
            async with session.get(self.url_sceleton % pair[0] + '-' + pair[1]) as response:
                response = await response.read()
                hrefs = []
                returned_email = False
                self._insert_tifs(response, pair, hrefs, returned_email)
                pair += (hrefs, returned_email)
                #print(pair[0] + ' ' + pair[1] + ' ' + str(len(pair[2])))
                return pair
        except aiohttp.errors.ClientOSError:
            print('Error at: ' + self.url_sceleton % pair[0] + '-' + pair[1])
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
    rows = distillator.get_rows()
    spider = Spider()
    start = time.time()
    pairs = spider.scrape(spider.load_items(rows, 50))
    end = time.time()
    print('Execution time: ' + str(end - start))
    with open('res.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(['Cause num', 'Order num', 'Doc', 'Returned email'])
        for pair in pairs:
            spamwriter.writerow([pair[0], pair[1], pair[2], pair[3]])


if __name__ == '__main__':
    main()
