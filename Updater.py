import os
from os import path
import subprocess
from lxml import html
import csv
from zipfile import ZipFile
from urllib.request import urlopen
import asyncio
from aiohttp import ClientSession
import selenium


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
            subprocess.call("mdb-export '%s' 'COGCC_Spacing_Download' > %s" % (self.temp_dir + '/' + mdb_path, csv_path),
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

    def load_items(self, csv_rows):
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
        pairs = set([(item[0], item[1]['cause_num'] + '-' + item[1]['order_num']) for i, item in enumerate(items)])
        return items, pairs

    def _insert_tifs(self, response, index, items, webdriver=None, page=1):
        d = items[index]
        document = html.fromstring(response)
        table = document.xpath("//table[@id='WQResultGridView']")
        hrefs = table.xpath('.//tr[position>1]//a[position=1]/@href')
        pages_tr = table.xpath(".//tr[@style='color:White;background-color:#284775;']")
        d['documents'] += hrefs
        if pages_tr:
            if not webdriver:
                webdriver = selenium.webdriver.PhantomJS(os.path.join(os.path.dirname(__file__), 'bin/phantomjs'))
                webdriver.get(self.url_sceleton % d['cause_num'] + '-' + d['order_num'])
            pages = len(pages_tr.xpath('.//a'))
            page += 1
            if page > pages:
                return
            if page != 1:
                webdriver.execute_script("__doPostBack('WQResultGridView','Page$%s')" % page)
            self._insert_tifs(webdriver.page_source, index, items, webdriver, page)

    async def _fetch(self, url, session):
        async with session.get(url) as response:
            return await response.read()

    async def _bound_fetch(self, sem, url, items_i, session):
        async with sem:
            return await self._fetch(url, session), items_i

    async def _run(self, pairs, items):
        tasks = []
        sem = asyncio.Semaphore(200)

        async with ClientSession() as session:
            for pair in pairs:
                task = asyncio.ensure_future(self._bound_fetch(sem, self.url_sceleton % pair[1], pair[0], session))
                tasks.append(task)

            responses = asyncio.gather(*tasks)
            await responses

            for response in responses._result:
                # for each dict appends links to .tif
                # response[0] = response, response[1] = items index
                self._insert_tifs(response[0], response[1], items)

    def scrape(self, pairs, items):

        loop = asyncio.get_event_loop()

        future = asyncio.ensure_future(self._run(pairs, items))
        loop.run_until_complete(future)


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
    spider.scrape(*spider.load_items(rows))

if __name__ == '__main__':
    main()