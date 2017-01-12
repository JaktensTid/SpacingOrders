from lxml import html
import asyncio
import csv
import os
from aiohttp import ClientSession

result = []

def chunks(l, n):
    for i in range(0, len(l), n):
        yield filter_list(l[i:i + n])

def filter_list(l):
    l = [str(item).strip() for item in l]
    return list(filter(None, l))

async def fetch(url, session):
    print('Scraping: ' + url)
    async with session.get(url) as response:
        content = await response.read()
        global result
        try:
            document = html.fromstring(content)
            causes_dates = document.xpath("//table[@cellpadding='4' and @border='0']//tr/td[position()=1]//text()")
            descriptions = document.xpath("//table[@cellpadding='4' and @border='0']//tr/td[position()=2]//text()")
            print(descriptions)
        except Exception:
            pass


async def bound_fetch(sem, url, session):
    async with sem:
        await fetch(url, session)


async def run(causes):
    url_sceleton = 'http://cogcc.state.co.us/Orders/orders.cfm?cause_num=%s'
    urls = [url_sceleton % cause for cause in causes]
    tasks = []
    sem = asyncio.Semaphore(1000)

    async with ClientSession() as session:
        for url in urls:
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        print(result)


def collect_descriptions():
    if os.path.isfile('res.csv'):
        print('Result exists')
        orders = []
        with open('res.csv') as file:
            for row in csv.reader(file):
                orders.append(row)
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(list(set([row[0] for row in orders[1:]]))[:5]))
        loop.run_until_complete(future)

if __name__ == '__main__':
    collect_descriptions()