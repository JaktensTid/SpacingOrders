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
            trs = document.xpath("//table[@cellpadding='4' and @border='0']//tr")
            for tr in trs:
                cause_year = filter_list(list(tr.xpath(".//td[position()=1]//text()")))
                desc = tr.xpath(".//td[position()=2]//text()")
                for c_y, d in zip(cause_year, desc):
                    if len(cause_year) == 2:
                        result += [(cause_year[0], cause_year[1], d)]
                    if len(cause_year) == 1:
                        result += [(cause_year[0], '', d)]
        except Exception:
            pass


async def bound_fetch(sem, url, session):
    async with sem:
        try:
            await fetch(url, session)
        except Exception as e:
            print('Error')
            print(str(e))
            import time
            time.sleep(10000)


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


def collect_descriptions():
    if os.path.isfile('res.csv'):
        print('Result exists')
        orders = []
        with open('res.csv') as file:
            for row in csv.reader(file):
                orders.append(row)
        causes = list(set([row[0] for row in orders[1:]]))
        print('causes count: ' + str(len(causes)))
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(causes))
        loop.run_until_complete(future)
        next = []
        for order in orders:
            for item in result:
                c_o = item[0].split('-')
                cause, order_num = c_o[0].strip(), c_o[1].strip()
                if cause == order[0] and order_num == order[1]:
                    order += [item[-2], item[-1]]
                    next.append(order + [item[-2], item[-1]])
        with open('main_res.csv', 'w', encoding='utf-8') as file:
            writer = csv.writer(file,delimiter=',',
                            quotechar='"')
            writer.writerow(['Cause num', 'Order num', 'Doc', 'Name', 'Date', 'Desc'])
            for order in next:
                writer.writerow(order)
        print('Over')

if __name__ == '__main__':
    collect_descriptions()
