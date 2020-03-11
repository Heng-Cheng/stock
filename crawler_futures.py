import random
from bs4 import BeautifulSoup as bs
import requests
import asyncio
import aiohttp
import csv
from datetime import datetime, timedelta
import time
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

class Crawler_hk_Investing():
    def __init__(self, day):
        self.day = day
        self.total_error = 0
        self.start_time = None
        self.end_time = None
        self.url = 'https://hk.investing.com/instruments/HistoricalDataAjax'
        self.session = requests.Session()
        self.result_data = {}
        self.id_dict = {
            'USD': 2206,
            'CNY': 9540,
            'JPY': 2075,
            'EUR': 1707,
            'Gold': 8830,
            'Silver': 8836,
            'Oil': 8833,
            'Copper': 8831,
            'Dji30': 8873,
            'SP500': 166,
            'N225': 178,
            'SSEC': 40820,
            'HSI': 179,
            'SZI': 942630
        }

    def user_agent_list(self):
        UA_list = [
            "Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19",
            "Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
            "Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
            "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"
        ]
        result = random.choice(UA_list)
        return result

    async def request_data(self, key):
        tmp_dict = {}
        async with aiohttp.ClientSession() as session:
            await session.get(url='https://hk.investing.com/')
            data = {
                'curr_id': self.id_dict[key],
                'st_date': '2010/01/04',
                'end_date': self.day,
                'interval_sec': 'Daily',
                'sort_col': 'date',
                'sort_ord': 'ASC',
                'action': 'historical_data',
            }
            headers = {
                'Host': 'hk.investing.com',
                'Origin': 'https://hk.investing.com',
                'Referer': 'https://hk.investing.com/equities/taiwan-semicond.manufacturing-co-historical-data',
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': self.user_agent_list(),
                'X-Requested-With': 'XMLHttpRequest'
            }
            async with session.post(url=self.url, headers=headers, data=data) as response:
                response = await response.text(encoding='utf-8', errors='ignore')

                bs_result = bs(response, features="lxml")
                bs_result = bs_result.find_all('tr')
                for b in bs_result[::-1][1:]:
                    td = b.find_all('td')
                    if len(td) > 0:
                        time = datetime.strptime(td[0].text, '%Y年%m月%d日')
                        tmp_dict[time.strftime("%Y%m%d")] = td[1].text

            self.result_data.update({key: tmp_dict})

    async def run(self):
        tasks = []
        for key, value in self.id_dict.items():
            tasks.append(asyncio.create_task(self.request_data(key)))
        await asyncio.wait(tasks, timeout=4)
        await asyncio.sleep(0)

def update_futures():
    start = time.time()
    day = datetime.today() - timedelta(days=1)
    day = day.strftime('%Y/%m/%d')
    chi = Crawler_hk_Investing(day)
    asyncio.run(chi.run())
    data = chi.result_data
    path = './futures/futures.csv'  # 路徑＋檔案名稱
    with open(path, 'w', newline='', encoding='utf-8') as f:
        write_csv = csv.writer(f)
        title = ['date']  # 日期title名稱
        for i in chi.id_dict.keys():
            title.append(i)
        write_csv.writerow(title)
        t = 0
        for k, v in data.items():
            if len(v) > t:
                t = len(v)
                r = k
        for k, v in data[r].items():
            tmp_list = [k]
            for i in chi.id_dict.keys():
                tmp_list.append(data[i].get(k, ''))
            write_csv.writerow(tmp_list)
    end = time.time()
    print(end - start)

if __name__ == '__main__':
    update_futures()
