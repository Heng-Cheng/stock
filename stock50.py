import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import os
import datetime
import time
import csv
import crawler_investor

class Daily_trade_data():
    def __init__(self):

        self.header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
        self.stock_dic =  {
                            '麗正': '2302',
                            '聯電': '2303',
                            '華泰': '2329',
                            '台積電': '2330',
                            '旺宏': '2337',
                            '光罩': '2338',
                            '茂矽': '2342',
                            '華邦電': '2344',
                            '順德': '2351',
                            '矽統': '2363',
                            '菱生': '2369',
                            '瑞昱': '2379',
                            '威盛': '2388',
                            '凌陽': '2401',
                            '南亞科': '2408',
                            '統懋': '2434',
                            '偉詮電': '2436',
                            '超豐': '2441',
                            '京元電子': '2449',
                            '創見': '2451',
                            '聯發科': '2454',
                            '義隆': '2458',
                            '強茂': '2481',
                            '晶豪科': '3006',
                            '聯陽': '3014',
                            '嘉晶': '3016',
                            '聯詠': '3034',
                            '智原': '3035',
                            '揚智': '3041',
                            '立萬利': '3054',
                            '聯傑': '3094',
                            '景碩': '3189',
                            '虹冠電': '3257',
                            '京鼎': '3413',
                            '創意': '3443',
                            '晶相光': '3530',
                            '台勝科': '3532',
                            # '誠創': '3536',
                            '敦泰': '3545',
                            '辛耘': '3583',
                            '通嘉': '3588',
                            '世芯-KY': '3661',
                            '達能': '3686',
                            '日月光投控': '3711',
                            '新唐': '4919',
                            '凌通': '4952',
                            '天鈺': '4961',
                            '十銓': '4967',
                            '立積': '4968',
                            '祥碩': '5269'
                            }
        self.columns = ['date', 'volume', 'open', 'high', 'low', 'close']
    def stock_20(self):
        request = requests.get(
            "https://www.taifex.com.tw/cht/2/elePropertion",
            headers = self.header
        )

        request.encoding = "utf8"
        bsr1 = BeautifulSoup(request.text, "html.parser")

        stock_html_list = bsr1.find_all("td", align="right")
        name_html_list = bsr1.find_all("span", class_="myContent")

        stock_list = [stock.text for stock in stock_html_list]
        stock_list = np.array(stock_list).reshape((-1, 3))
        stock_list = pd.DataFrame(stock_list, columns=['order', 'code', '%'])
        stock_list['code'] = stock_list['code'].apply(lambda code: code.strip())

        name_list = [name.text for name in name_html_list]
        name_list = pd.DataFrame(name_list, columns=['name'])

        merge = pd.concat([stock_list, name_list], axis=1)

        merge['order'] = merge['order'].astype(int)
        stock_list_20 = merge.sort_values(by=['order']).iloc[:20]

        code_20 = stock_list_20['code']
        name_20 = stock_list_20['name']
        newname = ['{} {}'.format(code_20.iloc[i], name_20.iloc[i]) for i in range(20)]
        return code_20, newname

    def get_date(self, yyyy, mm, code):
        if len(str(mm)) < 2:
            url = "https://www.twse.com.tw/en/exchangeReport/STOCK_DAY?response=csv&date={}0{}01&stockNo={}".format(yyyy, mm, code)
            response = requests.get(url, headers=self.header)
            print("get_date", yyyy, mm, code)
            return response.text

        else:
            url = 'https://www.twse.com.tw/en/exchangeReport/STOCK_DAY?response=csv&date={}{}01&stockNo={}'.format(yyyy, mm, code)
            response = requests.get(url, headers=self.header)
            print("get_date", yyyy, mm, code)
            return response.text

    def Download(self):
        stock_dic = self.stock_dic
        start = datetime.date(2020, 2, 1)
        end = datetime.datetime.now()
        if not os.path.isdir('stock'):
            os.mkdir('stock')
        for key in stock_dic:
            code = self.stock_dic[key]
            newname = '{} {}'.format(code, key)
            dirname = "./stock/{}".format(newname)
            if not os.path.isdir(dirname):
                os.mkdir(dirname)
            for y in range(end.year - start.year + 1):
                yyyy = start.year + y
                yearname = dirname + '/{}'.format(yyyy)
                if not os.path.isdir(yearname):
                    os.mkdir(yearname)
                if yyyy < end.year:
                    for mm in range(1, 13):
                        with open('{}/{}.csv'.format(yearname, mm), 'w', encoding='utf-8') as f:
                            f.write(self.get_date(yyyy, mm, code))
                            print('{}/{}.csv已下載完畢'.format(yearname, mm))
                        time.sleep(7)
                elif yyyy == end.year and start.month <= end.month:
                    for m in range(end.month - start.month + 1):
                        mm = start.month + m
                        with open('{}/{}.csv'.format(yearname, mm), 'w', encoding='utf-8') as f:
                            f.write(self.get_date(yyyy, mm, code))
                            print('{}/{}.csv已下載完畢'.format(yearname, mm))
                        time.sleep(7)
                else:
                    break

    def find_last_day(self, dirname):
        path0 = "./stock/{}".format(dirname)
        yyyy = sorted([dir for dir in os.listdir(path0) if not dir.startswith('.')])[-1]

        path1 = "./stock/{}/{}".format(dirname, yyyy)
        last_file = sorted([file for file in os.listdir(path1) if not file.startswith('.')])[-1]
        last_month = int(os.path.splitext(last_file)[0])

        df = self.Stock_data(dirname, yyyy, last_month)
        return df.date.values[-1]

    def find_last_month(self, dirname):
        path0 = "./stock/{}".format(dirname)
        last_year = sorted([dir for dir in os.listdir(path0) if not dir.startswith('.')])[-1]

        path1 = "./stock/{}/{}".format(dirname, last_year)
        last_file = sorted([file for file in os.listdir(path1) if not file.startswith('.')])[-1]
        last_month = int(os.path.splitext(last_file)[0])
        return last_year, last_month

    def date2yyyymmdd(self, date):
        yyyy = date.split('/')[0]
        mm = date.split('/')[1]
        dd = date.split('/')[2]
        yyyymmdd = yyyy + mm + dd
        return int(yyyymmdd)

    def Stock_data(self, stock, yyyy, mm):
        df = pd.read_csv('./stock/{}/{}/{}.csv'.format(stock, yyyy, mm), header=1, engine='python', thousands=',')
        rename_dic = {'Date': 'date',
                      'Trade Volume': 'volume',
                      'Opening Price': 'open',
                      'Highest Price': 'high',
                      'Lowest Price': 'low',
                      'Closing Price': 'close'
                      }
        df = df.drop(columns=['Trade Value', 'Change', 'Transaction', 'Unnamed: 9'],axis=1).rename(columns = rename_dic)
        df = df.dropna()
        df['date'] = df['date'].apply(self.date2yyyymmdd)
        df = df[df.close != '--']
        return df

    def find_first(self, stock):
        # 因為每個股的起始時間不同，故每個股分別看一次最早的時間，到有資料為第一天。
        code = self.stock_dic[stock]
        dirname = '{} {}'.format(code, stock)

        end = datetime.datetime.now()

        test = [[]]

        yyyy = 2010
        while yyyy <= 2020 and test[0] == []:
            mm = 1
            while mm <= 12 and test[0] == []:
                if yyyy < end.year:
                    with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'r') as csvfile:
                        csvreader = csv.reader(csvfile)
                        test = [row for row in csvreader]
                elif yyyy == end.year and mm <= end.month:
                    with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'r') as csvfile:
                        csvreader = csv.reader(csvfile)
                        test = [row for row in csvreader]
                first_month = mm
                mm += 1
            first_year = yyyy
            yyyy += 1
        return first_year, first_month

    def Merge_data(self):
        stock_dic = self.stock_dic
        for key in stock_dic:
            first_year, first_month = self.find_first(key)
            end = datetime.datetime.now()
            code = self.stock_dic[key]
            dirname = '{} {}'.format(code, key)
            main_df = pd.DataFrame(columns = self.columns)
            for y in range(end.year - first_year + 1):
                yyyy = first_year + y
                for mm in range(1, 13):
                    if yyyy == first_year and mm >= first_month:
                        with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'r') as csvfile:
                            csvreader = csv.reader(csvfile)
                            test = [row for row in csvreader]
                        print(test[0], './stock/{}/{}/{}.csv'.format(dirname, yyyy, mm))
                        if test[0] == []:
                            with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'w', encoding='utf-8') as f:
                                f.write(self.get_date(yyyy, mm, code))
                            time.sleep(5)

                        next_df = self.Stock_data(dirname, yyyy, mm)
                        main_df = pd.concat([main_df, next_df], ignore_index=True)
                    elif yyyy > first_year and yyyy < end.year:
                        with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'r') as csvfile:
                            csvreader = csv.reader(csvfile)
                            test = [row for row in csvreader]
                        print(test[0], './stock/{}/{}/{}.csv'.format(dirname, yyyy, mm))
                        if test[0] == []:
                            with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'w', encoding='utf-8') as f:
                                f.write(self.get_date(yyyy, mm, code))
                            time.sleep(5)

                        next_df = self.Stock_data(dirname, yyyy, mm)
                        main_df = pd.concat([main_df, next_df], ignore_index=True)
                    elif yyyy == end.year and mm <= end.month:
                        with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'r') as csvfile:
                            csvreader = csv.reader(csvfile)
                            test = [row for row in csvreader]
                        print(test[0], './stock/{}/{}/{}.csv'.format(dirname, yyyy, mm))
                        if test[0] == []:
                            with open('./stock/{}/{}/{}.csv'.format(dirname, yyyy, mm), 'w', encoding='utf-8') as f:
                                f.write(self.get_date(yyyy, mm, code))
                            time.sleep(5)

                        next_df = self.Stock_data(dirname, yyyy, mm)
                        main_df = pd.concat([main_df, next_df], ignore_index=True)
                    else:
                        break
            main_df.to_csv('./stock_clean/{}.csv'.format(dirname), index=False)

    def update_data(self):
        stock_dic = self.stock_dic
        for key in stock_dic:
            code = self.stock_dic[key]
            dirname = '{} {}'.format(code, key)

            main_df = pd.read_csv('./stock_clean/{}.csv'.format(dirname), engine='python')
            date = crawler_investor.next_day(main_df.date.values[-1])
            end_day = int(datetime.date.today().strftime("%Y%m%d"))
            while date <= end_day:
                yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
                if not os.path.isfile('./stock/{}/{}/{}.csv'.format(dirname, int(yyyy), int(mm))):
                    with open('./stock/{}/{}/{}.csv'.format(dirname, int(yyyy), int(mm)), 'w', encoding='utf-8') as f:
                        f.write(self.get_date(int(yyyy), int(mm), code))
                    time.sleep(5)

                with open('./stock/{}/{}/{}.csv'.format(dirname, int(yyyy), int(mm)), 'r') as csvfile:
                    csvreader = csv.reader(csvfile)
                    test = [row for row in csvreader]

                exist = [col for row in test for col in row]
                if test[0] == [] or '{}/{}/{}'.format(yyyy, mm, dd) not in exist:
                    with open('./stock/{}/{}/{}.csv'.format(dirname, int(yyyy), int(mm)), 'w', encoding='utf-8') as f:
                        f.write(self.get_date(int(yyyy), int(mm), code))
                    time.sleep(5)

                with open('./stock/{}/{}/{}.csv'.format(dirname, int(yyyy), int(mm)), 'r') as csvfile:
                    csvreader = csv.reader(csvfile)
                    daily_list = [row for row in csvreader]

                find = [col for row in daily_list for col in row]
                if '{}/{}/{}'.format(yyyy, mm, dd) in find:
                    next_df = self.Stock_data(dirname, int(yyyy), int(mm))
                    next_series = next_df[next_df.date == date]
                    main_df = pd.concat([main_df, next_series], ignore_index=True)
                    print(main_df, '{} {}已完成'.format(dirname, date))

                date = crawler_investor.next_day(date)
            main_df.to_csv('./stock_clean/{}.csv'.format(dirname), index=False)

class TAIEX():
    def __init__(self):

        self.header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
        self.columns = ['date', 'open', 'high', 'low', 'close']

    def get_date(self, yyyy, mm):
        if len(str(mm)) < 2:
            url = "https://www.twse.com.tw/en/indicesReport/MI_5MINS_HIST?response=csv&date={}0{}01".format(yyyy, mm)
            response = requests.get(url, headers=self.header)
            print("get_date", yyyy, mm)
            return response.text

        else:
            url = 'https://www.twse.com.tw/en/indicesReport/MI_5MINS_HIST?response=csv&date={}{}01'.format(yyyy, mm)
            response = requests.get(url, headers=self.header)
            print("get_date", yyyy, mm)
            return response.text

    def Download(self):
        start = datetime.date(2020, 2, 1)
        end = datetime.datetime.now()
        if not os.path.isdir('TAIEX'):
            os.mkdir('TAIEX')
        for y in range(end.year - start.year + 1):
            yyyy = start.year + y
            yearname = './TAIEX/{}'.format(yyyy)
            if not os.path.isdir(yearname):
                os.mkdir(yearname)
            if yyyy < end.year:
                for mm in range(1, 13):
                    with open('{}/{}.csv'.format(yearname, mm), 'w', encoding='utf-8') as f:
                        f.write(self.get_date(yyyy, mm))
                        print('{}/{}.csv已下載完畢'.format(yearname, mm))
                    time.sleep(7)
            elif yyyy == end.year and start.month <= end.month:
                for m in range(end.month - start.month + 1):
                    mm = start.month + m
                    with open('{}/{}.csv'.format(yearname, mm), 'w', encoding='utf-8') as f:
                        f.write(self.get_date(yyyy, mm))
                        print('{}/{}.csv已下載完畢'.format(yearname, mm))
                    time.sleep(7)
            else:
                break

    def find_last_day(self):
        yyyy = sorted([dir for dir in os.listdir('./TAIEX') if not dir.startswith('.')])[-1]
        path = "./TAIEX/{}".format(yyyy)

        last_file = sorted([file for file in os.listdir(path) if not file.startswith('.')])[-1]
        last_month = int(os.path.splitext(last_file)[0])

        df = self.Stock_data(yyyy, last_month)
        return df.date.values[-1]

    def date2yyyymmdd(self, date):
        yyyy = date.split('/')[0]
        mm = date.split('/')[1]
        dd = date.split('/')[2]
        yyyymmdd = yyyy + mm + dd
        return int(yyyymmdd)

    def Stock_data(self, yyyy, mm):
        df = pd.read_csv('./TAIEX/{}/{}.csv'.format(yyyy, mm), header=1, engine='python', thousands=',')
        rename_dic = {'Date': 'date',
                      'Opening Index': 'open',
                      'Highest Index': 'high',
                      'Lowest Index': 'low',
                      'Closing Index': 'close'
                      }
        df = df.drop(columns=['Unnamed: 5'],axis=1).rename(columns = rename_dic)
        df['date'] = df['date'].apply(self.date2yyyymmdd)
        return df

    def Merge_data(self):
        end = datetime.datetime.now()
        main_df = pd.DataFrame(columns = self.columns)
        for y in range(end.year - 2010 + 1):
            yyyy = 2010 + y
            for mm in range(1, 13):
                if yyyy >= 2010 and yyyy < end.year:
                    with open('./TAIEX/{}/{}.csv'.format(yyyy, mm), 'r') as csvfile:
                        csvreader = csv.reader(csvfile)
                        test = [row for row in csvreader]
                    print(test[0], './TAIEX/{}/{}.csv'.format(yyyy, mm), '已完成')
                    if test[0] == []:
                        with open('./TAIEX/{}/{}.csv'.format(yyyy, mm), 'w', encoding='utf-8') as f:
                            f.write(self.get_date(yyyy, mm))
                        time.sleep(5)

                    next_df = self.Stock_data(yyyy, mm)
                    main_df = pd.concat([main_df, next_df], ignore_index=True)
                elif yyyy == end.year and mm <= end.month:
                    with open('./TAIEX/{}/{}.csv'.format(yyyy, mm), 'r') as csvfile:
                        csvreader = csv.reader(csvfile)
                        test = [row for row in csvreader]
                    print(test[0], './TAIEX/{}/{}.csv'.format(yyyy, mm), '已完成')
                    if test[0] == []:
                        with open('./TAIEX/{}/{}.csv'.format(yyyy, mm), 'w', encoding='utf-8') as f:
                            f.write(self.get_date(yyyy, mm))
                        time.sleep(5)

                    next_df = self.Stock_data(yyyy, mm)
                    main_df = pd.concat([main_df, next_df], ignore_index=True)
                else:
                    break
        main_df.to_csv('./futures/TAIEX.csv', index=False)

    def update_data(self):
        main_df = pd.read_csv('./futures/TAIEX.csv', engine='python')
        date = crawler_investor.next_day(main_df.date.values[-1])
        end_day = int(datetime.date.today().strftime("%Y%m%d"))
        while date <= end_day:
            yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
            if not os.path.isfile('./TAIEX/{}/{}.csv'.format(int(yyyy), int(mm))):
                with open('./TAIEX/{}/{}.csv'.format(int(yyyy), int(mm)), 'w', encoding='utf-8') as f:
                    f.write(self.get_date(int(yyyy), int(mm)))
                time.sleep(5)

            with open('./TAIEX/{}/{}.csv'.format(int(yyyy), int(mm)), 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                test = [row for row in csvreader]

            exist = [col for row in test for col in row]
            if test[0] == [] or '{}/{}/{}'.format(yyyy, mm, dd) not in exist:
                with open('./TAIEX/{}/{}.csv'.format(int(yyyy), int(mm)), 'w', encoding='utf-8') as f:
                    f.write(self.get_date(int(yyyy), int(mm)))
                time.sleep(5)

            with open('./TAIEX/{}/{}.csv'.format(int(yyyy), int(mm)), 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                daily_list = [row for row in csvreader]

            find = [col for row in daily_list for col in row]
            if '{}/{}/{}'.format(yyyy, mm, dd) in find:
                next_df = self.Stock_data(int(yyyy), int(mm))
                next_series = next_df[next_df.date == date]
                main_df = pd.concat([main_df, next_series], ignore_index=True)
                print(main_df, '{}已完成'.format(date))

            date = crawler_investor.next_day(date)
        main_df.to_csv('./futures/TAIEX.csv', index=False)

if __name__ == '__main__':
    d = Daily_trade_data()
    d.update_data()
    # d.Merge_data()
    TAI = TAIEX()
    TAI.update_data()
    # TAI.Merge_data()
