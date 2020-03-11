import datetime
import pandas as pd
import crawler_investor
import os
import csv

def margin_delete():
    date = 20090101
    today = int(datetime.date.today().strftime("%Y%m%d"))

    while date <= today:
        yyyy, mm, dd = crawler_investor.split_yyyymmdd(date)
        filename = "./Margin_financing/{}/{} month/{}.csv".format(yyyy, mm, date)
        with open(filename, 'r', encoding="utf-8") as f:
            checklist = f.read()
            if "融資(交易單位)" not in checklist:
                os.remove(filename)
                if not os.path.exists(filename):
                    print('{}已被刪除'.format(filename))
        date = crawler_investor.next_day(date)

def Margin(date):
    yyyy, mm, dd = crawler_investor.split_yyyymmdd(date)
    df = pd.read_csv("./Margin_financing/{}/{} month/{}.csv".format(yyyy, mm, date), header=7, engine='python', dtype={'證券代號': object})
    df = df.drop(columns = ['前日餘額', '前日餘額.1','Unnamed: 16'])
    rename_dic = {'買進': 'margin_purchase',
                  '賣出': 'margin_sale',
                  '現金償還': 'cash_redemption',
                  '今日餘額': 'cash_balance',
                  '限額': 'margin_quota',
                  '買進.1': 'short_covering',
                  '賣出.1': 'short_sale',
                  '現券償還': 'stock_redemption',
                  '今日餘額.1': 'stock_balance',
                  '限額.1': 'short_quota',
                  '資券互抵': 'offsetting'
                  }
    margin = df.rename(columns = rename_dic)
    margin.insert(0, column='date', value=date)
    margin = margin.mask(margin.astype(object).eq('None')).dropna()
    return margin

def margin_write_in_csv(dataframe):
    for i in range(len(dataframe)):
        if dataframe.iloc[i]['股票代號'][0] == '=':
            code = dataframe.iloc[i]['股票代號'].replace('=', '').replace('"', '').strip()
        else:
            code = dataframe.iloc[i]['股票代號'].strip()
        name = dataframe.iloc[i]['股票名稱'].strip()
        filename = './securities_margin/{} {}.csv'.format(code, name)
        if not os.path.exists(filename):
            with open(filename, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(dataframe.columns.values.tolist())
                writer.writerow(dataframe.iloc[i])
        else:
            with open(filename, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(dataframe.iloc[i])

def Create_margin_table():
    date = 20090105
    end_day = int(datetime.date.today().strftime("%Y%m%d"))
    os.mkdir('./securities_margin')
    while date <= end_day:
        yyyy, mm, dd = crawler_investor.split_yyyymmdd(date)
        file = "./Margin_financing/{}/{} month/{}.csv".format(yyyy, mm, date)
        if os.path.exists(file):
            margin_write_in_csv(Margin(date))
            print('{}已完成'.format(date))
        else:
            print('無{}資料'.format(date))
        date = crawler_investor.next_day(date)

def update_margin(date):
    yyyy, mm, dd = crawler_investor.split_yyyymmdd(date)
    file = "./Margin_financing/{}/{} month/{}.csv".format(yyyy, mm, date)
    if os.path.exists(file):
        margin_write_in_csv(Margin(date))
        print('{}已完成'.format(date))
    else:
        print('無{}資料'.format(date))

def Short_sales(date):
    yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
    df = pd.read_csv("./Short Sales/{}/{}/{}.csv".format(yyyy, mm, date), header=1, engine='python')
    df = df.drop(columns = ['Unnamed: 5'])
    rename_dic = {"融券賣出成交數量": "short_sale",
                  "融券賣出成交金額": "short_sale_value",
                  "借券賣出成交數量": "SBL_short_sale",
                  "借券賣出成交金額": "SBL_short_sale_value"
                  }
    short_sales = df.rename(columns = rename_dic)
    short_sales.insert(0, column='date', value=date)
    short_sales = short_sales.mask(short_sales.astype(object).eq('None')).dropna()
    short_sales = short_sales[short_sales['證券名稱'] != "合計"]
    return short_sales

def short_sale_write_in_csv(dataframe):
    for i in range(len(dataframe)):
        if dataframe.iloc[i]['證券名稱'][0] == '=':
            name = dataframe.iloc[i]['證券名稱'].replace('=', '').replace('"', '').strip()
        else:
            name = dataframe.iloc[i]['證券名稱'].strip()
        code, stock = name.split(' ')[0], name.split(' ')[-1]
        filename = './securities_shortsale/{} {}.csv'.format(code, stock)
        if not os.path.exists(filename):
            with open(filename, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(dataframe.columns.values.tolist())
                writer.writerow(dataframe.iloc[i])
        else:
            with open(filename, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(dataframe.iloc[i])

def Create_Short_Sale_table():
    date = 20090105
    end_day = int(datetime.date.today().strftime("%Y%m%d"))
    os.mkdir('./securities_shortsale')
    while date <= end_day:
        yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
        file = "./Short Sales/{}/{}/{}.csv".format(yyyy, mm, date)
        if os.path.exists(file):
            short_sale_write_in_csv(Short_sales(date))
            print('{}已完成'.format(date))
        else:
            print('無{}資料'.format(date))
        date = crawler_investor.next_day(date)

def update_short_sale(date):
    yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
    file = "./Short Sales/{}/{}/{}.csv".format(yyyy, mm, date)
    if os.path.exists(file):
        short_sale_write_in_csv(Short_sales(date))
        print('{}已完成'.format(date))
    else:
        print('無{}資料'.format(date))

if __name__ == '__main__':
    Create_Short_Sale_table()