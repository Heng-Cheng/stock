import datetime
import os
import csv
import pandas as pd
import crawler_investor

choose_df = ['證券代號',
             '證券名稱',
             '買進股數',
             '賣出股數',
             '買賣超股數',
             '買進股數.1',
             '賣出股數.1',
             '買賣超股數.1',
             '買進股數.2',
             '賣出股數.2',
             '買賣超股數.2'
             ]

def Trust(date):
    yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
    df = pd.read_csv('./Trust/{}/{}/{}.csv'.format(yyyy, mm, date), header=1, engine='python', dtype={'證券代號': object})
    rename_dic = {'買進股數': '投信買進股數(總)',
                  '賣出股數': '投信賣出股數(總)',
                  '買賣超股數': '投信買賣超股數(總)'
                  }
    trust = df[choose_df[:5]].rename(columns = rename_dic)
    trust = trust.mask(trust.astype(object).eq('None')).dropna()
    trust['證券代號'] = trust['證券代號'].astype(object)
    return trust

def Dealers(date):
    yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)

    if date < 20141201:
        df = pd.read_csv('./Dealers/{}/{}/{}.csv'.format(yyyy, mm, date), header=1, engine='python', dtype={'證券代號': object})
        rename_dic = {'買進股數': '自營商買進股數(總)',
                      '賣出股數': '自營商賣出股數(總)',
                      '買賣超股數': '自營商買賣超股數(總)'
                      }
        dealers = df[choose_df[:5]].rename(columns = rename_dic)
        dealers = dealers.mask(dealers.astype(object).eq('None')).dropna()

        dealers＿miss_column = ['自營商買進股數(自有)',
                               '自營商賣出股數(自有)',
                               '自營商買賣超股數(自有)',
                               '自營商買進股數(避險)',
                               '自營商賣出股數(避險)',
                               '自營商買賣超股數(避險)']

        dealers＿miss = pd.DataFrame(columns=dealers＿miss_column)
        dealers = pd.concat([dealers, dealers＿miss], sort=False)
        dealers = dealers[choose_df[:2] + dealers＿miss_column + [rename_dic[key] for key in rename_dic]]
        dealers['證券代號'] = dealers['證券代號'].astype(object)
    else:
        df = pd.read_csv('./Dealers/{}/{}/{}.csv'.format(yyyy, mm, date), header=2, engine='python')
        rename_dic = {'買進股數':'自營商買進股數(自有)',
                      '賣出股數':'自營商賣出股數(自有)',
                      '買賣超股數':'自營商買賣超股數(自有)',
                      '買進股數.1':'自營商買進股數(避險)',
                      '賣出股數.1':'自營商賣出股數(避險)',
                      '買賣超股數.1':'自營商買賣超股數(避險)',
                      '買進股數.2':'自營商買進股數(總)',
                      '賣出股數.2':'自營商賣出股數(總)',
                      '買賣超股數.2':'自營商買賣超股數(總)'
                      }
        dealers = df[choose_df].rename(columns=rename_dic)
        dealers = dealers.mask(dealers.astype(object).eq('None')).dropna()
    return dealers

def Foriegn(date):
    yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)

    if date < 20171218:
        df = pd.read_csv('./Foriegn/{}/{}/{}.csv'.format(yyyy, mm, date), header=1, engine='python', dtype={'證券代號': object})
        rename_dic = {'買進股數': '外資買進股數(總)',
                      '賣出股數': '外資賣出股數(總)',
                      '買賣超股數': '外資買賣超股數(總)'
                      }
        foriegn = df[choose_df[:5]].rename(columns = rename_dic)
        foriegn = foriegn.mask(foriegn.astype(object).eq('None')).dropna()

        foriegn＿miss_column = ['外資買進股數(自有)',
                               '外資賣出股數(自有)',
                               '外資買賣超股數(自有)',
                               '外資買進股數(避險)',
                               '外資賣出股數(避險)',
                               '外資買賣超股數(避險)']

        foriegn＿miss = pd.DataFrame(columns=foriegn＿miss_column)
        foriegn = pd.concat([foriegn, foriegn＿miss], sort=False)
        foriegn = foriegn[choose_df[:2] + foriegn＿miss_column + [rename_dic[key] for key in rename_dic]]
        foriegn['證券代號'] = foriegn['證券代號'].astype(object)
    else:
        df = pd.read_csv('./Foriegn/{}/{}/{}.csv'.format(yyyy, mm, date), header=2, engine='python')
        rename_dic = {'買進股數':'外資買進股數(非自營)',
                      '賣出股數':'外資賣出股數(非自營)',
                      '買賣超股數':'外資買賣超股數(非自營)',
                      '買進股數.1':'外資買進股數(自營)',
                      '賣出股數.1':'外資賣出股數(自營)',
                      '買賣超股數.1':'外資買賣超股數(自營)',
                      '買進股數.2':'外資買進股數(總)',
                      '賣出股數.2':'外資賣出股數(總)',
                      '買賣超股數.2':'外資買賣超股數(總)'
                      }
        foriegn = df[choose_df].rename(columns=rename_dic)
        foriegn = foriegn.mask(foriegn.astype(object).eq('None')).dropna()
    return foriegn

def Merge_data(date):
    yyyy, mm, dd = crawler_investor.split_yyyymmdd(date)
    merge_table = pd.merge(Trust(date), Dealers(date), on=choose_df[:2], how='outer')
    total_table = pd.merge(merge_table, Foriegn(date), on=choose_df[:2], how='outer')
    total_table.insert(2, column='date', value=date)
    total_table.insert(3, column='year', value=yyyy)
    total_table.insert(4, column='month', value=mm)
    total_table.insert(5, column='day', value=dd)
    return total_table

def write_in_csv(dataframe):
    for i in range(len(dataframe)):
        if dataframe.iloc[i]['證券代號'][0] == '=':
            code = dataframe.iloc[i]['證券代號'].replace('=', '').replace('"', '').strip()
        else:
            code = dataframe.iloc[i]['證券代號'].strip()
        name = dataframe.iloc[i]['證券名稱'].strip()
        filename = './securities/{} {}.csv'.format(code, name)
        if not os.path.exists(filename):
            with open(filename, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(dataframe.columns.values.tolist())
                writer.writerow(dataframe.iloc[i])
        else:
            with open(filename, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(dataframe.iloc[i])

def Create_merge_table():
    date = 20200107
    end_day = int(datetime.date.today().strftime("%Y%m%d"))
    while date <= end_day:
        yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
        file1 = './Trust/{}/{}/{}.csv'.format(yyyy, mm, date)
        file2 = './Dealers/{}/{}/{}.csv'.format(yyyy, mm, date)
        file3 = './Foriegn/{}/{}/{}.csv'.format(yyyy, mm, date)
        if os.path.exists(file1) and os.path.exists(file2) and os.path.exists(file3):
            total_table = Merge_data(date)
            write_in_csv(total_table)
            print('{}已完成'.format(date))
        else:
            print('無{}資料'.format(date))
        date = crawler_investor.next_day(date)

def update_investor(date):
    yyyy, mm, dd = crawler_investor.str_yyyy_mm_dd(date)
    file1 = './Trust/{}/{}/{}.csv'.format(yyyy, mm, date)
    file2 = './Dealers/{}/{}/{}.csv'.format(yyyy, mm, date)
    file3 = './Foriegn/{}/{}/{}.csv'.format(yyyy, mm, date)
    if os.path.exists(file1) and os.path.exists(file2) and os.path.exists(file3):
        total_table = Merge_data(date)
        write_in_csv(total_table)
        print('{}已完成'.format(date))
    else:
        print('無{}資料'.format(date))

if __name__ == '__main__':
    Create_merge_table()