import datetime
import pandas as pd
from talib import abstract
import os

def sma(series, d):
    return series.rolling(d, min_periods = d).mean()

def lag_n(dataframe, variable, d):
    dataframe['{}_{}'.format(variable, d)] = dataframe['{}'.format(variable)].shift(periods = d)

def moving_average(dataframe, days = [5, 10, 20]):
    for d in days:
        dataframe['EMA{}'.format(d)] = abstract.EMA(dataframe, timeperiod = d)

def bias(dataframe, days = [3, 6, 10, 25]):
    series = dataframe['close']
    for d in days:
        dataframe['BIAS{}'.format(d)] = (series - sma(series, d)) * 100 / sma(series, d)

def counter_daily_potential(dataframe):
    dataframe['CDP'] = (dataframe['high'] + dataframe['low'] + dataframe['close'] * 2)/4
    PT = dataframe['high'] - dataframe['low']
    dataframe['AH'] = dataframe['CDP'] + PT
    dataframe['NH'] = dataframe['CDP'] * 2 - dataframe['low']
    dataframe['NL'] = dataframe['CDP'] * 2 - dataframe['high']
    dataframe['AL'] = dataframe['CDP'] - PT

def psy_line(dataframe, d = 12):
    days = dataframe['close'] > dataframe['close'].shift(periods = 1)
    dataframe['PSY'] = days.rolling(d, min_periods = d).mean()

def volumn_ratio(dataframe, d = 26):
    u_days = dataframe['close'] > dataframe['close'].shift(periods=1)
    p_days = dataframe['close'] == dataframe['close'].shift(periods=1)
    d_days = dataframe['close'] < dataframe['close'].shift(periods=1)
    u_volumns = dataframe['volume'].multiply(u_days.astype(int)).rolling(d, min_periods = d).sum()
    p_volumns = dataframe['volume'].multiply(p_days.astype(int)).rolling(d, min_periods = d).sum()
    d_volumns = dataframe['volume'].multiply(d_days.astype(int)).rolling(d, min_periods = d).sum()
    dataframe['VR'] = (u_volumns + p_volumns * 0.5)/(d_volumns + p_volumns * 0.5)

def on_balance_volume(dataframe):
    rsv_1 = (dataframe['close'] - dataframe['low'])/(dataframe['high'] - dataframe['low'])
    dataframe['OBV'] = dataframe['volume'].multiply(rsv_1)

def TA_processing(dataframe):
    bias(dataframe, days=[3, 6, 10, 25])
    moving_average(dataframe, days=[5, 10, 20])
    dataframe['ROC'] = abstract.ROC(dataframe, timeperiod=10)
    dataframe['MACD'] = abstract.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)['macd']
    dataframe['MACD_signal'] = abstract.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)['macdsignal']
    dataframe['UBBANDS'] = abstract.BBANDS(dataframe, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)['upperband']
    dataframe['MBBANDS'] = abstract.BBANDS(dataframe, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)['middleband']
    dataframe['LBBANDS'] = abstract.BBANDS(dataframe, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)['lowerband']
    dataframe['%K'] = abstract.STOCH(dataframe, fastk_period=9)['slowk']/100
    dataframe['%D'] = abstract.STOCH(dataframe, fastk_period=9)['slowd']/100
    dataframe['W%R'] = abstract.WILLR(dataframe, timeperiod=14)/100
    dataframe['RSI9'] = abstract.RSI(dataframe, timeperiod = 9)/100
    dataframe['RSI14'] = abstract.RSI(dataframe, timeperiod = 14)/100
    dataframe['CCI'] = abstract.CCI(dataframe, timeperiod=14)/100
    counter_daily_potential(dataframe)
    dataframe['MOM'] = abstract.MOM(dataframe, timeperiod=10)
    dataframe['DX'] = abstract.DX(dataframe, timeperiod=14)/100
    psy_line(dataframe)
    volumn_ratio(dataframe, d=26)
    on_balance_volume(dataframe)

def value_rank(dataframe, var, d):
    date = dataframe.date.iloc[d-1:].reset_index(drop=True)
    rank_list = [dataframe[var].iloc[i:d + i].rank(method='min', ascending = False).iloc[-1] for i in range(len(dataframe)-(d-1))]
    rank = pd.Series(rank_list, name='rank')
    concat_rank = pd.concat([date, rank], axis=1)
    return pd.merge(dataframe, concat_rank, on='date', how='left')['rank']

def increase_counts(dataframe, var, d):
    date = dataframe.date.iloc[d-1:].reset_index(drop=True)
    increase_days = (dataframe[var] > dataframe[var].shift(1)).astype(int)
    increase_list = [increase_days.iloc[i:d + i].sum() for i in range(len(dataframe)-(d-1))]
    increase = pd.Series(increase_list, name='increase')
    rank = increase.rank(method='dense', ascending = False)
    concat_increase = pd.concat([date, rank], axis=1)
    return pd.merge(dataframe, concat_increase, on='date', how='left')['increase']

def p_vol_compare(dataframe):
    if dataframe.close_u is True and dataframe.volume_u is True:
        return '1'
    elif dataframe.close_u is True and dataframe.volume_ed is True:
        return "2"
    elif dataframe.close_e is True:
        return "3"
    elif dataframe.close_d is True and dataframe.volume_u is True:
        return "4"
    elif dataframe.close_d is True and dataframe.volume_ed is True:
        return "5"

def p_bal_compare(dataframe):
    if dataframe.close_u is True and dataframe.cash_balance_u is True and dataframe.stock_balance_u is True:
        return '1'
    elif dataframe.close_u is True and dataframe.cash_balance_u is True and dataframe.stock_balance_d is True:
        return "2"
    elif dataframe.close_u is True and dataframe.cash_balance_d is True and dataframe.stock_balance_u is True:
        return "3"
    elif dataframe.close_u is True and dataframe.cash_balance_d is True and dataframe.stock_balance_d is True:
        return "4"
    elif dataframe.close_e is True:
        return "5"
    elif dataframe.close_d is True and dataframe.cash_balance_u is True and dataframe.stock_balance_u is True:
        return "6"
    elif dataframe.close_d is True and dataframe.cash_balance_u is True and dataframe.stock_balance_d is True:
        return "7"
    elif dataframe.close_d is True and dataframe.cash_balance_d is True and dataframe.stock_balance_u is True:
        return "8"
    elif dataframe.close_d is True and dataframe.cash_balance_d is True and dataframe.stock_balance_d is True:
        return "9"

def trans2yyyymmdd(date):
    yyyy = date[:4]
    mm = date[5:7]
    dd = date[-2:]
    yyyymmdd = yyyy + mm + dd
    return int(yyyymmdd)

def split2yyyymmdd(stryyyymmdd):
    yyyy = int(stryyyymmdd.replace('日', '').split('年')[0])
    mm = int(stryyyymmdd.replace('日', '').split('年')[1].split('月')[0])
    dd = int(stryyyymmdd.replace('日', '').split('年')[1].split('月')[1])
    date = datetime.date(yyyy, mm, dd)
    yyyymmdd = int(date.strftime("%Y%m%d"))
    return yyyymmdd

def transpercent2num(change):
    return float(str(change).split('%')[0])

def transchange2class(change):
    high = 1.8
    mild = 1
    if change < -high:
        return 0
    if change >= -high and change < -mild:
        return 1
    if change >= -mild and change < 0:
        return 2
    if change == 0:
        return 3
    if change > 0 and change < mild:
        return 4
    if change >= mild and change < high:
        return 5
    if change > high:
        return 6

def transclose2class3(change):
    if change >= -10 and change < -0.5:
        return 0
    elif change >= 0.5 and change < 10:
        return 2
    else:
        return 1

def futures():
    NI225 = pd.read_csv('./futures/日經225指數.csv', thousands = ",")
    NI225 = NI225.rename(columns={"日期": "date", "收市": "NI225", "更改%": "NI225_change"})

    SP500 = pd.read_csv('./futures/標準普爾500.csv', thousands = ",")
    SP500 = SP500.rename(columns={"日期": "date", "收市": "SP500", "更改%": "SP500_change"})

    Pl = pd.read_csv('./futures/白金.csv', thousands = ",")
    Pl = Pl.rename(columns={"日期": "date", "白金": "Pl", "漲跌幅": "Pl_change"})

    Gc = pd.read_csv('./futures/黃金.csv', thousands = ",")
    Gc = Gc.rename(columns={"日期": "date", "黃金": "Gc", "漲跌幅": "Gc_change"})

    Si = pd.read_csv('./futures/銀.csv', thousands = ",")
    Si = Si.rename(columns={"日期": "date", "收市": "Si", "更改%": "Si_change"})

    Hg = pd.read_csv('./futures/銅.csv', thousands = ",")
    Hg = Hg.rename(columns={"日期": "date", "收市": "Hg", "更改%": "Hg_change"})

    Ng = pd.read_csv('./futures/天然氣.csv', thousands = ",")
    Ng = Ng.rename(columns={"日期": "date", "收市": "Ng", "更改%": "Ng_change"})

    futures_dic = {'NI225':NI225, 'SP500':SP500, 'Pl':Pl, 'Gc':Gc, 'Si':Si, 'Hg':Hg, 'Ng':Ng}
    for f in futures_dic:
        futures_dic[f]["date"] = futures_dic[f]["date"].apply(split2yyyymmdd)

    all_days = pd.read_csv('./stock_clean/2330 台積電.csv', usecols = ['date'])
    merge_NI225 = pd.merge(all_days, NI225, on='date', how='left')
    merge_SP500 = pd.merge(merge_NI225, SP500, on='date', how='left')
    merge_Pl = pd.merge(merge_SP500, Pl, on='date', how='left')
    merge_Gc = pd.merge(merge_Pl, Gc, on='date', how='left')
    merge_Si = pd.merge(merge_Gc, Si, on='date', how='left')
    merge_Hg = pd.merge(merge_Si, Hg, on='date', how='left')
    merge_Ng = pd.merge(merge_Hg, Ng, on='date', how='left')
    futures = merge_Ng.sort_values(by=['date']).reset_index(drop=True)

    values_list = [key for key in futures_dic]
    change_list = ['{}_change'.format(value) for value in values_list]
    change_list = list(zip(values_list, change_list))
    for value in values_list:
        futures[value].fillna(method='ffill', inplace=True)
    for value, change in change_list:
        futures[change] = futures[change].apply(transpercent2num)
        temp_change = (futures[value] - futures[value].shift(1)) * 100 / futures[value].shift(1)
        futures[change].fillna(temp_change.round(2), inplace=True)
    futures = futures[futures.date <= 20191231]
    return futures

def Shanghai_shares():
    SSEC = pd.read_csv('./shi/上證指數.csv', thousands = ",")
    SSEC = SSEC.rename(columns={"日期": "date", "收盘": "SSEC_close", "开盘": "SSEC_open", "高": "SSEC_high", "低": "SSEC_low",
                                "涨跌幅": "SSEC_change"}).drop(columns='交易量')

    SPCITIC50 = pd.read_csv('./shi/中信標普50.csv', thousands=",")
    SPCITIC50 = SPCITIC50.rename(columns={"日期": "date", "收盘": "SPCITIC50_close", "开盘": "SPCITIC50_open", "高": "SPCITIC50_high", "低": "SPCITIC50_low",
                                          "涨跌幅": "SPCITIC50_change"}).drop(columns='交易量')

    Shanghai_shares_dic = {'SSEC':SSEC, 'SPCITIC50':SPCITIC50}
    for s in Shanghai_shares_dic:
        Shanghai_shares_dic[s]["date"] = Shanghai_shares_dic[s]["date"].apply(split2yyyymmdd)

    all_days = pd.read_csv('./stock_clean/2330 台積電.csv', usecols=['date'])
    merge_SSEC = pd.merge(all_days, SSEC, on='date', how='left')
    merge_SPCITIC50 = pd.merge(merge_SSEC, SPCITIC50, on='date', how='left')
    shanghai_shares = merge_SPCITIC50.sort_values(by=['date']).reset_index(drop=True)

    values_list = [column for column in shanghai_shares.columns if 'date' not in column and 'change' not in column]

    close_list =  [column for column in shanghai_shares.columns if 'close' in column]
    change_list = [column for column in shanghai_shares.columns if 'change' in column]
    change_list = list(zip(close_list, change_list))
    for value in values_list:
        shanghai_shares[value].fillna(method='ffill', inplace=True)
    for close, change in change_list:
        shanghai_shares[change] = shanghai_shares[change].apply(transpercent2num)
        temp_change = (shanghai_shares[close] - shanghai_shares[close].shift(1)) * 100 / shanghai_shares[close].shift(1)
        shanghai_shares[change].fillna(temp_change.round(2), inplace=True)
    shanghai_shares = shanghai_shares[shanghai_shares.date <= 20191231]
    return shanghai_shares

def Investors(shares):
    stock = pd.read_csv('./stock_clean/{}'.format(shares))
    for price in ['high', 'low', 'close']:
        stock['{}_change'.format(price)] = (stock[price].shift(-1) - stock[price]) * 100 / stock[price]
        stock['{}_change_class'.format(price)] = stock['{}_change'.format(price)].apply(transchange2class).astype(object)
    stock['close_change_class3'] = stock.close_change.apply(transclose2class3).astype(object)
    TA_processing(stock)
    stock = stock.mask(stock.astype(object).eq('None')).dropna().reset_index(drop=True)

    df = pd.read_csv('./securities/{}'.format(shares), thousands=",")[
        ['date', '投信買進股數(總)', '投信賣出股數(總)', '自營商買進股數(總)', '自營商賣出股數(總)', '外資買進股數(總)', '外資賣出股數(總)']]
    columns_dic = {"投信買進股數(總)": "trust_buy", "投信賣出股數(總)": "trust_sell",
                   "自營商買進股數(總)": "dealer_buy", "自營商賣出股數(總)": "dealer_sell",
                   "外資買進股數(總)": "foriegn_buy", "外資賣出股數(總)": "foriegn_sell"}
    df = df.rename(columns=columns_dic)

    merge_table = pd.merge(stock, df, on='date', how='left')
    for key in columns_dic:
        merge_table[columns_dic[key]].fillna(0, inplace=True)
    print(merge_table.close, merge_table.close_change)
    return merge_table

def stock_margin(shares):
    short_sales = pd.read_csv('./securities_shortsale/{}'.format(shares), thousands=",")
    short_sales = short_sales.drop(columns=['證券名稱'], axis=1)

    balance = pd.read_csv('./securities_margin/{}'.format(shares), thousands=",")
    balance = balance.drop(columns=['股票代號', '股票名稱', 'margin_quota', 'short_sale','short_quota', '註記'], axis=1)

    all_days = pd.read_csv('./stock_clean/{}'.format(shares), usecols=['date'])
    merge_short_sales = pd.merge(all_days, short_sales, on='date', how='left')
    merge_balance = pd.merge(merge_short_sales, balance, on='date', how='left')
    merge_balance = merge_balance[merge_balance.date <= 20191231]
    return merge_balance

def merge_date(shares):
    merge_table = pd.merge(Investors(shares), stock_margin(shares), on='date', how='left')
    merge_table = pd.merge(merge_table, Shanghai_shares(), on='date', how='left')
    merge_table = pd.merge(merge_table, futures(), on='date', how='left')
    merge_table = merge_table[merge_table.date <= 20191213]
    return merge_table

def output_data():
    stock_name = []
    close_means = []
    close_stds = []
    high_means = []
    high_stds = []
    low_means = []
    low_stds = []
    close_change_means = []
    close_change_stds = []
    high_change_means = []
    high_change_stds = []
    low_change_means = []
    low_change_stds = []

    for shares in sorted(os.listdir('./stock_clean/')):
        if not shares.startswith('.'):
            df = merge_date(shares).round(4)
            dirname = shares.split('.')[0]

            stock_name.append(dirname)
            close_means.append(df.close.mean())
            close_stds.append(df.close.std())
            high_means.append(df.high.mean())
            high_stds.append(df.high.std())
            low_means.append(df.low.mean())
            low_stds.append(df.low.std())
            close_change_means.append(df.close_change.mean())
            close_change_stds.append(df.close_change.std())
            high_change_means.append(df.high_change.mean())
            high_change_stds.append(df.high_change.std())
            low_change_means.append(df.low_change.mean())
            low_change_stds.append(df.low_change.std())

            for column in df.columns:
                if column not in ['date', 'high_change_class', 'low_change_class', 'close_change_class', 'close_change_class3']:
                    if df[column].std() != 0:
                        df[column] = (df[column] - df[column].mean()) / df[column].std()
                    else:
                        print(shares, column, df[column].std())
            # timesteps(df, days=10)

            total_data = df.mask(df.astype(object).eq('None')).dropna().reset_index(drop=True)
            train_change_class = total_data[['date', 'high_change_class', 'low_change_class', 'close_change_class','close_change_class3']]

            change_count = pd.DataFrame({'rank':list(range(7))})
            for column in ['high_change_class', 'low_change_class', 'close_change_class', 'close_change_class3']:
                change_class = pd.DataFrame({'rank':list(train_change_class[column].value_counts().sort_index().index),
                                             '{}'.format(column):list(train_change_class[column].value_counts().sort_index())})
                change_count = pd.merge(change_count, change_class, on='rank', how='left')
            change_count.fillna(0, inplace=True)

            os.mkdir("./auto/{}".format(dirname))
            total_data.round(4).iloc[:-1].to_csv('./auto/{}/x_train.csv'.format(dirname), index=False)
            # y_train_price.round(4).to_csv('./auto/{}/y_train_price.csv'.format(dirname), index=False)
            # y_train_change.round(4).to_csv('./auto/{}/y_train_change.csv'.format(dirname), index=False)
            # y_train_change_class.round(4).to_csv('./auto/{}/y_train_change_class.csv'.format(dirname), index=False)
            change_count.astype(int).to_csv('./auto/{}/change_class.csv'.format(dirname), index=False)

    Z_data = {'stock_name': stock_name,
              'close_means': close_means,
              'close_stds': close_stds,
              'high_means': high_means,
              'high_stds': high_stds,
              'low_means': low_means,
              'low_stds': low_stds,
              'close_change_means': close_change_means,
              'close_change_stds': close_change_stds,
              'high_change_means': high_change_means,
              'high_change_stds': high_change_stds,
              'low_change_means': low_change_means,
              'low_change_stds': low_change_stds
              }
    pd.DataFrame(Z_data).to_csv('./auto/Z_data.csv', index=False)


def reshape_data(dataframe):
    df = dataframe.drop(columns=['date'], axis = 1)
    for var in list(df.columns):
        lag_n(df, var, n = 9)
    df = df.sort_index(axis=1)
    df = df.mask(df.astype(object).eq('None')).dropna().reset_index(drop=True)
    return df.values.reshape(df.shape[0], 10, -1)

if __name__ == '__main__':
    Investors('2330 台積電.csv')
    # output_data()
    # print(transchange2class(-1.734104), transclose2class3(-1.734104))