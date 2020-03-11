import datetime
import pandas as pd
from talib import abstract
import os
import crawler_investor
import crawler_margin
import crawler_margin_financing
import crawler_futures
import stock50

# 定義移動平均
def sma(series, d):
    return series.rolling(d, min_periods = d).mean()

# 定義lag變數
def lag_n(dataframe, variable, d):
    dataframe['{}_{}'.format(variable, d)] = dataframe['{}'.format(variable)].shift(periods = d)

# 定義指數移動平均
def moving_average(dataframe, days = [5, 10, 20]):
    for d in days:
        dataframe['EMA{}'.format(d)] = abstract.EMA(dataframe, timeperiod = d)

# 定義乖離率
def bias(dataframe, days = [3, 6, 10, 25]):
    series = dataframe['close']
    for d in days:
        dataframe['BIAS{}'.format(d)] = (series - sma(series, d)) * 100 / sma(series, d)

# 定義逆勢操作指標
def counter_daily_potential(dataframe):
    dataframe['CDP'] = (dataframe['high'] + dataframe['low'] + dataframe['close'] * 2)/4
    PT = dataframe['high'] - dataframe['low']
    dataframe['AH'] = dataframe['CDP'] + PT
    dataframe['NH'] = dataframe['CDP'] * 2 - dataframe['low']
    dataframe['NL'] = dataframe['CDP'] * 2 - dataframe['high']
    dataframe['AL'] = dataframe['CDP'] - PT

# 定義心理線
def psy_line(dataframe, d = 12):
    days = dataframe['close'] > dataframe['close'].shift(periods = 1)
    dataframe['PSY'] = days.rolling(d, min_periods = d).mean()

# 定義成交量比率
def volumn_ratio(dataframe, d = 26):
    u_days = dataframe['close'] > dataframe['close'].shift(periods=1)
    p_days = dataframe['close'] == dataframe['close'].shift(periods=1)
    d_days = dataframe['close'] < dataframe['close'].shift(periods=1)
    u_volumns = dataframe['volume'].multiply(u_days.astype(int)).rolling(d, min_periods = d).sum()
    p_volumns = dataframe['volume'].multiply(p_days.astype(int)).rolling(d, min_periods = d).sum()
    d_volumns = dataframe['volume'].multiply(d_days.astype(int)).rolling(d, min_periods = d).sum()
    dataframe['VR'] = (u_volumns + p_volumns * 0.5)/(d_volumns + p_volumns * 0.5)

# 定義能量槽指標
def on_balance_volume(dataframe):
    rsv_1 = (dataframe['close'] - dataframe['low'])/(dataframe['high'] - dataframe['low'])
    dataframe['OBV'] = dataframe['volume'].multiply(rsv_1)

# 聯合技術指標群
# 乖離率、移動平均、變動率指標、MACD、布林通道、KD指標、威廉指標、相對強弱指數、順勢指標、逆勢操作指標、動量指標、趨向指標
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

# 定義lag變數，天數照時間先後排序。
def timesteps(dataframe, days = 20):
    notimesteps = ['date', 'high_change_class', 'low_change_class', 'close_change_class', 'close_change_class3']
    features = [column for column in dataframe.columns if column not in notimesteps]
    for d in range(days-1):
        for var in features:
            dataframe['{}_{}'.format(var, days-1-d)] = dataframe['{}'.format(var)].shift(days-1-d)
    for var in features:
        temp = dataframe['{}'.format(var)]
        dataframe.drop(columns=['{}'.format(var)], inplace=True)
        dataframe['{}'.format(var)] = temp

# 定義當日數值在過去n天的排序。
def value_rank(dataframe, var, d):
    date = dataframe.date.iloc[d-1:].reset_index(drop=True)
    rank_list = [dataframe[var].iloc[i:d + i].rank(method='min', ascending = False).iloc[-1] for i in range(len(dataframe)-(d-1))]
    rank = pd.Series(rank_list, name='rank')
    concat_rank = pd.concat([date, rank], axis=1)
    return pd.merge(dataframe, concat_rank, on='date', how='left')['rank']

# 定義過去n天漲跌次數多寡的排序。
def increase_counts(dataframe, var, d):
    date = dataframe.date.iloc[d-1:].reset_index(drop=True)
    increase_days = (dataframe[var] > dataframe[var].shift(1)).astype(int)
    increase_list = [increase_days.iloc[i:d + i].sum() for i in range(len(dataframe)-(d-1))]
    increase = pd.Series(increase_list, name='increase')
    rank = increase.rank(method='dense', ascending = False)
    concat_increase = pd.concat([date, rank], axis=1)
    return pd.merge(dataframe, concat_increase, on='date', how='left')['increase']

# 定義價量交互作用指標。
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

# 定義價與融資融券餘額交互作用指標。
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

# 定義時間格式轉換數字函數
def trans2yyyymmdd(date):
    yyyy = date[:4]
    mm = date[5:7]
    dd = date[-2:]
    yyyymmdd = yyyy + mm + dd
    return int(yyyymmdd)

# 定義時間格式轉換文字函數
def split2yyyymmdd(stryyyymmdd):
    yyyy = int(stryyyymmdd.replace('日', '').split('年')[0])
    mm = int(stryyyymmdd.replace('日', '').split('年')[1].split('月')[0])
    dd = int(stryyyymmdd.replace('日', '').split('年')[1].split('月')[1])
    date = datetime.date(yyyy, mm, dd)
    yyyymmdd = int(date.strftime("%Y%m%d"))
    return yyyymmdd

# 定義收盤價級距分類(7類)
def transchange2class(change):
    high = 1.5
    mild = 0.4
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

# 定義收盤價級距分類(3類)
def transclose2class3(change):
    cut = 0.5
    if change < -cut:
        return 0
    elif change >= cut:
        return 2
    else:
        return 1

# 定義台指級距分類(7類)
def TAIEX2class(change):
    high = 0.8
    mild = 0.4
    low = 0.1
    if change < -high:
        return 0
    if change >= -high and change < -mild:
        return 1
    if change >= -mild and change < -low:
        return 2
    if change >= -low and change < low:
        return 3
    if change >= low and change < mild:
        return 4
    if change >= mild and change < high:
        return 5
    if change > high:
        return 6

# 定義台指級距分類(3類)
def TAIEX2class3(change):
    cut = 0.33
    if change < -cut:
        return 0
    elif change >= cut:
        return 2
    else:
        return 1

# 期貨、外匯、期指資料整理
def futures_FX_index():
    futures = pd.read_csv('./futures/futures.csv', thousands = ",")

    TAIEX = pd.read_csv('./futures/TAIEX.csv')
    TAIEX = TAIEX[['date', 'close']].rename(columns={'close':'TAIEX'})

    all_days = pd.read_csv('./stock_clean/2330 台積電.csv', usecols=['date'])
    futures = pd.merge(all_days, futures, on='date', how='left')
    futures = pd.merge(futures, TAIEX, on='date', how='left')
    futures = futures.sort_values(by=['date']).reset_index(drop=True)

    values_list = futures.columns.tolist()
    values_list.remove('date')

    change_list = ['{}_change'.format(value) for value in values_list]
    change_list = list(zip(values_list, change_list))

    for value in values_list:
        futures[value].fillna(method='ffill', inplace=True)

    for value, change in change_list:
        futures[change] = (futures[value] - futures[value].shift(1)) * 100 / futures[value].shift(1)

    for index in ['TAIEX', 'SP500', 'SSEC', 'Dji30']:
        futures['{}_rank_5'.format(index)] = value_rank(futures, index, 5)
        futures['{}_rank_10'.format(index)] = value_rank(futures, index, 10)
        futures['{}_rank_20'.format(index)] = value_rank(futures, index, 20)

        futures['{}_increase_5'.format(index)] = increase_counts(futures, index, 5)
        futures['{}_increase_9'.format(index)] = increase_counts(futures, index, 9)
    return futures

# 股價、法人買賣資料整理
def Investors(shares):
    stock = pd.read_csv('./stock_clean/{}'.format(shares))
    for price in ['high', 'low', 'close']:
        stock['{}_change'.format(price)] = (stock[price] - stock[price].shift(1)) * 100 / stock[price].shift(1)
        stock['{}_change_class'.format(price)] = stock['{}_change'.format(price)].apply(transchange2class).astype(object)
        stock['{}_diff'.format(price)] = stock[price] - stock[price].shift(1)
    stock['close_change_class3'] = stock.close_change.apply(transclose2class3).astype(object)

    TA_processing(stock)

    stock['close_u'] = stock.close > stock.close.shift(1)
    stock['close_e'] = stock.close == stock.close.shift(1)
    stock['close_d'] = stock.close < stock.close.shift(1)
    stock['volume_u'] = stock.volume > stock.volume.shift(1)
    stock['volume_ed'] = stock.volume <= stock.volume.shift(1)

    stock['p_vol_pair'] = stock.apply(p_vol_compare, axis=1)
    stock.drop(columns=['volume_u', 'volume_ed'], inplace=True)

    df = pd.read_csv('./securities/{}'.format(shares), thousands=",")[
        ['date', '投信買進股數(總)', '投信賣出股數(總)', '投信買賣超股數(總)',
         '自營商買進股數(總)', '自營商賣出股數(總)', '自營商買賣超股數(總)',
         '外資買進股數(總)', '外資賣出股數(總)', '外資買賣超股數(總)']]
    columns_dic = {"投信買進股數(總)": "trust_buy", "投信賣出股數(總)": "trust_sell",
                   "自營商買進股數(總)": "dealer_buy", "自營商賣出股數(總)": "dealer_sell",
                   "外資買進股數(總)": "foriegn_buy", "外資賣出股數(總)": "foriegn_sell",
                   "投信買賣超股數(總)": "trust_diff", "自營商買賣超股數(總)": "dealer_diff",
                   "外資買賣超股數(總)": "foreign_diff"}
    df = df.rename(columns=columns_dic)

    merge_table = pd.merge(stock, df, on='date', how='left')
    for key in columns_dic:
        merge_table[columns_dic[key]].fillna(0, inplace=True)

    for index in ['close', 'high', 'low', 'trust_diff', 'dealer_diff', 'foreign_diff']:
        merge_table['{}_rank_5'.format(index)] = value_rank(merge_table, index, 5)
        merge_table['{}_rank_10'.format(index)] = value_rank(merge_table, index, 10)
        merge_table['{}_rank_20'.format(index)] = value_rank(merge_table, index, 20)

        merge_table['{}_increase_5'.format(index)] = increase_counts(merge_table, index, 5)
        merge_table['{}_increase_9'.format(index)] = increase_counts(merge_table, index, 9)
    return merge_table

# 融資融券餘額資料整理
def stock_margin(shares):
    short_sales = pd.read_csv('./securities_shortsale/{}'.format(shares), thousands=",")
    short_sales = short_sales.drop(columns=['證券名稱'], axis=1)

    balance = pd.read_csv('./securities_margin/{}'.format(shares), thousands=",")
    balance = balance.drop(columns=['股票代號', '股票名稱', 'margin_quota', 'short_sale','short_quota', '註記'], axis=1)

    balance['cash_balance_diff'] = balance.cash_balance - balance.cash_balance.shift(1)
    balance['stock_balance_diff'] = balance.stock_balance - balance.stock_balance.shift(1)

    for index in ['cash_balance_diff', 'stock_balance_diff']:
        balance['{}_rank_5'.format(index)] = value_rank(balance, index, 5)
        balance['{}_rank_10'.format(index)] = value_rank(balance, index, 10)
        balance['{}_rank_20'.format(index)] = value_rank(balance, index, 20)

        balance['{}_increase_5'.format(index)] = increase_counts(balance, index, 5)
        balance['{}_increase_9'.format(index)] = increase_counts(balance, index, 9)

    balance['cash_balance_u'] = balance.cash_balance > balance.cash_balance.shift(1)
    balance['cash_balance_d'] = balance.cash_balance <= balance.cash_balance.shift(1)
    balance['stock_balance_u'] = balance.stock_balance > balance.stock_balance.shift(1)
    balance['stock_balance_d'] = balance.stock_balance <= balance.stock_balance.shift(1)

    all_days = pd.read_csv('./stock_clean/{}'.format(shares), usecols=['date'])
    merge_short_sales = pd.merge(all_days, short_sales, on='date', how='left')
    merge_balance = pd.merge(merge_short_sales, balance, on='date', how='left')
    return merge_balance

# 資料依日期合併
def merge_date(shares):
    merge_table = pd.merge(Investors(shares), stock_margin(shares), on='date', how='left')
    merge_table['p_bal_pair'] = merge_table.apply(p_bal_compare, axis=1)
    merge_table.drop(columns=['close_u', 'close_e', 'close_d',
                              'cash_balance_u', 'cash_balance_d',
                              'stock_balance_u', 'stock_balance_d'], inplace=True)
    merge_table = merge_table.mask(merge_table.astype(object).eq('None')).dropna().reset_index(drop=True)
    return merge_table

# 輸出DNN模型所需的資料
def output_data():
    if not os.path.isdir("./class_training"):
        os.mkdir("./class_training")

    z_df = pd.DataFrame()
    futures = futures_FX_index()
    for shares in sorted(os.listdir('./stock_clean/')):
        if not shares.startswith('.'):
            df = pd.merge(merge_date(shares).round(4), futures, on='date', how='left')
            dirname = shares.split('.')[0]

            ycatego = ['high_change_class', 'low_change_class',
                       'close_change_class', 'close_change_class3']

            x_var = ['close', 'high', 'low',
                     'trust_diff', 'dealer_diff', 'foreign_diff',
                     'cash_balance_diff', 'stock_balance_diff',
                     'TAIEX', 'SP500', 'SSEC', 'Dji30']
            x_cls = ['rank_5', 'rank_10', 'rank_20', 'increase_5', 'increase_9']
            xcatego = ['{}_{}'.format(var, cls) for var in x_var for cls in x_cls]
            xcatego = xcatego + ['p_vol_pair', 'p_bal_pair']

            ti = ['BIAS3', 'BIAS6', 'BIAS10','BIAS25', 'EMA5', 'EMA10', 'EMA20',
                  'ROC', 'MACD', 'MACD_signal','UBBANDS', 'MBBANDS', 'LBBANDS',
                  '%K', '%D', 'W%R', 'RSI9', 'RSI14','CCI', 'CDP', 'AH', 'NH',
                  'NL', 'AL', 'MOM', 'DX', 'PSY', 'VR', 'OBV']

            not_z = ['date'] + ycatego + xcatego

            z_dic = {'stock_name':[dirname]}
            for column in df.columns:
                if column not in not_z:
                    z_dic['{}_means'.format(column)] = df[column].mean()
                    z_dic['{}_stds'.format(column)] = df[column].std()
                    if df[column].std() != 0:
                        df[column] = (df[column] - df[column].mean()) / df[column].std()
                    else:
                        print(shares, column, df[column].std())

            z_df = pd.DataFrame(z_dic) if z_df.empty else pd.concat([z_df, pd.DataFrame(z_dic)], ignore_index=True)

            if not os.path.isdir("./class_training"):
                os.mkdir("./class_training")

            if not os.path.isdir("./class_training/{}".format(dirname)):
                os.mkdir("./class_training/{}".format(dirname))

            cum = 0
            cum_df = df[['date'] + xcatego]
            for column in xcatego:
                cum_df[column] = cum_df[column].astype(int).apply(lambda num: num+cum)
                cum += cum_df[column].nunique()

                # sep_df = df[['date'] + [column]]
                # timesteps(sep_df, days=10)
                # sep_df = sep_df.mask(sep_df.astype(object).eq('None')).dropna().reset_index(drop=True)
                # sep_df.round(4).iloc[:-1].to_csv('./class_training/{}/{}.csv'.format(dirname, column), index=False)

            timesteps(cum_df, days=10)
            cum_df = cum_df.mask(cum_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            cum_df.astype(int).iloc[:-1].to_csv('./class_training/{}/one_embed.csv'.format(dirname), index=False)

            class_df = df[['date'] + xcatego]
            timesteps(class_df, days=10)
            class_df = class_df.mask(class_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            class_df.astype(int).iloc[:-1].to_csv('./class_training/{}/origin_class.csv'.format(dirname), index=False)

            TI_df = df.drop(columns=ycatego + xcatego, axis=1)
            timesteps(TI_df, days=10)
            TI_df = TI_df.mask(TI_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            TI_df.round(4).iloc[:-1].to_csv('./class_training/{}/Tech_I.csv'.format(dirname), index=False)

            other_df = df.drop(columns=ycatego + xcatego + ti, axis=1)
            timesteps(other_df, days=10)
            other_df = other_df.mask(other_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            other_df.round(4).iloc[:-1].to_csv('./class_training/{}/other(nonTI).csv'.format(dirname), index=False)

            y_train = df[['date', 'high', 'low', 'close',
                          'high_change_class', 'low_change_class',
                          'close_change_class', 'close_change_class3',
                          'high_change', 'low_change','close_change',
                          'high_diff', 'low_diff','close_diff'
                          ]].shift(periods=-1)
            y_train = y_train.mask(y_train.astype(object).eq('None')).dropna().reset_index(drop=True)

            y_train_price = y_train[['date', 'high', 'low', 'close']]
            y_train_price.round(4).iloc[9:].to_csv('./class_training/{}/y_train_price.csv'.format(dirname), index=False)

            y_train_change = y_train[['date', 'high_change', 'low_change','close_change']]
            y_train_change.round(4).iloc[9:].to_csv('./class_training/{}/y_train_change.csv'.format(dirname), index=False)

            y_train_change_class = y_train[['date', 'high_change_class', 'low_change_class', 'close_change_class','close_change_class3']]
            y_train_change_class.astype(int).iloc[9:].to_csv('./class_training/{}/y_train_change_class.csv'.format(dirname),index=False)

            y_train_diff = y_train[['date', 'high_diff', 'low_diff', 'close_diff']]
            y_train_diff.astype(int).iloc[9:].to_csv('./class_training/{}/y_train_diff.csv'.format(dirname),index=False)

            y_count = pd.DataFrame({'rank':list(range(7))})
            for column in ['high_change_class', 'low_change_class', 'close_change_class', 'close_change_class3']:
                change_class = pd.DataFrame({'rank':list(y_train_change_class[column].value_counts().sort_index().index),
                                             '{}'.format(column):list(y_train_change_class[column].value_counts().sort_index())})
                y_count = pd.merge(y_count, change_class, on='rank', how='left')
            y_count.fillna(0, inplace=True)
            y_count.astype(int).to_csv('./class_training/{}/change_class.csv'.format(dirname), index=False)
            print(dirname, '已完成')
    z_df.to_csv('./class_training/Z_data.csv', index=False)

# 輸出GAN模型所需的資料
def output_gandata():
    if not os.path.isdir("./gan"):
        os.mkdir("./gan")

    z_df = pd.DataFrame()
    futures = futures_FX_index()
    for shares in sorted(os.listdir('./stock_clean/')):
        if not shares.startswith('.'):
            df = pd.merge(merge_date(shares).round(4), futures, on='date', how='left')
            dirname = shares.split('.')[0]


            ycatego = ['high_change_class', 'low_change_class',
                       'close_change_class', 'close_change_class3']

            x_var = ['close', 'high', 'low',
                     'trust_diff', 'dealer_diff', 'foreign_diff',
                     'cash_balance_diff', 'stock_balance_diff',
                     'TAIEX', 'SP500', 'SSEC', 'Dji30']
            x_cls = ['rank_5', 'rank_10', 'rank_20', 'increase_5', 'increase_9']
            xcatego = ['{}_{}'.format(var, cls) for var in x_var for cls in x_cls]
            xcatego = xcatego + ['p_vol_pair', 'p_bal_pair']

            not_z = ['date'] + ycatego + xcatego

            z_dic = {'stock_name':[dirname]}
            for column in df.columns:
                if column not in not_z:
                    z_dic['{}_means'.format(column)] = df[column].mean()
                    z_dic['{}_stds'.format(column)] = df[column].std()
                    if df[column].std() != 0:
                        df[column] = (df[column] - df[column].mean()) / df[column].std()
                    else:
                        print(shares, column, df[column].std())

            z_df = pd.DataFrame(z_dic) if z_df.empty else pd.concat([z_df, pd.DataFrame(z_dic)], ignore_index=True)

            g_time = 1
            steps = 8

            if not os.path.isdir("./gan"):
                os.mkdir("./gan")

            if not os.path.isdir("./gan/{}".format(dirname)):
                os.mkdir("./gan/{}".format(dirname))

            class_df = df[['date'] + xcatego]
            timesteps(class_df, days=steps)
            class_df = class_df.mask(class_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            class_df.astype(int).iloc[:-g_time].to_csv('./gan/{}/origin_class.csv'.format(dirname), index=False)

            other_df = df.drop(columns=ycatego+xcatego, axis=1)
            timesteps(other_df, days=steps)
            other_df = other_df.mask(other_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            other_df.round(4).iloc[:-g_time].to_csv('./gan/{}/other.csv'.format(dirname), index=False)

            features = ['date', 'open', 'high', 'low', 'close',
                        'high_diff', 'low_diff', 'close_diff']
            x_train_chosen = df[features]
            timesteps(x_train_chosen, days=steps)
            x_train_chosen = x_train_chosen.mask(x_train_chosen.astype(object).eq('None')).dropna().reset_index(drop=True)
            x_train_chosen.round(4).iloc[:-g_time].to_csv('./gan/{}/x_train_chosen.csv'.format(dirname), index=False)

            y_train = df[features].shift(periods=-g_time)
            y_train = y_train.mask(y_train.astype(object).eq('None')).dropna().reset_index(drop=True)
            y_train.round(4).iloc[steps-1:].to_csv('./gan/{}/y_train.csv'.format(dirname), index=False)
            print(dirname, '已完成')

    z_df.to_csv('./gan/Z_data.csv', index=False)

# 輸出AUTOENCODER模型所需的資料
def output_autodata():
    if not os.path.isdir("./auto"):
        os.mkdir("./auto")

    z_df = pd.DataFrame()
    futures = futures_FX_index()
    for shares in sorted(os.listdir('./stock_clean/')):
        if not shares.startswith('.'):
            df = pd.merge(merge_date(shares).round(4), futures, on='date', how='left')
            dirname = shares.split('.')[0]

            for price in ['high', 'low', 'close']:
                df['{}_change'.format(price)] = df['{}_change'.format(price)].shift(-1)
                df['{}_change_class'.format(price)] = df['{}_change_class'.format(price)].shift(-1)
            df = df.mask(df.astype(object).eq('None')).dropna().reset_index(drop=True)

            x_var = ['close', 'high', 'low',
                     'trust_diff', 'dealer_diff', 'foreign_diff',
                     'cash_balance_diff', 'stock_balance_diff',
                     'TAIEX', 'SP500', 'SSEC', 'Dji30']
            x_cls = ['rank_5', 'rank_10', 'rank_20', 'increase_5', 'increase_9']
            xcatego = ['{}_{}'.format(var, cls) for var in x_var for cls in x_cls]
            xcatego = xcatego + ['p_vol_pair', 'p_bal_pair']
            change_cls = ['high_change_class', 'low_change_class', 'close_change_class', 'close_change_class3']
            not_z = ['date'] + xcatego + change_cls

            z_dic = {'stock_name':[dirname]}
            for column in df.columns:
                if column not in not_z:
                    z_dic['{}_means'.format(column)] = df[column].mean()
                    z_dic['{}_stds'.format(column)] = df[column].std()
                    if df[column].std() != 0:
                        df[column] = (df[column] - df[column].mean()) / df[column].std()
                    else:
                        print(shares, column, df[column].std())

            for column in ['high_change', 'low_change','close_change', 'high_diff', 'low_diff','close_diff']:
                z_dic['{}_max'.format(column)] = df[column].max()
                z_dic['{}_min'.format(column)] = df[column].min()
                df[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())

            z_df = pd.DataFrame(z_dic) if z_df.empty else pd.concat([z_df, pd.DataFrame(z_dic)], ignore_index=True)

            if not os.path.isdir("./auto"):
                os.mkdir("./auto")

            if not os.path.isdir("./auto/{}".format(dirname)):
                os.mkdir("./auto/{}".format(dirname))

            class_df = df[['date'] + xcatego + change_cls]
            timesteps(class_df, days=10)
            class_df = class_df.mask(class_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            class_df.astype(int).to_csv('./auto/{}/origin_class.csv'.format(dirname), index=False)

            other_df = df.drop(columns = xcatego, axis=1)
            timesteps(other_df, days=10)
            other_df = other_df.mask(other_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            other_df.round(4).to_csv('./auto/{}/other.csv'.format(dirname), index=False)

            train_change_class = other_df[change_cls]

            change_count = pd.DataFrame({'rank':list(range(7))})
            for column in change_cls:
                change_class = pd.DataFrame({'rank':list(train_change_class[column].value_counts().sort_index().index),
                                             '{}'.format(column):list(train_change_class[column].value_counts().sort_index())})
                change_count = pd.merge(change_count, change_class, on='rank', how='left')
            change_count.fillna(0, inplace=True)
            change_count.astype(int).to_csv('./auto/{}/change_class.csv'.format(dirname), index=False)
            print(dirname, '已完成')
    z_df.to_csv('./auto/Z_data.csv', index=False)

# 輸出原始資料
def output_rawdata():
    futures = futures_FX_index()
    for shares in sorted(os.listdir('./stock_clean/')):
        if not shares.startswith('.'):
            df = pd.merge(merge_date(shares).round(4), futures, on='date', how='left')
            dirname = shares.split('.')[0]

            ycatego = ['high_change_class', 'low_change_class',
                       'close_change_class', 'close_change_class3']

            x_var = ['close', 'high', 'low',
                     'trust_diff', 'dealer_diff', 'foreign_diff',
                     'cash_balance_diff', 'stock_balance_diff',
                     'TAIEX', 'SP500', 'SSEC', 'Dji30']
            x_cls = ['rank_5', 'rank_10', 'rank_20', 'increase_5', 'increase_9']
            xcatego = ['{}_{}'.format(var, cls) for var in x_var for cls in x_cls]
            xcatego = xcatego + ['p_vol_pair', 'p_bal_pair']

            if not os.path.isdir("./training_raw"):
                os.mkdir("./training_raw")

            if not os.path.isdir("./training_raw/{}".format(dirname)):
                os.mkdir("./training_raw/{}".format(dirname))

            class_df = df[['date'] + xcatego]
            class_df = class_df.mask(class_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            class_df.astype(int).iloc[:-1].to_csv('./training_raw/{}/origin_class.csv'.format(dirname), index=False)

            other_df = df.drop(columns=ycatego+xcatego, axis=1)
            other_df = other_df.mask(other_df.astype(object).eq('None')).dropna().reset_index(drop=True)
            other_df.round(4).iloc[:-1].to_csv('./training_raw/{}/other.csv'.format(dirname), index=False)

            y_train = df[['date', 'high', 'low', 'close',
                          'high_change_class', 'low_change_class',
                          'close_change_class', 'close_change_class3',
                          'high_change', 'low_change','close_change'
                          ]].shift(periods=-1)
            y_train = y_train.mask(y_train.astype(object).eq('None')).dropna().reset_index(drop=True)

            y_train_price = y_train[['date', 'high', 'low', 'close']]
            y_train_price.round(4).to_csv('./training_raw/{}/y_train_price.csv'.format(dirname), index=False)

            y_train_change = y_train[['date', 'high_change', 'low_change','close_change']]
            y_train_change.round(4).to_csv('./training_raw/{}/y_train_change.csv'.format(dirname), index=False)

            y_train_change_class = y_train[['date', 'high_change_class', 'low_change_class', 'close_change_class','close_change_class3']]
            y_train_change_class.astype(int).to_csv('./training_raw/{}/y_train_change_class.csv'.format(dirname),
                                                 index=False)

            y_count = pd.DataFrame({'rank':list(range(7))})
            for column in ['high_change_class', 'low_change_class', 'close_change_class', 'close_change_class3']:
                change_class = pd.DataFrame({'rank':list(y_train_change_class[column].value_counts().sort_index().index),
                                             '{}'.format(column):list(y_train_change_class[column].value_counts().sort_index())})
                y_count = pd.merge(y_count, change_class, on='rank', how='left')
            y_count.fillna(0, inplace=True)
            y_count.astype(int).to_csv('./training_raw/{}/change_class.csv'.format(dirname), index=False)
            print(dirname, '已完成')

# 輸出台指及標普500、道瓊指數
def output_TAIEXdata():
    df = futures_FX_index().round(4)
    df = df.mask(df.astype(object).eq('None')).dropna().reset_index(drop=True)
    df['TAIEX_change_class'] = df.TAIEX_change.apply(TAIEX2class).astype(object)
    df['TAIEX_change_class3'] = df.TAIEX_change.apply(TAIEX2class3).astype(object)

    x_var = ['TAIEX', 'SP500', 'Dji30']
    x_cls = ['rank_5', 'rank_10','rank_20','increase_5', 'increase_9']
    xcatego = ['{}_{}'.format(var, cls) for var in x_var for cls in x_cls]

    x_change = ['{}_change'.format(var) for var in x_var]
    x_var = x_var + x_change

    ycatego = ['TAIEX_change_class', 'TAIEX_change_class3']
    not_z = ['date'] + ycatego + xcatego

    df = df[['date'] + x_var + xcatego + ycatego]
    for column in df.columns:
        if column not in not_z:
            if df[column].std() != 0:
                df[column] = (df[column] - df[column].mean()) / df[column].std()
            else:
                print(column, df[column].std())

    if not os.path.isdir("./TAIEX"):
        os.mkdir("./TAIEX")

    cum = 0
    cum_df = df[['date'] + xcatego]
    for column in xcatego:
        cum_df[column] = cum_df[column].astype(int).apply(lambda num: num + cum)
        cum += cum_df[column].nunique()

    timesteps(cum_df, days=5)
    cum_df = cum_df.mask(cum_df.astype(object).eq('None')).dropna().reset_index(drop=True)
    cum_df.astype(int).iloc[:-1].to_csv('./TAIEX/one_embed.csv', index=False)

    timesteps(df, days=5)
    df = df.mask(df.astype(object).eq('None')).dropna().reset_index(drop=True)
    x_train = df[['date'] + x_var]
    x_train.round(4).iloc[:-1].to_csv('./TAIEX/x_train.csv', index=False)

    y_train = df[['date', 'TAIEX_change'] + ycatego].shift(periods=-1)
    y_train = y_train.mask(y_train.astype(object).eq('None')).dropna().reset_index(drop=True)
    y_train.round(4).to_csv('./TAIEX/y_train.csv', index=False)

    y_count = pd.DataFrame({'rank':list(range(7))})
    for column in ycatego:
        change_class = pd.DataFrame({'rank':list(y_train[column].value_counts().sort_index().index),
                                     '{}'.format(column):list(y_train[column].value_counts().sort_index())})
        y_count = pd.merge(y_count, change_class, on='rank', how='left')
    y_count.fillna(0, inplace=True)
    y_count.astype(int).to_csv('./TAIEX/change_class.csv', index=False)
    print('TAIEX已完成')

if __name__ == '__main__':
    # d = stock50.Daily_trade_data()
    # d.update_data()
    # TAI = stock50.TAIEX()
    # TAI.update_data()
    # crawler_investor.crawler_and_update()
    # crawler_margin.crawler_and_update()
    # crawler_margin_financing.crawler_and_update()
    # crawler_futures.update_futures()
    # output_data()
    output_gandata()
    # output_autodata()
    # output_rawdata()
    # output_TAIEXdata()