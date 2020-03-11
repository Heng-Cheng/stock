import requests
import datetime
import time
import os
import margin_clean

def split_yyyymmdd(yyyymmdd):
  date = str(yyyymmdd)
  yyyy = int(date[:4])
  mm = int(date[4:6])
  dd = int(date[-2:])
  return yyyy, mm, dd

def str_yyyy_mm_dd(yyyymmdd):
  date = str(yyyymmdd)
  yyyy = date[:4]
  mm = date[4:6]
  dd = date[-2:]
  return yyyy, mm, dd

def next_day(yyyymmdd):
  yyyy, mm, dd = split_yyyymmdd(yyyymmdd)
  this_day = datetime.date(yyyy, mm, dd)
  next_day = this_day + datetime.timedelta(days = 1)
  next_yyyymmdd = int(next_day.strftime("%Y%m%d"))
  return next_yyyymmdd

def next_month(yyyymmdd):
  yyyy, mm, dd = split_yyyymmdd(yyyymmdd)
  if mm < 12:
      next_mm = mm + 1
      next_yyyy = yyyy
  else:
      next_mm = 1
      next_yyyy = yyyy + 1
  next_month = datetime.date(next_yyyy, next_mm, dd)
  next_yyyymmdd = int(next_month.strftime("%Y%m%d"))
  return next_yyyymmdd

def find_last_day():
    path0 = "./Short Sales"
    yyyy = sorted([dir for dir in os.listdir(path0) if not dir.startswith('.')])[-1]

    path1 = "./Short Sales/{}".format(yyyy)
    mm = sorted([dir for dir in os.listdir(path1) if not dir.startswith('.')])[-1]

    path2 = "./Short Sales/{}/{}".format(yyyy, mm)
    last_file = sorted([file for file in os.listdir(path2) if not file.startswith('.')])[-1]
    last_day = int(os.path.splitext(last_file)[0])
    return last_day

def makedir(date):
    yyyy, mm, dd = str_yyyy_mm_dd(date)
    path0 = "./Short Sales"
    path1 = "./Short Sales/{}".format(yyyy)
    path2 = "./Short Sales/{}/{}".format(yyyy, mm)
    if not os.path.exists(path0):
        os.mkdir(path0)
        os.mkdir(path1)
        os.mkdir(path2)
    elif not os.path.exists(path1):
        os.mkdir(path1)
        os.mkdir(path2)
    elif not os.path.exists(path2):
        os.mkdir(path2)
    else:
        pass

def crawler_and_update():
    if not os.path.exists("./Short Sales/2008/09"):
        date = 20080926
    else:
        date = next_day(find_last_day())
    today = int(datetime.date.today().strftime("%Y%m%d"))

    while date <= today:
        yyyy, mm, dd = str_yyyy_mm_dd(date)
        url = 'https://www.twse.com.tw/exchangeReport/TWTASU?response=csv&date={}'.format(date)
        response = requests.get(url)

        makedir(date)
        filename = "./Short Sales/{}/{}/{}.csv".format(yyyy, mm, date)

        if "證券名稱" in response.text:
            with open(filename, 'w', encoding = "utf-8") as f:
                f.write(response.text)
            print('{}已下載完畢'.format(filename))
            margin_clean.update_short_sale(date)
            time.sleep(5)
        else:
            print('{}無資料'.format(date))
        date = next_day(date)

if __name__ == '__main__':
    crawler_and_update()