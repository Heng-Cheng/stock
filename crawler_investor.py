import requests
import datetime
import time
import os
import ssl
import investors_merge

ssl._create_default_https_context = ssl._create_unverified_context

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


def find_last_day(investors):
    path0 = "./{}".format(investors)
    yyyy = sorted([dir for dir in os.listdir(path0) if not dir.startswith('.')])[-1]

    path1 = "./{}/{}".format(investors, yyyy)
    mm = sorted([dir for dir in os.listdir(path1) if not dir.startswith('.')])[-1]

    path2 = "./{}/{}/{}".format(investors, yyyy, mm)
    last_file = sorted([file for file in os.listdir(path2) if not file.startswith('.')])[-1]
    last_day = int(os.path.splitext(last_file)[0])
    return last_day

def find_last_day_all():
    last_list = []
    for investors in ['Dealers', 'Trust', 'Foriegn']:
        path0 = "./{}".format(investors)
        yyyy = sorted([dir for dir in os.listdir(path0) if not dir.startswith('.')])[-1]

        path1 = "./{}/{}".format(investors, yyyy)
        mm = sorted([dir for dir in os.listdir(path1) if not dir.startswith('.')])[-1]

        path2 = "./{}/{}/{}".format(investors, yyyy, mm)
        last_file = sorted([file for file in os.listdir(path2) if not file.startswith('.')])[-1]
        last_day = int(os.path.splitext(last_file)[0])
        last_list.append(last_day)
    return max(last_list)

def geturl(date, investors):
    if investors == "Dealers":
        url = "https://www.twse.com.tw/fund/TWT43U?response=csv&date={}".format(date)
    elif investors == "Trust":
        url = "https://www.twse.com.tw/fund/TWT44U?response=csv&date={}".format(date)
    elif investors == "Foriegn":
        url = "https://www.twse.com.tw/fund/TWT38U?response=csv&date={}".format(date)
    return url

def makedir(date, investors):
    yyyy, mm, dd = str_yyyy_mm_dd(date)
    path0 = "./{}".format(investors)
    path1 = "./{}/{}".format(investors, yyyy)
    path2 = "./{}/{}/{}".format(investors, yyyy, mm)
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

def crawler(investors):
    if not os.path.exists("./{}/2004/12".format(investors)):
        date = 20041217
    else:
        date = next_day(find_last_day(investors))
    today = int(datetime.date.today().strftime("%Y%m%d"))

    while date <= today:
        yyyy, mm, dd = str_yyyy_mm_dd(date)
        url = geturl(date, investors)
        response = requests.get(url)

        makedir(date, investors)
        filename = "./{}/{}/{}/{}.csv".format(investors, yyyy, mm, date)

        if "證券代號" in response.text:
            with open(filename, 'w', encoding = "utf-8") as f:
                f.write(response.text)
            print('{}{}已下載完畢'.format(investors, filename))
            time.sleep(7)
        else:
            print('{}{}無資料'.format(investors, date))
        date = next_day(date)

def crawler_and_update():
    date = next_day(find_last_day_all())
    today = int(datetime.date.today().strftime("%Y%m%d"))

    while date <= today:
        yyyy, mm, dd = str_yyyy_mm_dd(date)
        for investors in ['Dealers', 'Trust', 'Foriegn']:
            url = geturl(date, investors)
            response = requests.get(url)

            makedir(date, investors)
            filename = "./{}/{}/{}/{}.csv".format(investors, yyyy, mm, date)

            if "證券代號" in response.text:
                with open(filename, 'w', encoding = "utf-8") as f:
                    f.write(response.text)
                print('{}{}已下載完畢'.format(investors, filename))
                time.sleep(7)
            else:
                print('{}{}無資料'.format(investors, date))

        investors_merge.update_investor(date)

        date = next_day(date)

if __name__ == '__main__':
    crawler_and_update()