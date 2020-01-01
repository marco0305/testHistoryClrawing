# -*- coding: UTF-8 -*-
#!python3
#!/usr/bin/python
#20190130 version 1.0
#add to raspberry system.@2019/12/10
#modify the os.path.join to twin-os: windows and linux. @ 2020/01/01

import pandas as pd
import sqlite3
import os, sys
import time
import datetime
import random
import urllib
from urllib import request
import requests
from bs4 import BeautifulSoup
import zipfile
import http


def mtx_download():
    #os.chdir(os.getcwd() + "/mtx_zipdata/")
    os.chdir(os.path.join(os.getcwd(), "mtx_zipdata"))
    #到期貨網頁去，抓取30日檔案資料。
    #先確認網頁是否存在。
    url = 'http://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData'
    
    #--------------------------------
    while True:
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"}
            r = requests.get(url, headers = headers)
        except ValueError as err:
            #顯示錯誤資訊並寫入log檔中。
            print(date_str + " " + str(err) + ":今日資料或沒開盤！")
            log_info(date_str + " " + str(err) + ":今日資料或沒開盤！")
            datas = pd.DataFrame()
            stat = 0
            break
        except requests.exceptions.ChunkedEncodingError as err:
            print(str(err))
            time.sleep(600)
            continue
        except requests.exceptions.ConnectionError as err:
            print(str(err))
            time.sleep(600)
            continue
        except http.client.HTTPException as err:
            print(str(err))
            time.sleep(600)
            continue
        break
    #-------------------------------

    #抓取日期並比對現有資料。
    res = BeautifulSoup(r.text, 'html.parser')
    res = res.findAll("table")[1].findAll("tr")

    date = []
    for i in range(1,len(res)):
        d_date = res[i].findAll("td")[1].text.split("/")
        string = "Daily_" + d_date[0] + "_" + d_date[1] + "_" + d_date[2] + ".csv"
        date.append(string)
    #打開資料zip
    path = os.getcwd()
    sourcefile = zipfile.ZipFile(os.getcwd() + "/mtx_zipdate.zip", 'a')
    datas = sourcefile.namelist()

    for d in date:
        if (d in datas):
            pass
            #print(d, "已存檔！")
            #print(sourcefile.getinfo(d).file_size)
            
        else:
            print(d, "不存在！")
            #下載
            url = "http://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/" + d.split('.')[0] + ".zip"
            print(url)
            work_path = os.path.join(path,url.split('/')[-1])
            request.urlretrieve(url,work_path)
            print(d, "download OK!")
            #解壓縮
            downfile = zipfile.ZipFile(work_path)
            #驗證下載的檔案是否有問題，跟比對資料更新的時間。
            if (downfile.testzip() == None) & (downfile.getinfo(d).date_time[3] >= 16):
                downfile.extract(downfile.namelist()[0], path)
                #寫入壓縮
                sourcefile.write(d, compress_type=zipfile.ZIP_DEFLATED)
                log_info(str(d) + " mtx be downloaded.")

                print(d, "存檔完畢，休息30秒")
                os.remove(d)
                downfile.close()
                os.remove(d.split('.')[0] + ".zip")
                print(d, "刪除完成！")
                time.sleep(10)
            else:
                print(downfile.getinfo(d).date_time)
                downfile.close()
                print(d, "還沒晚上六點，資料還未更新完畢，下次重新下載。")
                time.sleep(10)
            
    sourcefile.close()

    #os.chdir("/home/pi/Documents/Finance")
    os.chdir(home_dir)



def twii_zip():
    #os.chdir(os.getcwd() + "/INDEX/")
    os.chdir(os.path.join(os.getcwd(), "INDEX"))
    sourcefile = zipfile.ZipFile(os.getcwd() + "/twii_index.zip", 'a')
    datas = sourcefile.namelist()
    for f in os.listdir():
        if f.endswith('.csv') and (f not in datas):
            print(f, 'zip to twii_index.zip.')
            sourcefile.write(f, compress_type=zipfile.ZIP_DEFLATED)
        elif f.endswith('.csv') and (f in datas):
            print(f, 'remove success.')
            os.remove(f)
        else:
            print(f, 'is not csv file.')
    sourcefile.close()
    


def downloadData(date_str, sqlitedb):
    print(date_str + "開始抓取大盤資料：twii_index.")
    date_str = date_str
    url = "http://www.twse.com.tw/exchangeReport/MI_5MINS_INDEX?response=html&date="    
    while True:
        try:
            print(url + date_str)
            datas = pd.read_html(url + date_str)
        except ValueError as err:
            #顯示錯誤資訊並寫入log檔中。
            print(date_str + " " + str(err) + ":今日資料或沒開盤！")
            log_info(date_str + " " + str(err) + ":今日資料或沒開盤！")
            datas = pd.DataFrame()
            stat = 0
            break
        except urllib.error.URLError as err:
            #顯示錯誤資訊並寫入log檔中。
            print(date_str + " " + str(err))
            log_info(date_str + " Error: " + str(err))
            time.sleep(600)
            continue
        except http.client.HTTPException as err:
            print(str(err))
            time.sleep(600)
            continue
        break

    
    if (len(datas) != 0):
        datass = pd.DataFrame(datas[0])
        columns = []
        for d in datass.columns.values:
            #print(d, d[1])
            columns.append(d[1])
        while True:
            try:
                temp = datass.columns.values[0][0]
                temp = temp.strip()
                #datass.to_csv(".\\INDEX\\" + datass.columns.values[0][0] + ".csv", encoding = 'big5')
                datass.to_csv("./INDEX/" + str(temp) + ".csv", encoding = 'big5')
            except OSError as err:
                #顯示錯誤資訊並寫入log檔中。
                print(date_str + " " + str(err) + " :csv 寫入錯誤")
                log_info(date_str + " Error: " + str(err) + " :csv 寫入錯誤")
                print(date_str + " : 等10秒後，重試存檔。")
                time.sleep(10)
                temp = datass.columns.values[0][0]
                temp = temp.strip()
                print(temp)
                datas = pd.read_html(url + date_str)
                datass = pd.DataFrame(datas[0])
                continue
            break

        print(datass.columns.values[0][0] + ".csv 存檔成功。")
        datass.columns = columns
        datass['日期'] = pd.Series(date_str, index=datass.index)
        temp = datass.columns.tolist()
        temp.remove("日期")
        temp.insert(0, "日期")
        datass = datass.reindex(columns = temp)
        datas = datass.iloc[:,0:3]
        #datas.columns = ['日期', '時間', '加權指數']

        datas.columns = ['Date', 'Time', 'twii']
        datas.index = pd.to_datetime(datas.Date + " " + datas.Time)
        datas = datas.drop(columns=['Date', 'Time'])

        conn = sqlite3.connect(sqlitedb)
        datas.to_sql('twii_index', conn, if_exists = "append", index_label='Datetime')
        Klines(sqlitedb, datas)
        conn.commit()
        conn.close()
        log_info(date_str + " :twii_index寫入完成！")
        print(date_str + " :twii_index寫入完成！")
        stat = 1
    else:
        pass
    return stat

def Klines(sqlitedb, DataFrame):
    #build the k line data.
    date = DataFrame.index[0]
    #DataFrame.index = pd.to_datetime(DataFrame.日期 + " " + DataFrame.時間)
    DataFrame.index.name = 'Datetime'
    #DataFrame = DataFrame.drop(columns=['日期', '時間'])
    ohlc_dict = {'Open':'first','High':'max','Low':'min','Close':'last'}
    
    conn = sqlite3.connect(sqlitedb)

    #rawdata to min5K:
    min5K = DataFrame[1:-1].resample('5min').apply(ohlc_dict, closed='left', label='left') #五分k線
    min5K.columns = ['Open', 'High', 'Low', 'Close']
    #min5K 13:25:00 columns: close is error. its modified.
    min5K.Close[53] = DataFrame.twii[-1]
    print(min5K.head())
    print(min5K.tail())
    print("---" * len(min5K.columns))
    min5K.to_sql('min5K', conn, if_exists = "append", index_label='Datetime')

    #rawdata to dayKK:
    dayK = DataFrame[1:].resample('D').apply(ohlc_dict, closed='left', label='left') #日K線
    dayK.columns = ['Open', 'High', 'Low', 'Close']
    print(dayK)
    print("---" * len(min5K.columns))
    dayK.to_sql('dayK', conn, if_exists = "append", index_label='Datetime')

    conn.commit()
    conn.close()
    print(str(date) + " min5K and dayK were load to db.")
    log_info(str(date) + " min5K and dayK were load to db.")

def main():
    start = datetime.date(2004, 10, 15)
    delta = datetime.timedelta(days = 1)

    if datetime.datetime.today().time() < datetime.time(14, 0, 0, 0): #讀取現在時間，如果是下午三點前
        end = datetime.datetime.today().date() - delta #就將日期設為前一天。
        
    else:
        end = datetime.datetime.today().date() #不是的話就設為當天。
    dataed = findataed()
    t = 0
    while start <= end:
        date_str = str(int(start.strftime("%Y%m%d"))) #將開始日期轉為文字 20041015。
        
        if date_str not in dataed:
            print(date_str)
            #run downloadData to sport out twii_index, csvfile, min5K and dayK.
            stat = downloadData(date_str, "twii_index.db")
            if stat == 0:
                #no table to sleep(1)
                time.sleep(1)
                t += 1
                if t > 3:
                    s = random.randint(10, 20)
                    print('Twii_index were downloaded, and now wait for ' + str(s) + " second to re-download.\n-")    
                    time.sleep(s)

            elif stat == 1:
                #downloaded data to sleep.
                s = random.randint(20, 30)
                print('Twii_index were downloaded, and now wait for ' + str(s) + " second to download next day.\n-")    
                time.sleep(s)
                t = 0 
        start = start + delta
    print('指數區塊抓取完畢。')

def log_info(info):
    tm = datetime.datetime.now().isoformat(sep=' ', timespec='seconds')
    txt = open("./log_info.txt", 'a', encoding='utf-8')
    txt.write(str(tm) + " " + str(info) + '\n')
    txt.close()

def findataed():
    txt = open("./log_info.txt", 'r', encoding='utf-8')
    dataed = []
    for line in txt:
        if (line[20:28] not in dataed) and (line[29:34] != "Error"):
            dataed.append(line[20:28])
    txt.close()
    return dataed

home_dir = str(os.getcwd())
main()
mtx_download()
twii_zip()