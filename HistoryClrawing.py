import datetime, time
import requests
from bs4 import BeautifulSoup
import os, csv

os.chdir("D:\\dropbox\\python\\tseHistoryClrawing")

#TODO: 設定起迄日期，西元年作運算，python支援計算，先由2004/10/15日開始。
startDate = time.strptime("2014/10/06", "%Y"+'/'+'%m'+'/'+"%d") #轉換文字到時間格式(time.strptime)
endDate = time.strptime("2017/04/20", "%Y"+'/'+'%m'+'/'+"%d") #轉換文字到時間格式(time.strptime)
startDate = datetime.date(startDate[0], startDate[1], startDate[2]) #讀取日期…
endDate = datetime.date(endDate[0], endDate[1], endDate[2])
rangeDate = datetime.timedelta(days = 1)

while startDate <= endDate :
    #TODO: 取得中華民國年月日。
    taiwanDate = str(int(startDate.strftime("%Y")) - 1911) + '/' + startDate.strftime("%m/%d")
    starTime = time.time() #取得讀取網頁的當下時間。
    #TODO: 使用requests.post送入taiwanDate
    data = dict(qdate = taiwanDate)
    html=requests.post("http://www.tse.com.tw/ch/trading/exchange/MI_5MINS_INDEX/MI_5MINS_INDEX.php", data)
    bsObj = BeautifulSoup(html.text,'html.parser')
    table = bsObj.find('table')
    rows = table.findAll("tr")
    readTime = time.time()
    print("讀取時間：%s秒 \n" % (readTime - starTime))

    #TODO: 將資料寫入txt檔案。
    txtname = rows[0].get_text() + '.txt'
    txt = open(txtname,'w',encoding='UTF-8')
    txt.close()
    txt = open(txtname,'a',encoding='UTF-8')
    #TODO: 將資料寫入csv檔案。
    csvname = rows[0].get_text() + '.csv'
    csvfile = open(csvname,'w',encoding='BIG5')
    csvfile.close()
    csvfile = open(csvname,'a',encoding='BIG5', newline='')
    csvwriter = csv.writer(csvfile, dialect= 'excel')
    print(rows[0].get_text() +"寫入檔案中…\n")

    for row in range(len(rows)):
        cols = rows[row].findAll("td")
        csvcol=[]
        for col in range(len(cols)):
            txta = cols[col].get_text() + '\t'
            txt.write(txta)
            csvcol.append(cols[col].get_text())
            
        txt.write('\n')
        csvwriter.writerow(csvcol)    
    txt.close()
    csvfile.close()
    print(rows[0].get_text() + "寫入完成。\n")
    endTime = time.time()
    print("抓取使用時間：%s 秒\n" % (endTime - starTime))

    time.sleep(5)
    startDate = startDate + rangeDate
