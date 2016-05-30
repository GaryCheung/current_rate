from bs4 import BeautifulSoup
import requests
import pymysql
from datetime import datetime

config = {
    'host':'127.0.0.1',
    'port':8889,
    'user':'root',
    'password':'root',
    'db':'currency',
    'charset':'utf8',
    'unix_socket':'/Applications/MAMP/tmp/mysql/mysql.sock'
}

present_date = datetime.now().date()

def delete_today_data(config):
    connection = pymysql.connect(**config)
    try:
        with connection.cursor() as cursor:
            # 执行sql语句，插入记录
            sql = "DELETE FROM currency where date = %s"
            cursor.execute(sql,(present_date))
            # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
        connection.commit()
    finally:
        connection.close()
    print('Executed on-----------',present_date,'\n','-----------------------delete success!----------------','\n')

def get_boc_currency_data(config,source):
    url = 'http://www.boc.cn/sourcedb/whpj/'
    web_data = requests.get(url)
    soup = BeautifulSoup(web_data.text,'lxml')
    # print(soup)
    names = soup.select('div.publish > div:nth-of-type(2) > table > tr > td:nth-of-type(1)')
    buy_xianchaoes = soup.select('div.publish > div:nth-of-type(2) > table > tr > td:nth-of-type(3)')
    buy_xianhuies = soup.select('div.publish > div:nth-of-type(2) > table > tr > td:nth-of-type(2)')
    sell_xianhuies = soup.select('div.publish > div:nth-of-type(2) > table > tr > td:nth-of-type(4)')
    sell_xianchaoes = soup.select('div.publish > div:nth-of-type(2) > table > tr > td:nth-of-type(5)')
    times = soup.select('div.publish > div:nth-of-type(2) > table > tr > td:nth-of-type(8)')
    for name,buy_xianchao,buy_xianhui,sell_xianchao,sell_xianhui,time in zip(names,buy_xianchaoes,buy_xianhuies,sell_xianchaoes,sell_xianhuies,times):
        name = name.get_text().encode('latin-1').decode('utf-8')
        buy_xianhui = buy_xianhui.get_text()
        buy_xianchao = buy_xianchao.get_text()
        sell_xianhui = sell_xianhui.get_text()
        sell_xianchao = sell_xianchao.get_text()
        time = time.get_text()
        print('name',name,'--buy_xianhui',buy_xianhui,'---buy_xianchao',buy_xianchao,'---sell_xianchao',sell_xianchao,'---sell_xianhui',sell_xianhui,'---time',time)
        connection = pymysql.connect(**config)
        try:
            with connection.cursor() as cursor:
                # 执行sql语句，插入记录
                sql = 'INSERT INTO currency (name,buy_xianhui,buy_xianchao,sell_xianhui,sell_xianchao,date,time,source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                cursor.execute(sql, (name,buy_xianhui,buy_xianchao,sell_xianhui,sell_xianchao,present_date,time,source))
                # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
            connection.commit()
        finally:
            connection.close()

def get_cmb_currency_data(config,source):
    url = 'http://fx.cmbchina.com/hq/'
    web_data = requests.get(url)
    soup = BeautifulSoup(web_data.text,'lxml')
    # print(soup)
    names = soup.select('div.box.hq > div > table > tr > td:nth-of-type(1)')
    buy_xianchaoes = soup.select('div.box.hq > div > table > tr > td:nth-of-type(8)')
    buy_xianhuies = soup.select('div.box.hq > div > table > tr > td:nth-of-type(7)')
    sell_xianhuies = soup.select('div.box.hq > div > table > tr > td:nth-of-type(5)')
    sell_xianchaoes = soup.select('div.box.hq > div > table > tr > td:nth-of-type(6)')
    times = soup.select('div.box.hq > div > table > tr > td:nth-of-type(9)')
    for name,buy_xianchao,buy_xianhui,sell_xianchao,sell_xianhui,time in zip(names,buy_xianchaoes,buy_xianhuies,sell_xianchaoes,sell_xianhuies,times):
        name = name.get_text()
        name = name.strip()
        buy_xianhui = buy_xianhui.get_text()
        buy_xianchao = buy_xianchao.get_text()
        sell_xianhui = sell_xianhui.get_text()
        sell_xianchao = sell_xianchao.get_text()
        time = time.get_text()
        print('---name',name,'--buy_xianhui',buy_xianhui,'---buy_xianchao',buy_xianchao,'---sell_xianchao',sell_xianchao,'---sell_xianhui',sell_xianhui,'---time',time)
        connection = pymysql.connect(**config)
        try:
            with connection.cursor() as cursor:
                # 执行sql语句，插入记录
                sql = 'INSERT INTO currency (name,buy_xianhui,buy_xianchao,sell_xianhui,sell_xianchao,date,time,source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                cursor.execute(sql, (name,buy_xianhui,buy_xianchao,sell_xianhui,sell_xianchao,present_date,time,source))
                # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
            connection.commit()
        finally:
            connection.close()



source = ['boc','cmb']
delete_today_data(config)
get_boc_currency_data(config,source[0])
get_cmb_currency_data(config,source[1])
