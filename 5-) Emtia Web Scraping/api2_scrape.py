from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date,datetime
from dbase import Database
import time

db = Database('emtia_db.db')

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


url = "https://www.marketwatch.com/investing/future/mtfc00"


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--incognito')
chrome_options.add_argument( "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")

driver = webdriver.Chrome(options = chrome_options)

count = 0
while count < 200:
    try:
        driver.get(url)

        html_content = driver.page_source

        soup = BeautifulSoup(html_content, "html.parser")

        brent_price = [item.text for item in soup.find_all('div', attrs={"class": "element element--table overflow--table FuturesContracts"})[0].find_all('td',attrs = {"class":"table__cell"})]

        prices = list(divide_chunks(brent_price,7))

        for price in prices:
            db.insert_api2(price[0], float(str(price[1]).split('$')[1]), price[2], float(str(price[3]).split('$')[1]), float(str(price[4]).split('$')[1]), float(str(price[5]).split('$')[1]), datetime.now(),url)

        api2_name = [item[0] for item in prices]
        api2_last = [item[1] for item in prices]
        api2_chg = [item[2] for item in prices]
        api2_open = [item[3] for item in prices]
        api2_high = [item[4] for item in prices]
        api2_low = [item[5] for item in prices]
        api2_time = datetime.now()

        api2_future = {"Names": api2_name,
                        "Price_last": api2_last,
                        "Price_chg": api2_chg,
                        "Price_open": api2_open,
                        "Price_high": api2_high,
                        "Price_low": api2_low,
                        "Time": api2_time}

        api2 = pd.DataFrame()

        api2["names"] = api2_future["Names"]
        api2["price_last"] = api2_future["Price_last"]
        api2["price_chg"] = api2_future["Price_chg"]
        api2["price_open"] = api2_future["Price_open"]
        api2["price_high"] = api2_future["Price_high"]
        api2["price_low"] = api2_future["Price_low"]
        api2["price_time"] = api2_future["Time"]

        print("DONE")
        print(api2)
    except:
        print("ERROR")
    count += 1
    time.sleep(900)



