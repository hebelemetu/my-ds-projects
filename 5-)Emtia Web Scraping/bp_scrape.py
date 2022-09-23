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


url = "https://www.marketwatch.com/investing/future/brn00?countrycode=uk"


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
            db.insert_brent(price[0], float(str(price[1]).split('$')[1]), price[2], float(str(price[3]).split('$')[1]), float(str(price[4]).split('$')[1]), float(str(price[5]).split('$')[1]), datetime.now(),url)

        brent_name = [item[0] for item in prices]
        brent_last = [item[1] for item in prices]
        brent_chg = [item[2] for item in prices]
        brent_open = [item[3] for item in prices]
        brent_high = [item[4] for item in prices]
        brent_low = [item[5] for item in prices]
        brent_time = datetime.now()

        brent_future = {"Names": brent_name,
                        "Price_last": brent_last,
                        "Price_chg": brent_chg,
                        "Price_open": brent_open,
                        "Price_high": brent_high,
                        "Price_low": brent_low,
                        "Time": brent_time}

        brent = pd.DataFrame()

        brent["names"] = brent_future["Names"]
        brent["price_last"] = brent_future["Price_last"]
        brent["price_chg"] = brent_future["Price_chg"]
        brent["price_open"] = brent_future["Price_open"]
        brent["price_high"] = brent_future["Price_high"]
        brent["price_low"] = brent_future["Price_low"]
        brent["price_time"] = brent_future["Time"]

        print("DONE")
        print(brent)
    except:
        print("ERROR")
    count += 1
    time.sleep(900)



