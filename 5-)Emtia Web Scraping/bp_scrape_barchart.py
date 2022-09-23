from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date,datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import date,datetime,timedelta
import time
from dbase import Database

db = Database('emtia_db.db')


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--incognito')
chrome_options.add_argument( "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")
products_all = []


this_year = (date.today()).strftime("%y")
next_year = (date.today()+timedelta(days=365)).strftime("%y")
year_after_next = (date.today()+timedelta(days=730)).strftime("%y")
next_month = (date.today()+timedelta(days=30)).strftime('%b')

products = {"Jan": "QAF",
          "Feb": "QAG",
          "Mar": "QAH",
          "Apr": "QAJ",
          "May": "QAK",
          "Jun": "QAM",
          "Jul": "QAN",
          "Aug": "QAQ",
          "Sep": "QAU",
          "Oct": "QAV",
          "Nov": "QAX",
          "Dec": "QAZ"}

start = date.today().month
left= 13-start+1
count = 0
months = []

start = date.today().month+1
start_month = date.today()+timedelta(days=30)
print(start)
left= 13-start
count =0
months = []
while count<left:
    days_add = 30*count
    month = (start_month+timedelta(days=days_add)).strftime("%b")
    print(month)
    products_all.append(str(products.get(month))+str(this_year))
    count+=1


products = {"Jan": "QAF",
          "Feb": "QAG",
          "Mar": "QAH",
          "Apr": "QAJ",
          "May": "QAK",
          "Jun": "QAM",
          "Jul": "QAN",
          "Aug": "QAQ",
          "Sep": "QAU",
          "Oct": "QAV",
          "Nov": "QAX",
          "Dec": "QAZ"}
for key,value in products.items():
    products_all.append(str(products.get(key))+str(next_year))
print(products_all)

TURN = 0
while TURN<200:
    start_time = time.time()
    for item in products_all:
        url = f"https://www.barchart.com/futures/quotes/{item}/overview"
        print(url)
        driver = webdriver.Chrome(options = chrome_options)
        driver.get(url)



        phigh = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div.small-6.column.text-right > div:nth-child(3)')
        plow = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div:nth-child(1) > div:nth-child(3)')
        pname = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div.symbol-name > h1 > span.symbol')
        plast = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.last-change.ng-binding')
        try:
            popen = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope.marking > div.mark.ng-scope > span')
            price_open = float(popen.get_attribute("innerText").split(" ")[1])
        except NoSuchElementException:
            try:
                popen = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope > div.mark.ng-scope > span')
                try:
                    price_open = float(popen.get_attribute("innerText").split(" ")[1])
                except:
                    price_open = ""
            except NoSuchElementException:
                price_open = ""

        try:
            pchg = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.down > span.last-change.ng-binding')
        except NoSuchElementException:
            pchg = driver.find_element(by = By.CSS_SELECTOR,value = '#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.up > span.last-change.ng-binding')


        price_name = pname.get_attribute("innerText")
        try:
            price_last = float(plast.get_attribute("innerText").split(" ")[0])
        except ValueError:
            price_last = float(plast.get_attribute("innerText").split(" ")[0].split('s')[0])
        price_chg = float(pchg.get_attribute("innerText"))
        price_high = float(phigh.get_attribute("innerText"))
        price_low = float(plow.get_attribute("innerText"))


        print(f"PRODUCT:{price_name}\nPrice Last: {price_last}\nPrice Change: {price_chg}\nPrice Open: {price_open}\nPrice High: {price_high}\nPrie Low: {price_low}")
        db.insert_brent(price_name,price_last,price_chg,price_open,price_high,price_low,datetime.now(),url)
        print("Data recorded to database!")
        driver.quit()
    TURN += 1
    final_time = (time.time() - start_time) / 60
    print("Runtime:--- %s minutes ---" % final_time)