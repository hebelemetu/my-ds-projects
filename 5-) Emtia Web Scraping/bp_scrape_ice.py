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
url_ice = f"https://www.theice.com/products/219/Brent-Crude-Futures/data"
driver_ice = webdriver.Chrome(options = chrome_options)
driver_ice.get(url_ice)
time.sleep(5)
start = date.today().month+1
start_month = date.today()+timedelta(days=30)
print(start)
left= 13-start
print(left)
rng = left+12

TURN = 0
while TURN<200:
    try:
        start_time = time.time()
        for i in range(1,rng):
            try:
                item = driver_ice.find_element(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody[{i}]/tr[1]/td[1]/a')
                last = driver_ice.find_element(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody[{i}]/tr[1]/td[2]')
                change = driver_ice.find_element(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody[{i}]/tr[1]/td[4]')
                brent_prd = item.get_attribute("innerText")
                try:
                    brent_lst = float(last.get_attribute("innerText"))
                except ValueError:
                    brent_lst = ""
                try:
                    brent_chg = float(change.get_attribute("innerText"))
                except ValueError:
                    brent_chg = ""
                print(f"PRODUCT:{brent_prd}\nPrice Last: {brent_lst}\nPrice Change: {brent_chg}")
                db.insert_brent(str("ice_"+str(brent_prd)),brent_lst,brent_chg,"","","",datetime.now(),url_ice)
                time.sleep(0.02)
            except AttributeError:
                continue
        final_time = (time.time() - start_time) / 60
        print("Runtime:--- %s minutes ---" % final_time)
    except:
        print("ERROR!!!")
    time.sleep(900)
    TURN += 1
