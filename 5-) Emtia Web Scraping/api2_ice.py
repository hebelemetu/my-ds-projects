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
url_ice = f"https://www.theice.com/products/243/API2-Rotterdam-Coal-Futures/data"
start = date.today().month+1
start_month = date.today()+timedelta(days=30)
print(start)
left= 13-start
print(left)
rng = left+12
TURN = 0
while TURN<200:
    try:
        driver_ice = webdriver.Chrome(options=chrome_options)
        driver_ice.get(url_ice)
        time.sleep(5)
        start_time = time.time()
        count = driver_ice.find_elements(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody')
        for i in range(1,len(count)+1):
            try:
                item = driver_ice.find_element(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody[{i}]/tr[1]/td[1]/a')
                last = driver_ice.find_element(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody[{i}]/tr[1]/td[2]')
                change = driver_ice.find_element(by = By.XPATH,value = f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[2]/div/div/table/tbody[{i}]/tr[1]/td[4]')
                api2_prd = item.get_attribute("innerText")
                try:
                    api2_lst = float(last.get_attribute("innerText"))
                except ValueError:
                    api2_lst = ""
                try:
                    api2_chg = float(change.get_attribute("innerText"))
                except ValueError:
                    api2_chg = ""
                print(f"PRODUCT:{api2_prd}\nPrice Last: {api2_lst}\nPrice Change: {api2_chg}")
                db.insert_api2(str("ice_"+str(api2_prd)),api2_lst,api2_chg,"","","",datetime.now(),url_ice)
                time.sleep(0.02)
            except AttributeError:
                continue
        final_time = (time.time() - start_time) / 60
        print("Runtime:--- %s minutes ---" % final_time)
    except:
        print("ERROR!!!")
    time.sleep(900)
    TURN += 1