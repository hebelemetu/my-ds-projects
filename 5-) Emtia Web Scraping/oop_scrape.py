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
from datetime import date,datetime,timedelta
from dbase import Database
import time

class Scrapes:
    def __init__(self):
        self.db = Database('emtia_db.db')

    def api2_ice(self):
        db = Database('emtia_db.db')
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")
        url_ice = f"https://www.theice.com/products/243/API2-Rotterdam-Coal-Futures/data"
        start = date.today().month + 1
        start_month = date.today() + timedelta(days=30)
        print(start)
        left = 13 - start
        print(left)
        rng = left + 12
        TURN = 0
        while TURN < 200:
            try:
                start_time = time.time()
                driver_ice = webdriver.Chrome(options=chrome_options)
                driver_ice.get(url_ice)
                time.sleep(5)
                count = driver_ice.find_elements(by=By.XPATH,
                                                 value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody')
                for i in range(1, len(count) + 1):
                    try:
                        item = driver_ice.find_element(by=By.XPATH,
                                                       value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[1]')
                        last = driver_ice.find_element(by=By.XPATH,
                                                       value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[2]')
                        change = driver_ice.find_element(by=By.XPATH,
                                                         value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[4]')
                        api2_prd = item.get_attribute("innerText")
                        try:
                            api2_lst = float(last.get_attribute("innerText"))
                        except ValueError:
                            api2_lst = ""
                        try:
                            api2_chg = float(change.get_attribute("innerText"))
                        except ValueError:
                            api2_chg = ""
                        print(f"PRODUCT:api2{api2_prd}\nPrice Last: {api2_lst}\nPrice Change: {api2_chg}")
                        db.insert_api2(str("ice_" + str(api2_prd)), api2_lst, api2_chg, "", "", "", datetime.now(),
                                       url_ice)
                        time.sleep(0.02)
                    except AttributeError:
                        continue
                final_time = (time.time() - start_time) / 60
                print("Runtime API2 ICE:--- %s minutes ---" % final_time)
            except:
                print("ERROR api2 ice!!!")
            time.sleep(900)
            TURN += 1

    def api2_mw(self):
        def divide_chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]

        url = "https://www.marketwatch.com/investing/future/mtfc00"

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")

        driver = webdriver.Chrome(options=chrome_options)

        count = 0
        while count < 200:
            try:
                driver.get(url)

                html_content = driver.page_source

                soup = BeautifulSoup(html_content, "html.parser")

                brent_price = [item.text for item in soup.find_all('div', attrs={
                    "class": "element element--table overflow--table FuturesContracts"})[0].find_all('td', attrs={
                    "class": "table__cell"})]

                prices = list(divide_chunks(brent_price, 7))

                for price in prices:
                    self.db.insert_api2(price[0], float(str(price[1]).split('$')[1]), price[2],
                                   float(str(price[3]).split('$')[1]), float(str(price[4]).split('$')[1]),
                                   float(str(price[5]).split('$')[1]), datetime.now(), url)

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

    def api2_barchart(self):
        db = Database('emtia_db.db')
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")

        products_all = []

        this_year = (date.today()).strftime("%y")
        next_year = (date.today() + timedelta(days=365)).strftime("%y")
        year_after_next = (date.today() + timedelta(days=730)).strftime("%y")
        next_month = (date.today() + timedelta(days=30)).strftime('%b')

        products = {"Jan": "ITFF",
                    "Feb": "ITFG",
                    "Mar": "ITFH",
                    "Apr": "ITFJ",
                    "May": "ITFK",
                    "Jun": "ITFM",
                    "Jul": "ITFN",
                    "Aug": "ITFQ",
                    "Sep": "ITFU",
                    "Oct": "ITFV",
                    "Nov": "ITFX",
                    "Dec": "ITFZ"}

        start = date.today().month
        left = 13 - start + 1
        count = 0
        months = []

        start = date.today().month
        start_month = date.today()
        print(start)
        left = 13 - start
        count = 0
        months = []
        while count < left:
            days_add = 30 * count
            month = (start_month + timedelta(days=days_add)).strftime("%b")
            print(month)
            if month == next_month:
                products_all.append(str(products.get(month)) + str(this_year))
            else:
                products_all.append(str(products.get(month)) + str(this_year))
            count += 1

        for key, value in products.items():
            products_all.append(str(products.get(key)) + str(next_year))
        print(products_all)

        TURN = 0
        while TURN < 200:
            try:
                start_time = time.time()
                for item in products_all:
                    url = f"https://www.barchart.com/futures/quotes/{item}/overview"
                    print(url)
                    driver = webdriver.Chrome(options=chrome_options)
                    driver.get(url)

                    phigh = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div.small-6.column.text-right > div:nth-child(3)')
                    plow = driver.find_element(by=By.CSS_SELECTOR,
                                               value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div:nth-child(1) > div:nth-child(3)')
                    pname = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div.symbol-name > h1 > span.symbol')
                    plast = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.last-change.ng-binding')
                    try:
                        popen = driver.find_element(by=By.CSS_SELECTOR,
                                                    value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope.marking > div.mark.ng-scope > span')
                        price_open = float(popen.get_attribute("innerText").split(" ")[1])
                    except NoSuchElementException:
                        try:
                            try:
                                popen = driver.find_element(by=By.CSS_SELECTOR,
                                                            value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope > div.mark.ng-scope > span')
                                price_open = float(popen.get_attribute("innerText").split(" ")[1])
                            except ValueError:
                                price_open = ""
                        except NoSuchElementException:
                            price_open = ""

                    try:
                        pchg = driver.find_element(by=By.CSS_SELECTOR,
                                                   value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.down > span.last-change.ng-binding')
                        price_chg = float(pchg.get_attribute("innerText"))
                    except NoSuchElementException:
                        try:
                            pchg = driver.find_element(by=By.CSS_SELECTOR,
                                                       value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.up > span.last-change.ng-binding')
                            price_chg = float(pchg.get_attribute("innerText"))
                        except:
                            price_chg = ""

                    price_name = pname.get_attribute("innerText")
                    try:
                        price_last = float(plast.get_attribute("innerText").split(" ")[0])
                    except ValueError:
                        price_last = plast.get_attribute("innerText").split(" ")[0].split('s')[0]
                        if price_last == "N/A":
                            price_last = ""
                        else:
                            price_last = float(price_last)

                    if phigh.get_attribute("innerText") == 'N/A':
                        price_high = ""
                    else:
                        price_high = float(phigh.get_attribute("innerText"))

                    if plow.get_attribute("innerText") == 'N/A':
                        price_low = ""
                    else:
                        price_low = float(phigh.get_attribute("innerText"))

                    print(
                        f"PRODUCT:{price_name}\nPrice Last: {price_last}\nPrice Change: {price_chg}\nPrice Open: {price_open}\nPrice High: {price_high}\nPrie Low: {price_low}")
                    db.insert_api2(price_name, price_last, price_chg, price_open, price_high, price_low, datetime.now(),
                                   url)
                    print("Data recorded to database!")
                    driver.quit()
            except:
                print("ERROR!! api2 barchart")
            TURN += 1
            final_time = (time.time() - start_time) / 60
            print("Runtime:--- %s minutes ---" % final_time)

    def bp_mw(self):
        def divide_chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]

        url = "https://www.marketwatch.com/investing/future/brn00?countrycode=uk"

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")

        driver = webdriver.Chrome(options=chrome_options)

        count = 0
        while count < 200:
            try:
                driver.get(url)

                html_content = driver.page_source

                soup = BeautifulSoup(html_content, "html.parser")

                brent_price = [item.text for item in soup.find_all('div', attrs={
                    "class": "element element--table overflow--table FuturesContracts"})[0].find_all('td', attrs={
                    "class": "table__cell"})]

                prices = list(divide_chunks(brent_price, 7))

                for price in prices:
                    self.db.insert_brent(price[0], float(str(price[1]).split('$')[1]), price[2],
                                    float(str(price[3]).split('$')[1]), float(str(price[4]).split('$')[1]),
                                    float(str(price[5]).split('$')[1]), datetime.now(), url)

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

    def bp_barchart(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")
        products_all = []

        this_year = (date.today()).strftime("%y")
        next_year = (date.today() + timedelta(days=365)).strftime("%y")
        year_after_next = (date.today() + timedelta(days=730)).strftime("%y")
        next_month = (date.today() + timedelta(days=30)).strftime('%b')

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
        left = 13 - start + 1
        count = 0
        months = []

        start = date.today().month + 1
        start_month = date.today() + timedelta(days=30)
        print(start)
        left = 13 - start
        count = 0
        months = []
        while count < left:
            days_add = 30 * count
            month = (start_month + timedelta(days=days_add)).strftime("%b")
            print(month)
            products_all.append(str(products.get(month)) + str(this_year))
            count += 1

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
        for key, value in products.items():
            products_all.append(str(products.get(key)) + str(next_year))
        print(products_all)

        TURN = 0
        while TURN < 200:
            try:
                start_time = time.time()
                for item in products_all:
                    url = f"https://www.barchart.com/futures/quotes/{item}/overview"
                    print(url)
                    driver = webdriver.Chrome(options=chrome_options)
                    driver.get(url)

                    phigh = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div.small-6.column.text-right > div:nth-child(3)')
                    plow = driver.find_element(by=By.CSS_SELECTOR,
                                               value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div:nth-child(1) > div:nth-child(3)')
                    pname = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div.symbol-name > h1 > span.symbol')
                    plast = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.last-change.ng-binding')
                    try:
                        popen = driver.find_element(by=By.CSS_SELECTOR,
                                                    value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope.marking > div.mark.ng-scope > span')
                        price_open = float(popen.get_attribute("innerText").split(" ")[1])
                    except NoSuchElementException:
                        try:
                            popen = driver.find_element(by=By.CSS_SELECTOR,
                                                        value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope > div.mark.ng-scope > span')
                            try:
                                price_open = float(popen.get_attribute("innerText").split(" ")[1])
                            except:
                                price_open = ""
                        except NoSuchElementException:
                            price_open = ""

                    try:
                        pchg = driver.find_element(by=By.CSS_SELECTOR,
                                                   value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.down > span.last-change.ng-binding')
                    except NoSuchElementException:
                        pchg = driver.find_element(by=By.CSS_SELECTOR,
                                                   value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.up > span.last-change.ng-binding')

                    price_name = pname.get_attribute("innerText")
                    try:
                        price_last = float(plast.get_attribute("innerText").split(" ")[0])
                    except ValueError:
                        price_last = float(plast.get_attribute("innerText").split(" ")[0].split('s')[0])
                    price_chg = float(pchg.get_attribute("innerText"))
                    price_high = float(phigh.get_attribute("innerText"))
                    price_low = float(plow.get_attribute("innerText"))

                    print(
                        f"PRODUCT:{price_name}\nPrice Last: {price_last}\nPrice Change: {price_chg}\nPrice Open: {price_open}\nPrice High: {price_high}\nPrie Low: {price_low}")
                    self.db.insert_brent(price_name, price_last, price_chg, price_open, price_high, price_low,
                                    datetime.now(), url)
                    print("Data recorded to database!")
                    driver.quit()
                TURN += 1
                final_time = (time.time() - start_time) / 60
                print("Runtime:--- %s minutes ---" % final_time)
            except:
                print("ERROR!! bp barchart")

    def bp_ice(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")
        url_ice = f"https://www.theice.com/products/219/Brent-Crude-Futures/data"
        start = date.today().month + 1
        start_month = date.today() + timedelta(days=30)
        print(start)
        left = 13 - start
        print(left)
        rng = left + 12
        TURN = 0
        while TURN < 200:
            try:
                start_time = time.time()
                driver_ice = webdriver.Chrome(options=chrome_options)
                driver_ice.get(url_ice)
                time.sleep(5)
                count = driver_ice.find_elements(by=By.XPATH,
                                                 value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody')
                for i in range(1, len(count) + 1):
                    try:
                        item = driver_ice.find_element(by=By.XPATH,
                                                       value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[1]')
                        last = driver_ice.find_element(by=By.XPATH,
                                                       value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[2]')
                        change = driver_ice.find_element(by=By.XPATH,
                                                         value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[4]')
                        brent_prd = item.get_attribute("innerText")
                        try:
                            brent_lst = float(last.get_attribute("innerText"))
                        except ValueError:
                            brent_lst = ""
                        try:
                            brent_chg = float(change.get_attribute("innerText"))
                        except ValueError:
                            brent_chg = ""
                        print(f"PRODUCT:Brent_Petrol{brent_prd}\nPrice Last: {brent_lst}\nPrice Change: {brent_chg}")
                        self.db.insert_brent(str("ice_" + str(brent_prd)), brent_lst, brent_chg, "", "", "", datetime.now(),
                                        url_ice)
                        time.sleep(0.02)
                    except AttributeError:
                        continue
                final_time = (time.time() - start_time) / 60
                print("Runtime BP ICE:--- %s minutes ---" % final_time)
            except:
                print("ERROR!!! bp ice")
            time.sleep(900)
            TURN += 1

    def ttf_ice(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")
        url_ice = f"https://www.theice.com/products/27996665/Dutch-TTF-Gas-Futures/data"
        start = date.today().month + 1
        start_month = date.today() + timedelta(days=30)
        print(start)
        left = 13 - start
        print(left)
        rng = left + 12
        TURN = 0
        while TURN < 200:
            try:
                start_time = time.time()
                driver_ice = webdriver.Chrome(options=chrome_options)
                driver_ice.get(url_ice)
                time.sleep(5)
                count = driver_ice.find_elements(by=By.XPATH,
                                                 value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody')
                for i in range(1, len(count) + 1):
                    try:
                        item = driver_ice.find_element(by=By.XPATH,
                                                       value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[1]')
                        last = driver_ice.find_element(by=By.XPATH,
                                                       value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[2]')
                        change = driver_ice.find_element(by=By.XPATH,
                                                         value=f'/html/body/div[1]/div/div[1]/div[3]/div[1]/div/div/div[4]/div/div/div[1]/table/tbody[{i}]/tr/td[4]')
                        TTF_prd = item.get_attribute("innerText")
                        try:
                            TTF_lst = float(last.get_attribute("innerText"))
                        except ValueError:
                            TTF_lst = ""
                        try:
                            TTF_chg = float(change.get_attribute("innerText"))
                        except ValueError:
                            TTF_chg = ""
                        print(f"PRODUCT:TTF{TTF_prd}\nPrice Last: {TTF_lst}\nPrice Change: {TTF_chg}")
                        self.db.insert_ttf(str("ice_" + str(TTF_prd)), TTF_lst, TTF_chg, "", "", "", datetime.now(), url_ice)
                        time.sleep(0.02)
                    except AttributeError:
                        continue
                final_time = (time.time() - start_time) / 60
                print("Runtime TTF ICE:--- %s minutes ---" % final_time)
            except:
                print("ERROR!!! ttf ice")
            time.sleep(900)
            TURN += 1

    def ttf_barchart(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36")

        products = {"Jan": "TGF",
                    "Feb": "TGG",
                    "Mar": "TGH",
                    "Apr": "TGJ",
                    "May": "TGK",
                    "Jun": "TGM",
                    "Jul": "TGN",
                    "Aug": "TGQ",
                    "Sep": "TGU",
                    "Oct": "TGV",
                    "Nov": "TGX",
                    "Dec": "TGZ"}

        products_all = []

        this_year = (date.today()).strftime("%y")
        next_year = (date.today() + timedelta(days=365)).strftime("%y")
        year_after_next = (date.today() + timedelta(days=730)).strftime("%y")

        start = date.today().month + 1
        start_month = date.today() + timedelta(days=30)
        print(start)
        left = 13 - start
        count = 0
        months = []

        while count < left:
            days_add = 30 * count
            month = (start_month + timedelta(days=days_add)).strftime("%b")
            print(month)
            products_all.append(str(products.get(month)) + str(this_year))
            count += 1

        for key, value in products.items():
            products_all.append(str(products.get(key)) + str(next_year))
        print(products_all)

        TURN = 0
        while TURN < 200:
            try:
                start_time = time.time()
                for item in products_all:
                    url = f"https://www.barchart.com/futures/quotes/{item}/overview"
                    print(url)
                    driver = webdriver.Chrome(options=chrome_options)
                    driver.get(url)

                    phigh = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div.small-6.column.text-right > div:nth-child(3)')
                    plow = driver.find_element(by=By.CSS_SELECTOR,
                                               value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row > div:nth-child(1) > div:nth-child(3)')
                    pname = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div.symbol-name > h1 > span.symbol')
                    plast = driver.find_element(by=By.CSS_SELECTOR,
                                                value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.last-change.ng-binding')
                    try:
                        popen = driver.find_element(by=By.CSS_SELECTOR,
                                                    value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope.marking > div.mark.ng-scope > span')
                    except NoSuchElementException:
                        popen = driver.find_element(by=By.CSS_SELECTOR,
                                                    value='#main-content-column > div > div.bc-quote-overview.row.ng-scope > div.small-12.large-5.column > div.bc-quote-row-chart > div.row-chart.ng-isolate-scope > div.mark.ng-scope > span')

                    try:
                        pchg = driver.find_element(by=By.CSS_SELECTOR,
                                                   value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.down > span.last-change.ng-binding')
                    except NoSuchElementException:
                        pchg = driver.find_element(by=By.CSS_SELECTOR,
                                                   value='#main-content-column > div > div.page-title.symbol-header-info.ng-scope > div:nth-child(2) > span.up > span.last-change.ng-binding')

                    price_name = pname.get_attribute("innerText")
                    try:
                        price_last = float(plast.get_attribute("innerText").split(" ")[0])
                    except ValueError:
                        price_last = float(plast.get_attribute("innerText").split(" ")[0].split('s')[0])
                    price_chg = float(pchg.get_attribute("innerText"))
                    price_open = float(popen.get_attribute("innerText").split(" ")[1])
                    price_high = float(phigh.get_attribute("innerText"))
                    price_low = float(plow.get_attribute("innerText"))

                    print(
                        f"PRODUCT:{price_name}\nPrice Last: {price_last}\nPrice Change: {price_chg}\nPrice Open: {price_open}\nPrice High: {price_high}\nPrie Low: {price_low}")
                    print(
                        f"Taken from url: {url} TTF prices are:\nPrice Last: {price_last}\nPrice Change {price_chg}\nPrice Open {price_open}\nPrice High {price_high}\nPrie Low {price_low}")
                    self.db.insert_ttf(price_name, price_last, price_chg, price_open, price_high, price_low, datetime.now(),
                                  url)
                    print("Data recorded to database!")
                    driver.quit()
                final_time = round((time.time() - start_time) / 60, 1)
                print("Runtime:--- %s minutes ---" % final_time)
            except:
                print("ERROR!! ttf barchart")
            TURN += 1


if __name__ == '__main__':
    scraper = Scrapes()
    scraper.api2_ice()