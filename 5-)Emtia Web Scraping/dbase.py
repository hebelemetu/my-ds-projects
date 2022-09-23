import sqlite3

import pandas as pd


class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def insert_brent(self,name,last,chg,open,high,low,date,url):
        self.cur.execute("INSERT INTO brent_petrol VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (name, last, chg, open, high, low, date,url))
        self.conn.commit()

    def insert_ttf(self,name,last,chg,open,high,low,date,url):
        self.cur.execute("INSERT INTO ttf_gas VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (name, last, chg, open, high, low, date,url))
        self.conn.commit()

    def insert_api2(self,name,last,chg,open,high,low,date,url):
        self.cur.execute("INSERT INTO api2_data VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (name, last, chg, open, high, low, date,url))
        self.conn.commit()

    def fetch_brent(self):
        self.cur.execute("SELECT * FROM brent_petrol")
        rows = self.cur.fetchall()
        return rows

    def db_to_df(self,query,columns):
        sql_query = pd.read_sql_query("",self.conn)
        df = pd.DataFrame(sql_query, columns = columns)
        df.head()
        return df
