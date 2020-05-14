import sched, time
from binance.client import Client
from binance.websockets import BinanceSocketManager
import requests
import pprint
import json
import sqlite3
ups_db = "ups.db"

PUBLIC_KEY = "a4ZzJYKTaMJoTBLSi9Nx849YEsen4ZOOBviNfeIYBz4r1gU24KDN6O8wxOeefCCr"
SECRET_KEY = "CaE2RTCNYSV5ATDZ6MFtzfQVUXviPm1Mo4OxdkZmS6sx6N3d9ZfATjpT7cJUHFPj"
updates_f = {}
updates_b = {}

#perform contained operations on each update that passes through our stream
def update_update(msg):
    updates_f[msg['U']] = msg
    updates_b[msg['u']] = msg
    # insert_update_database(msg['U'],msg['u'],msg['a'],msg['b'])

#write to database
def create_update_database():
    conn = sqlite3.connect(ups_db)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ups (firstUpdateId int, finalUpdateId int, asks text, bids text);''')
    conn.commit()
    conn.close()

def insert_update_database(first,final,asks,bids):
    conn = sqlite3.connect(ups_db)
    c = conn.cursor()
    c.execute('''INSERT into ups VALUES ({},{},'{}','{}');'''.format(first,final,json.dumps(asks),json.dumps(bids)))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    #create_update_database()
    client = Client(api_key=PUBLIC_KEY, api_secret=SECRET_KEY)
    bm = BinanceSocketManager(client)
    depthstream = bm.start_depth_socket("BNBBTC", update_update)
    bm.start()
