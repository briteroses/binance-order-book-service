import sched, time
import requests
import pprint
import json
import sqlite3
snaps_db = "snaps.db"

CURRENCY = "BNBBTC"
LIMIT = 1000
DELAY = 30
snapshots = {}

#convert snapshots into efficient data structure
s = sched.scheduler(time.time, time.sleep)
def update_snapshots():
    order_book = {}
    r = requests.get("https://www.binance.com/api/v1/depth?symbol={}&limit={}".format(CURRENCY,LIMIT))
    snap = r.json()
    least_ask, greatest_bid = snap['asks'][0][0], snap['bids'][0][0]
    for k in ['asks', 'bids']:
        for i in range(len(snap[k])):
            f_pointer = snap[k][i+1][0] if i < LIMIT-1 else None
            b_pointer = snap[k][i-1][0] if i > 0 else None
            order_book[snap[k][i][0]] = [snap[k][i][1], f_pointer, b_pointer]
    snapshots[snap['lastUpdateId']] = {'order_book': order_book,
                                        'least_ask': least_ask,
                                     'greatest_bid': greatest_bid}
    #insert_snapshot_database(snap['lastUpdateId'],{'order_book': order_book,
    #                                                'least_ask': least_ask,
    #                                                'greatest_bid': greatest_bid})
    return snapshots[snap['lastUpdateId']]

#set up background process to ingest and persist snapshot every DELAY=30 secs
def snapshot_daemon(local_handler, t):
    update_snapshots()
    local_handler.enterabs(t+DELAY, 1, snapshot_daemon, (local_handler,t+DELAY))

#write to database
def create_snapshot_database():
    conn = sqlite3.connect(snaps_db)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS snaps (lastUpdateId int, order_book text);''')
    conn.commit()
    conn.close()

def insert_snapshot_database(id,book):
    conn = sqlite3.connect(snaps_db)
    c = conn.cursor()
    c.execute('''INSERT into snaps VALUES ({},'{}');'''.format(id,json.dumps(book)))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    #create_snapshot_database()
    sch = sched.scheduler(time.time, time.sleep)
    t = time.time()
    sch.enter(0, 1, snapshot_daemon, (sch, t))
    sch.run()
