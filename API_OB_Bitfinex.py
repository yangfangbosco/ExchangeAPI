import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_OB_Bitfinex.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Bitfinex_cnx = mysql.connector.connect(add your database info)
except:
    logging.error('not able to connect to database');


def fetch_orderbook_bitfinex():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            symbol = ['BTCUSD', 'ETHUSD']
            transaction = []
            
            for sym in symbol:
                api = "https://api.bitfinex.com/v2/book/t" + sym + "/P0"
                r = requests.get(api)
                data = r.json()
                orderbook_data = []
                
                for item in data:
                    orderbook_data.append(item)
                json_str = json.dumps(orderbook_data)
                test_dict = {'time':time.time(), 'symbol':sym, 'data':json_str}
                transaction.append(test_dict)
            
            #data_dict = {'exchange':'Bitfinex', 'item':'orderbook', 'data':transaction}
            #return json.dumps(data_dict)
            insert_db(Bitfinex_cnx, "ob", transaction)
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            time.sleep(20)
        except:
            continue


def main():
    fetch_orderbook_bitfinex()

if __name__ == "__main__":
    main()



