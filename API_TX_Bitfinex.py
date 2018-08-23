import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_TX_Bitfinex.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Bitfinex_cnx = mysql.connector.connect(add your database info)
except:
    logging.error('not able to connect to database');

def fetch_trade_bitfinex():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            symbol = ['BTCUSD', 'ETHUSD']
            transaction = []
            
            for sym in symbol:
                api = "https://api.bitfinex.com/v2/trades/t" + sym + "/hist?limit=20"
                r = requests.get(api)
                data = r.json()
                for item in data:
                    if item[2] < 0:
                        type = "sell"
                    else:
                        type = "buy"
                    test_dict = {'time':item[1], 'symbol':sym, 'price':item[3], 'amount':abs(item[2]), 'type':type,'txid':item[0]}
                    transaction.append(test_dict)
            
            #data_dict = {'exchange':'Bitfinex', 'item':'transaction', 'data':transaction}
            #print(json.dumps(data_dict) + "\n")
           
            hasDuplicate = insert_db(Bitfinex_cnx, "tx", transaction)
            if(hasDuplicate == 1):
                logging.info("no missing data")
            else:
                logging.warning("transaction missing")
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            remainWaitTime =20-float(str(difference))
            if(remainWaitTime > 0):
                time.sleep(remainWaitTime)
        except:
            continue



def main():
    fetch_trade_bitfinex()

if __name__ == "__main__":
    main()



