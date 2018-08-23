import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_OB_Binance.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Binance_cnx = mysql.connector.connect(add your database info)
except:
    logging.error('not able to connect to database');


def fetch_orderbook_binance():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://api.binance.com'
            endpoint = '/api/v1/depth'
            # define the symbols
            symbols = ['BTCUSDT','ETHUSDT']
            for symbol in symbols:
                param = {'symbol':symbol, 'limit':20}
                response = requests.get(url+endpoint, param)
                currenttime = time.time();
                inputdata = response.json()
                outputdata = {'time':currenttime, 'symbol':symbol}
                outputdata['data'] = str(inputdata)
                output.append(outputdata)
            insert_db(Binance_cnx, "ob", output)
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            time.sleep(20)
        except:
            continue


def main():
    fetch_orderbook_binance()

if __name__ == "__main__":
    main()



