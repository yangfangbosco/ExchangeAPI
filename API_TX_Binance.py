import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_TX_Binance.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Binance_cnx = mysql.connector.connect(user='dataapplab', password = 'gaojizhongxue123',
                                      host='exchangedata.cluster-c4dhxbzodofv.us-west-2.rds.amazonaws.com',
                                      database='BinanceDB')
except:
    logging.error('not able to connect to database');

def fetch_trade_binance():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://api.binance.com'
            endpoint = '/api/v1/trades'
            # define the symbols
            symbols = ['BTCUSDT','ETHUSDT']
            for symbol in symbols:
                param = {'symbol':symbol, 'limit':200}
                response = requests.get(url+endpoint, param)
                inputdata = response.json()
                for inputitem in inputdata:
                    outputitem = {}
                    outputitem['time'] = inputitem['time']
                    outputitem['price'] = inputitem['price']
                    outputitem['amount'] = inputitem['qty']
                    outputitem['symbol'] = symbol
                    outputitem['txid'] = inputitem['id']
                    if(inputitem['isBuyerMaker'] == False):
                        outputitem['type'] = 'sell'
                    else:
                        outputitem['type'] = 'buy'
                    output.append(outputitem)
            hasDuplicate = insert_db(Binance_cnx, "tx", output)
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
    fetch_trade_binance()

if __name__ == "__main__":
    main()



