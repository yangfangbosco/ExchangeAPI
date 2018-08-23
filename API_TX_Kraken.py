import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_TX_Kraken.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Kraken_cnx = mysql.connector.connect()
except:
    logging.error('not able to connect to database');

def fetch_trade_kraken():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://api.kraken.com'
            endpoint = '/0/public/Trades'
            # define the symbols
            symbols = ['XBTUSD','ETHUSD']
            for symbol in symbols:
                param = {'pair':symbol}
                response = requests.get(url+endpoint, param)
                inputdata = response.json()
                if(symbol == 'XBTUSD'):
                    for inputitem in inputdata['result']['XXBTZUSD'][800:]:
                        outputitem = {}
                        outputitem['txid'] = 0
                        outputitem['price'] = inputitem[0]
                        outputitem['amount'] = inputitem[1]
                        outputitem['time'] = inputitem[2]
                        if(inputitem[3] == 'b'):
                            outputitem['type'] = 'buy'
                        else:
                            outputitem['type'] = 'sell'
                        outputitem['symbol'] = symbol
                        output.append(outputitem)
                if(symbol == 'ETHUSD'):
                    for inputitem in inputdata['result']['XETHZUSD'][800:]:
                        outputitem['txid'] = 0
                        outputitem['price'] = inputitem[0]
                        outputitem['amount'] = inputitem[1]
                        outputitem['time'] = inputitem[2]
                        if(inputitem[3] == 'b'):
                            outputitem['type'] = 'buy'
                        else:
                            outputitem['type'] = 'sell'
                        outputitem['symbol'] = symbol
                        output.append(outputitem)


            hasDuplicate = insert_db(Kraken_cnx, "tx", output)
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
    fetch_trade_kraken()

if __name__ == "__main__":
    main()
