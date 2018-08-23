import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_TX_Okex.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Okex_cnx = mysql.connector.connect()
except:
    logging.error('not able to connect to database');


def fetch_trade_okex():
    logging.info("Enter the program at " + str(logtime));
    while True:
        try:
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://www.okex.com'
            endpoint = '/api/v1/trades.do'
            # define the symbols
            symbols = ['btc_usdt','eth_usdt']
            for symbol in symbols:
                param = {'symbol':symbol}
                response = requests.get(url+endpoint, param)
                inputdata = response.json()
                for inputitem in inputdata[:200]:
                    outputitem = {}
                    outputitem['time'] = inputitem['date_ms']
                    outputitem['price'] = inputitem['price']
                    outputitem['amount'] = inputitem['amount']
                    outputitem['symbol'] = symbol.upper().replace("_", "")
                    outputitem['type'] = inputitem['type']
                    outputitem['txid'] = inputitem['tid']
                    output.append(outputitem)
            
            #data_dict = {'exchange':'Okex', 'item':'transaction', 'data':output}
            #print(json.dumps(data_dict) + "\n")
            hasDuplicate = insert_db(Okex_cnx, "tx", output)
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
    fetch_trade_okex()

if __name__ == "__main__":
    main()



