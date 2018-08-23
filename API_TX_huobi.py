import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_TX_huobi.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Huobi_cnx = mysql.connector.connect()
except:
    logging.error('not able to connect to database');

def fetch_trade_huobi():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://api.huobipro.com'
            endpoint = '/market/history/trade'
            # define the symbols
            symbols = ['btcusdt','ethusdt']
            for symbol in symbols:
                param = {'symbol':symbol,'size':200}
                response = requests.get(url+endpoint, param)
                inputdata = response.json()
                for inputitem in inputdata['data']:
                    outputitem = {}
                    outputitem['time'] = inputitem['data'][0]['ts']
                    outputitem['price'] = inputitem['data'][0]['price']
                    outputitem['amount'] = inputitem['data'][0]['amount']
                    outputitem['symbol'] = symbol.upper()
                    outputitem['type'] = inputitem['data'][0]['direction']
                    outputitem['txid'] = inputitem['data'][0]['id']
                    output.append(outputitem)
            
            #data_dict = {'exchange':'Huobi', 'item':'transaction', 'data':output}
            #print(json.dumps(output) + "\n")
            hasDuplicate = insert_db(Huobi_cnx, "tx", output)
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
    fetch_trade_huobi()

if __name__ == "__main__":
    main()



