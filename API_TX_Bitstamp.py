import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_TX_Bitstamp.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Bitstamp_cnx = mysql.connector.connect(user='dataapplab', password = 'gaojizhongxue123',
                                   host='exchangedata.cluster-c4dhxbzodofv.us-west-2.rds.amazonaws.com',
                                   database='BitstampDB')
except:
    logging.error('not able to connect to database');

def fetch_trade_bitstamp():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://www.bitstamp.net'
            endpoint = '/api/v2/transactions/'
            # define the symbols
            symbols = ['btcusd','ethusd']
            for symbol in symbols:
                response = requests.get(url+endpoint+symbol)
                inputdata = response.json()
                for inputitem in inputdata:
                    outputitem = {}
                    outputitem['time'] = inputitem['date']
                    outputitem['price'] = inputitem['price']
                    outputitem['amount'] = inputitem['amount']
                    outputitem['symbol'] = symbol.upper().replace("_", "")
                    
                    if inputitem['type'] == '0':
                        outputitem['type'] = "buy"
                    else:
                        outputitem['type'] = "sell"

                    outputitem['txid'] = inputitem['tid']
                    output.append(outputitem)
            #data_dict = {'exchange':'Okex', 'item':'transaction', 'data':output}
            #print(json.dumps(data_dict) + "\n")
            insert_db(Bitstamp_cnx, "tx", output)
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
    fetch_trade_bitstamp()

if __name__ == "__main__":
    main()
