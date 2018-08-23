import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_OB_Kraken.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Kraken_cnx = mysql.connector.connect(add your database info)
except:
    logging.error('not able to connect to database');

def fetch_orderbook_kraken():
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
                currenttime = time.time();
                inputdata = response.json()
                #process bids
                if(symbol == 'XBTUSD'):
                    outputdata = {'time':currenttime, 'symbol':'BTCUSD'}
                    outputdata['data'] = str(inputdata)
                elif(symbol == 'ETHUSD'):
                    outputdata = {'time':currenttime, 'symbol':'ETHUSD'}
                    outputdata['data'] = str(inputdata)
                output.append(outputdata)
            
            
            insert_db(Kraken_cnx, "ob", output)
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            time.sleep(20)
        except:
            continue



def main():
    fetch_orderbook_kraken()

if __name__ == "__main__":
    main()



