import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_OB_Bitstamp.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Bitstamp_cnx = mysql.connector.connect()
except:
    logging.error('not able to connect to database');

def fetch_orderbook_bitstamp():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://www.bitstamp.net'
            endpoint = '/api/v2/order_book/'
            # define the symbols
            symbols = ['btcusd','ethusd']
            output = []
            for symbol in symbols:
                response = requests.get(url+endpoint+symbol)
                inputdata = response.json()
                #process bids
                temp_dict = {'bids':inputdata['bids'][:20], 'asks':inputdata['asks'][:20]}
                outputdata = {'time':inputdata['timestamp'], 'symbol':symbol.upper().replace("_", ""), 'data':temp_dict}
                output.append(outputdata)
            
            #data_dict = {'exchange':'Okex', 'item':'orderbook', 'data':output}
            #print(json.dumps(data_dict))
            insert_db(Bitstamp_cnx, "ob", output)
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            time.sleep(20)
        except:
            continue




def main():
    fetch_orderbook_bitstamp()

if __name__ == "__main__":
    main()



