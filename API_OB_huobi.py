import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_OB_huobi.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Huobi_cnx = mysql.connector.connect(add your database info)
except:
    logging.error('not able to connect to database');

def fetch_orderbook_huobi():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://api.huobipro.com'
            endpoint = '/market/depth'
            # define the symbols
            symbols = ['btcusdt','ethusdt']
            for symbol in symbols:
                param = {'symbol':symbol,'type':'step1'}
                response = requests.get(url+endpoint, param)
                currenttime = time.time();
                inputdata = response.json()
                
                #process bids
                outputdata = {'time':currenttime, 'symbol':symbol.upper()}
                outputdata['data'] = str(inputdata)
                output.append(outputdata)
            
            #data_dict = {'exchange':'Huobi', 'item':'orderbook', 'data':output}
            #return json.dumps(data_dict)
            insert_db(Huobi_cnx, "ob", output)
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            time.sleep(20)
        except:
            continue


def main():
    fetch_orderbook_huobi()

if __name__ == "__main__":
    main()



