import requests
import json
import time
import threading
import mysql.connector
import datetime
import logging
from database import *

logging.basicConfig(filename='./log/API_OB_Okex.log',level=logging.INFO)
logtime =  datetime.datetime.now()
logging.info("Starting the program at " + str(logtime));

try:
    Okex_cnx = mysql.connector.connect(user='dataapplab', password = 'gaojizhongxue123',
                                     host='exchangedata.cluster-c4dhxbzodofv.us-west-2.rds.amazonaws.com',
                                     database='OkexDB')
except:
    logging.error('not able to connect to database');

def fetch_orderbook_okex():
    while True:
        try:
            logging.info("Enter the program at " + str(logtime));
            startingTime = datetime.datetime.now()
            logging.info("fetch starting at: " +str(startingTime));
            output = []
            # get price data
            url = 'https://www.okex.com'
            endpoint = '/api/v1/depth.do'
            # define the symbols
            symbols = ['btc_usdt','eth_usdt']
            output = []
            for symbol in symbols:
                param = {'symbol':symbol, 'size':20}
                response = requests.get(url+endpoint, param)
                currenttime = time.time();
                inputdata = response.json()
                #process bids
                outputdata = {'time':currenttime, 'symbol':symbol.upper().replace("_", "")}
                outputdata['data'] = str(inputdata)
                output.append(outputdata)
            
            #data_dict = {'exchange':'Okex', 'item':'orderbook', 'data':output}
            #print(json.dumps(data_dict))
            insert_db(Okex_cnx, "ob", output)
            finishingTime = datetime.datetime.now()
            logging.info("fetch finishing at: " +str(finishingTime));
            difference = (finishingTime - startingTime).total_seconds()
            logging.info("the total runing time is " + str(difference) + " second");
            time.sleep(20)
        except:
            continue



def main():
    fetch_orderbook_okex()

if __name__ == "__main__":
    main()



