import requests
import json
import time
import threading
import mysql.connector
import datetime

def insert_db(cnx, tbname, json_obj):
    
    hasDuplicate = 0
    
    cursor = cnx.cursor()

    if tbname == "tx":
        for item in json_obj:
            txId = item["txid"]
            time = item["time"]
            symbol = item["symbol"]
            price = item["price"]
            amount = item["amount"]
            type = item["type"]
            
            query = ("INSERT INTO Transaction(Txid,Time,Symbol,Price,Amount,Type)"+"VALUES(%s,%s,%s,%s,%s,%s);")
            data = (txId,time,symbol,price,amount,type)
            
            try:
                cursor.execute(query,data)
            except Exception as e:
                if('Duplicate entry' in str(e)):
                    hasDuplicate = 1
                #print("encounter an error, the error msg is the folloing:")
                #print(e)
                pass

    if tbname == "ob":
        for item in json_obj:
            time = item["time"]
            symbol = item["symbol"]
            json_data = json.dumps(item["data"])
            
            query = ("INSERT INTO Orderbook(Time,Symbol,Data)"+"VALUES(%s,%s,%s);")
            data = (time,symbol,json_data)
            
            try:
                cursor.execute(query,data)
            except Exception as e:
                if('Duplicate entry' in str(e)):
                    hasDuplicate = 1
                #print("encounter an error, the error msg is the folloing:")
                #print(e)
                pass

    cnx.commit()
    return hasDuplicate
