#!/bin/bash
# My first script

nohup python3 API_OB_Binance.py &
nohup python3 API_OB_Bitfinex.py &
nohup python3 API_OB_huobi.py &
nohup python3 API_OB_Kraken.py &
nohup python3 API_OB_Okex.py &
nohup python3 API_OB_Bitstamp.py &
nohup python3 API_TX_Binance.py &
nohup python3 API_TX_Bitfinex.py &
nohup python3 API_TX_huobi.py &
nohup python3 API_TX_Kraken.py &
nohup python3 API_TX_Okex.py &
nohup python3 API_TX_Bitstamp.py &

