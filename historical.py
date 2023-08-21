from kiteconnect import KiteConnect
import pandas as pd
import numpy as np
import time

if __name__ == '__main__':

    key_secret = open("api_key.txt", 'r').read().split()
    kite = KiteConnect(
        api_key=key_secret[0], access_token="1aitLPnVMnsDNQ4Vqjva7ZOjR1iqOKq8")
    exc = ''
    instr = pd.read_csv('instruments.csv')
    for index, row in instr.iterrows():
        df = pd.DataFrame(kite.historical_data(
            row['instrument_token'], '2023-06-15 09:45:00', '2023-06-15 15:30:00', 'second'))
        df.to_csv(row['tradingsymbol']+'.csv')
        print(row['tradingsymbol'])
        time.sleep(0.2)