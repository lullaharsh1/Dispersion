import pandas as pd
import numpy as np
import glob 
import os
from datetime import datetime, timedelta, time
import py_vollib.black.implied_volatility
from py_vollib.black_scholes.implied_volatility import implied_volatility as iv
import py_vollib_vectorized
import matplotlib as pyplot


def iv_with_exception_handling(price, S, K, t, r, flag):
    try:
        return iv(price, S, K, t, r, flag).IV
    except Exception as e:
        return np.nan
    
def getIVData(df, multiplier):
    data = df[df['StrikePrice'] == round(df['FuturePrice']*multiplier/100,0)*100].copy()
    data['Date'] = pd.to_datetime(data['Date'], format="%Y-%m-%d %H:%M:%S UTC")
    data['ExpiryDate'] = pd.to_datetime(data['ExpiryDate']) + timedelta(hours = 15.5)
    data['tte'] = data.apply(lambda d: 
            np.busday_count(d.Date.date(), d.ExpiryDate.date(), holidays=['2023-05-01'])/250 + 
            (datetime.strptime(d.Date.date().strftime(format('%m-%d-%Y')) + " 15:30:00",'%m-%d-%Y %H:%M:%S') - d.Date).total_seconds()/86400/250,
    axis = 1) 
    #print(datetime.strptime(data['Date'].dt.date.strftime(format('%m-%d-%Y')) + " 15:30:00",'%m-%d-%Y %H:%M:%S') - data['Date'].dt.date).total_seconds()/86400
    #data['tte'] = ((data['ExpiryDate'] - data['Date']).dt.total_seconds()/86400 - data['holidays'])/250
    data.loc[data["OptionType"] == "CE", "OptionType"] = 'c'
    data.loc[data["OptionType"] == "PE", "OptionType"] = 'p'
    print(data)
    DateCount = data['Date'].dt.date.value_counts() 
    DateCount = DateCount[DateCount > 600]
    data = data[data['Date'].dt.date.isin(DateCount.index)]

    data['IV'] = data.apply(lambda row: iv_with_exception_handling(row.OptionPrice, row.FuturePrice, row.StrikePrice, row.tte, 0.00, row.OptionType), axis=1)
    return data

files = os.path.join("options.parquet")
files = glob.glob(files)
NewData_Opt = pd.concat(map(pd.read_parquet, files))
NewData_Fut = pd.read_parquet("fut.parquet")
Combined = pd.merge(NewData_Opt[['close','strike','instrument_name']],NewData_Fut['close'],on = 'date', how= 'left').sort_index().reset_index()
Combined.columns = ['Date', 'OptionPrice', 'StrikePrice','OptionSymbol','FuturePrice']
Combined['OptionType'] = Combined['OptionSymbol'].apply(lambda x : str(x[-2:]))
Combined['ExpiryDate'] = '2023-04-27'
Combined.loc[Combined['Date'] >= '2023-04-20' , 'ExpiryDate'] = '2023-05-25'
Rawdata = Combined
IndexRawdata = Rawdata.pivot_table(index = ['Date'], columns= ['OptionType','StrikePrice'], values='OptionPrice').ffill(limit = 30).unstack()

atmdata = getIVData(Rawdata,1)

#IndexRawdata.iloc[IndexRawdata.index.get_loc((optionType,buildingStrike1,current))+impact]