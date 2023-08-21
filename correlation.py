import pandas as pd
import numpy as np
from py_vollib.black_scholes.implied_volatility import implied_volatility as iv
from datetime import datetime, timedelta

rawOptionsData = pd.concat([pd.read_parquet('options/harsh_000000000000.parquet'), pd.read_parquet('options/harsh_000000000001.parquet'), pd.read_parquet('options/harsh_000000000002.parquet'), pd.read_parquet(
    'options/harsh_000000000003.parquet'), pd.read_parquet('options/harsh_000000000004.parquet'), pd.read_parquet('options/harsh_000000000005.parquet'), pd.read_parquet('options/harsh_000000000006.parquet')])
rawOptionsData.to_csv('options/rawOptions.csv')
# BANK OF BARODA DATA MISSING
optionsData = {'AUBANK': rawOptionsData.query("Underlying == 'AUBANK'"), 'AXISBANK': rawOptionsData.query("Underlying == 'AXISBANK'"), 'BANDHANBNK': rawOptionsData.query("Underlying == 'BANDHANBNK'"), 'FEDERALBNK': rawOptionsData.query("Underlying == 'FEDERALBNK'"), 'HDFCBANK': rawOptionsData.query(
    "Underlying == 'HDFCBANK'"), 'ICICIBANK': rawOptionsData.query("Underlying == 'ICICIBANK'"), 'IDFCFIRSTB': rawOptionsData.query("Underlying == 'IDFCFIRSTB'"), 'INDUSINDBK': rawOptionsData.query("Underlying == 'INDUSINDBK'"), 'KOTAKBANK': rawOptionsData.query("Underlying == 'KOTAKBANK'"), 'PNB': rawOptionsData.query("Underlying == 'PNB'"), 'SBIN': rawOptionsData.query("Underlying == 'SBIN'")}
print(optionsData)
underlyingsData = {'AUBANK': pd.read_csv('underlyings/AUBANK.csv'), 'AXISBANK': pd.read_csv('underlyings/AXISBANK.csv'), 'BANDHANBNK': pd.read_csv('underlyings/BANDHANBNK.csv'), 'FEDERALBNK': pd.read_csv('underlyings/FEDERALBNK.csv'), 'HDFCBANK': pd.read_csv(
    'underlyings/HDFCBANK.csv'), 'ICICIBANK': pd.read_csv('underlyings/ICICIBANK.csv'), 'IDFCFIRSTB': pd.read_csv('underlyings/IDFCFIRSTB.csv'), 'INDUSINDBK': pd.read_csv('underlyings/INDUSINDBK.csv'), 'KOTAKBANK': pd.read_csv('underlyings/KOTAKBANK.csv'), 'PNB': pd.read_csv('underlyings/PNB.csv'), 'SBIN': pd.read_csv('underlyings/SBIN.csv')}
strikeWidths = {'AUBANK': 10, 'AXISBANK': 10, 'BANDHANBNK': 2.5, 'FEDERALBNK': 1, 'HDFCBANK': 10,
                'ICICIBANK': 10, 'IDFCFIRSTB': 1, 'INDUSINDBK': 10, 'KOTAKBANK': 20, 'PNB': 1, 'SBIN': 5}
for key, value in underlyingsData.items():
    df = pd.to_datetime(value['Date'], format='%Y-%m-%d %H:%M:%S+05:30')
    value['Date'] = df
    print(value)


def iv_with_exception_handling(price, S, K, t, r, flag):
    try:
        return iv(price, S, K, t, r, flag)
    except Exception as e:
        # print(e)
        return np.nan


def getIVData(df, multiplier, strikeWidth):
    # print(df)
    data = df[df['StrikePrice'].astype(float) == round(
        df['FuturePrice']*multiplier/strikeWidth, 0)*strikeWidth].copy()
    data['Date'] = pd.to_datetime(data['Date'], format="%Y-%m-%d %H:%M:%S")
    data['ExpiryDate'] = pd.to_datetime(
        data['ExpiryDate']) + timedelta(hours=15.5)
    data['tte'] = data.apply(lambda d:
                             np.busday_count(d.Date.date(), d.ExpiryDate.date(), holidays=['2023-05-01'])/250 +
                             (datetime.strptime(d.Date.date().strftime(format(
                                 '%m-%d-%Y')) + " 15:30:00", '%m-%d-%Y %H:%M:%S') - d.Date).total_seconds()/86400/250,
                             axis=1)
    # print(datetime.strptime(data['Date'].dt.date.strftime(format('%m-%d-%Y')) + " 15:30:00",'%m-%d-%Y %H:%M:%S') - data['Date'].dt.date).total_seconds()/86400
    # data['tte'] = ((data['ExpiryDate'] - data['Date']).dt.total_seconds()/86400 - data['holidays'])/250
    data.loc[data["OptionType"] == "CE", "OptionType"] = 'c'
    data.loc[data["OptionType"] == "PE", "OptionType"] = 'p'

    DateCount = data['Date'].dt.date.value_counts()
    DateCount = DateCount[DateCount > 600]
    data = data[data['Date'].dt.date.isin(DateCount.index)]
    print(data)
    data['IV'] = data.apply(lambda row: iv_with_exception_handling(
        float(row.OptionPrice), float(row.FuturePrice), float(row.StrikePrice), row.tte, 0.00, row.OptionType), axis=1)
    return data


Combined = {}
atmData = {}

for underlying in underlyingsData.keys():
    print(underlying)
    df = pd.merge(optionsData[underlying][['Date', 'LTP', 'StrikePrice', 'GlobalTicker', 'ExpiryDate']],
                  underlyingsData[underlying][['Date', 'close']], left_on='Date', right_on='Date', how='left')
    # print(df)
    df.columns = ['Date', 'OptionPrice',
                  'StrikePrice', 'OptionSymbol', 'ExpiryDate', 'FuturePrice']
    df['OptionType'] = df['OptionSymbol'].apply(
        lambda x: str(x[-2:]))
    Combined[underlying] = df
    # print(df)
    atmData[underlying] = getIVData(df, 1, strikeWidths[underlying])
    atmData[underlying].to_csv('atmData/'+str(underlying)+'.csv')

with pd.ExcelWriter('atmData/atmData.xlsx') as writer:
    for underlying in underlyingsData.keys():
        atmData[underlying].to_excel(writer, sheet_name=underlying)
print(Combined)
print(atmData)
