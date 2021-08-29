# %%
import pandas as pd
import requests
import datetime
import multiprocessing
from tqdm import tqdm
# import grequests

import sys
sys.path.append('..')

tickers = pd.read_csv(
    '/Users/hun/OneDrive - SNU/Quantry/quantryGit/market_data/Korea_data/krx_tickers.csv')

cols = ['ticker', 'name', 'timestamp', 'Recommend.All|1m', 'Recommend.MA|1m', 'Recommend.Other|1m', 'Recommend.All|5m', 'Recommend.MA|5m', 'Recommend.Other|5m',
        'Recommend.All|15m', 'Recommend.MA|15m', 'Recommend.Other|15m', 'Recommend.All|1h', 'Recommend.MA|1h', 'Recommend.Other|1h',
        'Recommend.All|4h', 'Recommend.MA|4h', 'Recommend.Other|4h', 'Recommend.All|1D', 'Recommend.MA|1D', 'Recommend.Other|1D',
        'Recommend.All|1W', 'Recommend.MA|1W', 'Recommend.Other|1W', 'Recommend.All|1M', 'Recommend.MA|1M', 'Recommend.Other|1M']

inds = ["Recommend.Other", f"Recommend.All", f"Recommend.MA", f"RSI", f"RSI[1]", f"Stoch.K",
        f"Stoch.D", f"Stoch.K[1]", f"Stoch.D[1]", f"CCI20", f"CCI20[1]",
        f"ADX", f"ADX+DI", f"ADX-DI", f"ADX+DI[1]", f"ADX-DI[1]", f"AO", f"AO[1]", f"AO[2]",
        f"Mom", f"Mom[1]", f"MACD.macd", f"MACD.signal", f"Rec.Stoch.RSI", f"Stoch.RSI.K",
        f"Rec.WR", f"W.R", f"Rec.BBPower", f"BBPower", f"Rec.UO", f"UO", f"EMA10", f"close",
        f"SMA10", f"EMA20", f"SMA20", f"EMA30", f"SMA30", f"EMA50", f"SMA50", f"EMA100", f"SMA100", f"EMA200", f"SMA200",
        f"Rec.Ichimoku", f"Ichimoku.BLine", f"Rec.VWMA", f"VWMA", f"Rec.HullMA9", f"HullMA9",
        f"Pivot.M.Classic.S3", f"Pivot.M.Classic.S2", f"Pivot.M.Classic.S1", f"Pivot.M.Classic.Middle",
        f"Pivot.M.Classic.R1", f"Pivot.M.Classic.R2", f"Pivot.M.Classic.R3", f"Pivot.M.Fibonacci.S3",
        f"Pivot.M.Fibonacci.S2", f"Pivot.M.Fibonacci.S1", f"Pivot.M.Fibonacci.Middle", f"Pivot.M.Fibonacci.R1",
        f"Pivot.M.Fibonacci.R2", f"Pivot.M.Fibonacci.R3", f"Pivot.M.Camarilla.S3", f"Pivot.M.Camarilla.S2",
        f"Pivot.M.Camarilla.S1", f"Pivot.M.Camarilla.Middle", f"Pivot.M.Camarilla.R1", f"Pivot.M.Camarilla.R2",
        f"Pivot.M.Camarilla.R3", f"Pivot.M.Woodie.S3", f"Pivot.M.Woodie.S2", f"Pivot.M.Woodie.S1",
        f"Pivot.M.Woodie.Middle", f"Pivot.M.Woodie.R1", f"Pivot.M.Woodie.R2", f"Pivot.M.Woodie.R3", f"Pivot.M.Demark.S1",
        f"Pivot.M.Demark.Middle", f"Pivot.M.Demark.R1"]


def getTradingView_all(ticker="005930"):
    # res_list = [ticker.split(':')[1]]
    url = f"https://scanner.tradingview.com/korea/scan"
    res_list = []
    target_list = ['1', '5', '15', '60', '240', '1D', '1W', '1M']
    for target in target_list:
        if target == '1D':
            data = {f"symbols": {f"tickers": [f"KRX:"+ticker], f"query": {f"types": []}},
                    f"columns": [f"Recommend.Other", f"Recommend.All", f"Recommend.MA", f"RSI", f"RSI[1]", f"Stoch.K", f"Stoch.D", f"Stoch.K[1]", f"Stoch.D[1]", f"CCI20", f"CCI20[1]",
                                 f"ADX", f"ADX+DI", f"ADX-DI", f"ADX+DI[1]", f"ADX-DI[1]", f"AO", f"AO[1]", f"AO[2]",
                                 f"Mom", f"Mom[1]", f"MACD.macd", f"MACD.signal", f"Rec.Stoch.RSI", f"Stoch.RSI.K",
                                 f"Rec.WR", f"W.R", f"Rec.BBPower", f"BBPower", f"Rec.UO", f"UO", f"EMA10", f"close",
                                 f"SMA10", f"EMA20", f"SMA20", f"EMA30", f"SMA30", f"EMA50", f"SMA50", f"EMA100", f"SMA100", f"EMA200", f"SMA200",
                                 f"Rec.Ichimoku", f"Ichimoku.BLine", f"Rec.VWMA", f"VWMA", f"Rec.HullMA9", f"HullMA9",
                                 f"Pivot.M.Classic.S3", f"Pivot.M.Classic.S2", f"Pivot.M.Classic.S1", f"Pivot.M.Classic.Middle",
                                 f"Pivot.M.Classic.R1", f"Pivot.M.Classic.R2", f"Pivot.M.Classic.R3", f"Pivot.M.Fibonacci.S3",
                                 f"Pivot.M.Fibonacci.S2", f"Pivot.M.Fibonacci.S1", f"Pivot.M.Fibonacci.Middle", f"Pivot.M.Fibonacci.R1",
                                 f"Pivot.M.Fibonacci.R2", f"Pivot.M.Fibonacci.R3", f"Pivot.M.Camarilla.S3", f"Pivot.M.Camarilla.S2",
                                 f"Pivot.M.Camarilla.S1", f"Pivot.M.Camarilla.Middle", f"Pivot.M.Camarilla.R1", f"Pivot.M.Camarilla.R2",
                                 f"Pivot.M.Camarilla.R3", f"Pivot.M.Woodie.S3", f"Pivot.M.Woodie.S2", f"Pivot.M.Woodie.S1",
                                 f"Pivot.M.Woodie.Middle", f"Pivot.M.Woodie.R1", f"Pivot.M.Woodie.R2", f"Pivot.M.Woodie.R3", f"Pivot.M.Demark.S1",
                                 f"Pivot.M.Demark.Middle", f"Pivot.M.Demark.R1"]}
        else:
            data = {f"symbols": {f"tickers": [f"KRX:"+ticker], f"queryf": {f"types": []}},
                    f"columns": [f"Recommend.Other|{target}", f"Recommend.All|{target}", f"Recommend.MA|{target}", f"RSI|{target}", f"RSI[1]|{target}", f"Stoch.K|{target}", f"Stoch.D|{target}", f"Stoch.K[1]|{target}", f"Stoch.D[1]|{target}", f"CCI20|{target}", f"CCI20[1]|{target}",
                                 f"ADX|{target}", f"ADX+DI|{target}", f"ADX-DI|{target}", f"ADX+DI[1]|{target}", f"ADX-DI[1]|{target}", f"AO|{target}", f"AO[1]|{target}", f"AO[2]|{target}",
                                 f"Mom|{target}", f"Mom[1]|{target}", f"MACD.macd|{target}", f"MACD.signal|{target}", f"Rec.Stoch.RSI|{target}", f"Stoch.RSI.K|{target}",
                                 f"Rec.WR|{target}", f"W.R|{target}", f"Rec.BBPower|{target}", f"BBPower|{target}", f"Rec.UO|{target}", f"UO|{target}", f"EMA10|{target}", f"close|{target}",
                                 f"SMA10|{target}", f"EMA20|{target}", f"SMA20|{target}", f"EMA30|{target}", f"SMA30|{target}", f"EMA50|{target}", f"SMA50|{target}", f"EMA100|{target}", f"SMA100|{target}", f"EMA200|{target}", f"SMA200|{target}",
                                 f"Rec.Ichimoku|{target}", f"Ichimoku.BLine|{target}", f"Rec.VWMA|{target}", f"VWMA|{target}", f"Rec.HullMA9|{target}", f"HullMA9|{target}",
                                 f"Pivot.M.Classic.S3|{target}", f"Pivot.M.Classic.S2|{target}", f"Pivot.M.Classic.S1|{target}", f"Pivot.M.Classic.Middle|{target}",
                                 f"Pivot.M.Classic.R1|{target}", f"Pivot.M.Classic.R2|{target}", f"Pivot.M.Classic.R3|{target}", f"Pivot.M.Fibonacci.S3|{target}",
                                 f"Pivot.M.Fibonacci.S2|{target}", f"Pivot.M.Fibonacci.S1|{target}", f"Pivot.M.Fibonacci.Middle|{target}", f"Pivot.M.Fibonacci.R1|{target}",
                                 f"Pivot.M.Fibonacci.R2|{target}", f"Pivot.M.Fibonacci.R3|{target}", f"Pivot.M.Camarilla.S3|{target}", f"Pivot.M.Camarilla.S2|{target}",
                                 f"Pivot.M.Camarilla.S1|{target}", f"Pivot.M.Camarilla.Middle|{target}", f"Pivot.M.Camarilla.R1|{target}", f"Pivot.M.Camarilla.R2|{target}",
                                 f"Pivot.M.Camarilla.R3|{target}", f"Pivot.M.Woodie.S3|{target}", f"Pivot.M.Woodie.S2|{target}", f"Pivot.M.Woodie.S1|{target}",
                                 f"Pivot.M.Woodie.Middle|{target}", f"Pivot.M.Woodie.R1|{target}", f"Pivot.M.Woodie.R2|{target}", f"Pivot.M.Woodie.R3|{target}", f"Pivot.M.Demark.S1|{target}",
                                 f"Pivot.M.Demark.Middle|{target}", f"Pivot.M.Demark.R1|{target}"]}
        res_list.append(requests.post(url, json=data).json()['data'][0]['d'])

    return pd.DataFrame(res_list, index=target_list, columns=inds)


def check(number):
    check_ticker, check_name = tickers.ticker[number], tickers.name[number]
    check_res = getTradingView_summary(check_ticker, check_name)
    return check_res


# def get_each(ticker, target):
#     url = "https://scanner.tradingview.com/korea/scan"
#     header = {"user-agent": "Mozilla/5.0"}
#     try:
#         if target == '1D':
#             data = {'symbols': {'tickers': ["KRX:"+ticker], 'query': {'types': []}},
#                     'columns': ['Recommend.All', 'Recommend.MA', 'Recommend.Other']}
#         else:
#             data = {'symbols': {'tickers': ["KRX:"+ticker], 'query': {'types': []}},
#                     'columns': [f'Recommend.All|{target}', f'Recommend.MA|{target}', f'Recommend.Other|{target}']}
#         resp = requests.post(url, headers=header, json=data).json()['data'][0]['d']
#         return pd.Series(resp, index = data['columns'])
#     except:
#         pass

def getTradingView_summary(ticker, name):

    url = "https://scanner.tradingview.com/korea/scan"
    header = {"user-agent": "Mozilla/5.0"}
    target_list = ['1', '5', '15', '60', '240', '1D', '1W', '1M']

    try:
        res_list = [ticker, name, datetime.datetime.now()]
        for target in target_list:
            if target == '1D':
                data = {'symbols': {'tickers': ["KRX:"+ticker], 'query': {'types': []}},
                        'columns': ['Recommend.All', 'Recommend.MA', 'Recommend.Other']}
            else:
                data = {'symbols': {'tickers': ["KRX:"+ticker], 'query': {'types': []}},
                        'columns': [f'Recommend.All|{target}', f'Recommend.MA|{target}', f'Recommend.Other|{target}']}
            resp = requests.post(url, headers=header, json=data)
            res_list += resp.json()['data'][0]['d']
        return pd.Series(res_list, index=cols)
    except Exception as e:
        print(e)
        print(ticker, name)
        # print(resp.json())

# %%

def exception_handler(requests, exception):
    print(f'Failed with {exception}')


def getTV_async(ticker, name):

    url = "https://scanner.tradingview.com/korea/scan"
    header = {"user-agent": "Safari/537.36"}
    target_list = ['1', '5', '15', '60', '240', '1D', '1W', '1M']
    res_list = [ticker, name, datetime.datetime.now()]

    def gen_data(target):
        if target == '1D':
            data = {'symbols': {'tickers': ["KRX:"+ticker], 'query': {'types': []}},
                    'columns': ['Recommend.All', 'Recommend.MA', 'Recommend.Other']}
        else:
            data = {'symbols': {'tickers': ["KRX:"+ticker], 'query': {'types': []}},
                    'columns': [f'Recommend.All|{target}', f'Recommend.MA|{target}', f'Recommend.Other|{target}']}
        return data

    try:
        rs = (grequests.post(url, headers=header, json=gen_data(target))
              for target in target_list)
        res = grequests.map(rs)
        time.sleep(2)
        
        temp = [resp.json()['data'][0]['d'] for resp in res]
        res_list += temp
        return pd.Series(res_list, index=cols)
    except:
        print(ticker, name, res)


# %%
# 1분,4분,15분,1시간,4시간,1주, 1달
# target = ['1', '5', '15', '60', '240', '1D', '1W', '1M']
# data = {f"symbolsf": {f"tickersf": [f"NASDAQ:AAPLf"], f"queryf": {f"typesf": []}}, f"columnsf": [ff"Recommend.Other|{target}", ff"Recommend.All|{target}", ff"Recommend.MA|{target}"]}
# 1일은 이거 사용해야함.
# data = {f"symbolsf":{f"tickersf":[f"NASDAQ:AAPLf"],f"queryf":{f"typesf":[]}},f"columnsf":[f"Recommend.Otherf",f"Recommend.Allf",f"Recommend.MAf"]}
# 이건 전체 데이터 가져올때

# 결과값 형태
# {‘data’: [{‘s’: ‘NASDAQ: AAPL’, ‘d’: [0.18181818, 0.55757576, 0.93333333]}], ‘totalCount’: 1}
# 숫자 3개 순서대로 recommend.other, recommend.all, recommend.MA인데,
# other: oscillator
# all: summary
# ma: moving average
# 대충 관찰해 본 결과
# neutral: -0.2~0.2
# buy: 0.2~0.6 or 0.7
# strong buy: 0.7~ 이랬음
# + 중간에 recommend all은 other, ma를 정확히 평균낸거임.

# #%%
# # res = [getTradingView_summary(ticker, name) for ticker, name \
#     # in tqdm(zip(tickers.ticker, tickers.name))]

# res = []
# for ticker, name in tqdm(zip(tickers.ticker, tickers.name)):
#     try:
#         temp = getTradingView_summary(ticker, name)
#         res.append(temp)
#     except:
#         pass
# res = pd.Dataframe(res)

# #%%
# # res = []
# proc = []
# with multiprocessing.Pool(processes=2) as pool:
#     for ticker, name in tqdm(zip(tickers.ticker[:10], tickers.name[:10])):
#         temp = pool.apply_async(func = getTradingView_summary,
#                                args = (ticker, name))
#         proc.append(temp)
#     res = [i.get() for i in proc]
#     # pool.map()

# %%
# data = [i for i in zip(tickers.ticker[:10], tickers.name[:10])]
# results = multiprocessing.Pool(2).map(ttemp, data)


if __name__ == "__main__":
    import time

    tickers = tickers
    start = time.time()
    pool = multiprocessing.Pool(5)
    results = pool.starmap(getTradingView_summary,
                           zip(tickers.ticker, tickers.name))
    # results = pool.starmap(getTV_async,
    #                        zip(tickers.ticker, tickers.name))
    pool.close()
    pool.join()

    print(f"time : {time.time() - start}")

    res = pd.DataFrame([value for value in results if value is not None])
    today = datetime.date.today().strftime('%m%d')
    res.to_csv(f'/krx_data/tview{today}_v2.csv')
    res.timestamp = today
    res.to_csv(f'/krx_data/tview{today}.csv')