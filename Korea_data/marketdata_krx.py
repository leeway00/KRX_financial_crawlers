# %%
import requests
from io import BytesIO
import pandas as pd

import requests
import datetime
from tqdm import tqdm

import sys
sys.path.append('/Users/hun/OneDrive - SNU/Quantry/quantryGit/market_data')
print(sys.path)

from hiroku import hiroku


def KRX_tickers_otp():
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    headers = {"Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010101",
               "User-Agent": "Mozilla/5.0"}
    data = {"mktId": "ALL",
            "share": "1",
            "csvxls_isNo": "false",
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT01901"}
    resp = requests.post(url, headers=headers, data=data)
    return resp.text


def KRX_tickers_get():
    otp = KRX_tickers_otp()
    url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    header = {"Cache-Control": "max-age=0",
              "Connection": "keep-alive",
              "Host": "data.krx.co.kr",
              "Origin": "http://data.krx.co.kr",
              "User-Agent": "Mozilla/5.0"}
    data = {'code': otp}
    resp = requests.post(url, headers=header, data=data)
    if resp.status_code == 200:
        df = pd.read_csv(BytesIO(resp.content), encoding='cp949')
        df.columns = ('full_ticker', 'ticker', 'name', 'name_short',
                      'name_eng', 'listed_data', 'market', 'security_category',
                      'related_department', 'preferred', 'face_value', 'shares_available')
        return df
    else:
        print(f'Check conenction : {resp.status_code}')


# %%
# temp = KRX_tickers_get()
# temp.to_csv('krx_tickers.csv')

# %%
tickers = pd.read_csv('krx_tickers.csv')

#%%
def KRX_price_otp(full_ticker, start, end):
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    headers = {"Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
               "User-Agent": "Safari/537.36"}
    data = {#"tboxisuCd_finder_stkisu0_1": "005930/삼성전자", 
            "isuCd": f"{full_ticker}",
            # "isuCd2": "KR7005930003",
            # "codeNmisuCd_finder_stkisu0_1": "삼성전자",
            "param1isuCd_finder_stkisu0_1": "ALL",
            "strtDd": start,
            "endDd": end,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT01701"}
    resp = requests.post(url, headers=headers, data=data)
    return resp.text

def KRX_price_get(full_ticker, ticker, name):
    start = "20180101"
    end = datetime.datetime.now().strftime('%Y%m%d')
    
    otp = KRX_price_otp(full_ticker, start, end)
    url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    header = {"Cache-Control": "max-age=0",
              "Connection": "keep-alive",
              "Host": "data.krx.co.kr",
              "Origin": "http://data.krx.co.kr",
              "User-Agent": "Mozilla/5.0"}
    data = {'code': otp}
    resp = requests.post(url, headers=header, data=data)
    if resp.status_code == 200:
        df = pd.read_csv(BytesIO(resp.content), encoding='cp949')
        df['ticker'] = ticker
        df['name'] = name
        df = rename_KRX(df)
        return df

def rename_KRX(data):
    data = data.rename(columns={'일자': 'date', '시가': 'open', '고가': 'high',
                         '저가': 'low', '종가': 'close', '거래량': 'volume',
                         '대비': "daily_change", '등락률': 'daily_return', '거래대금': 'transaction_amount',
                         '시가총액': 'mktcap','상장주식수': "stock_avail"})
    cols = data.columns.to_list()
    cols = [cols[0]]+ cols[-2:] + cols[1:-2]
    return data.reindex(columns = cols)
    


# %%
if __name__=='__main__':
    conn = hiroku()
    table = 'market_krx_daily'
    cols = 'date,ticker, name, open, high, low, close, volume, \
        daily_change, daily_return, transaction_amount, mktcap, stock_avail'
    
    for i in tqdm(tickers.values):
        full_ticker, ticker, name = i[1], i[2], i[3]
        # if len(res) ==0 :
        #     res = KRX_price_get(full_ticker, ticker, name)
        # else:
        #     res.append(KRX_price_get(full_ticker, ticker, name))
        res = KRX_price_get(full_ticker, ticker, name)
        # conn.insert(table, res, cols)
        res.to_csv('krx_marketdata.csv', index = False, mode = 'a')
# %%
