# %%
import requests
from io import BytesIO
import pandas as pd

import requests
import datetime
from tqdm import tqdm

import sys
sys.path.append(
    '/Users/hun/OneDrive - SNU/PersonalGit/KRX_financial_AI/Korea_data/krx_market')

tickers = pd.read_csv('krx_tickers.csv')
tickers_merged = pd.read_csv('krx_tickers_merged.csv')
tickers_merged['ticker'] = tickers_merged.ticker.apply(
    lambda x: '0'*(6-len(str(x)))+str(int(x)))


# %%


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
def KRX_price_otp(full_ticker, ticker, name, start):
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    headers = {"Origin": "http://data.krx.co.kr",
               "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020104", #daily에서 바꿔야하는거
               "User-Agent": "Safari/537.36"}
    data = {
        # "tboxisuCd_finder_stkisu0_1": f"{ticker}/{name}",
        "isuCd": f"{full_ticker}",
        "isuCd2": f"{ticker}",
        "param1isuCd_finder_stkisu0_1": "ALL",
        "strtYymm": start,  #daily에서 바꿔야하는거
        "endYymm": '202109', #daily에서 바꿔야하는거
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT01801" #daily주의 바꿔야하는부분
    }

    resp = requests.post(url, headers=headers, data=data)

    # if resp.json()['title'] == 'Access Denied':
    # raise Exception("otp problem, access denied")
    # else:
    try:
        return resp.text
    except:
        print(name, ticker, resp)


def KRX_price_get(full_ticker, ticker, name):
    start = "1997"
    # end = datetime.datetime.now().strftime('%Y%m%d')
    # end = "2021"

    otp = KRX_price_otp(full_ticker, ticker, name, start)
    url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    header = {"Connection": "keep-alive",
              "Host": "data.krx.co.kr",
              "Origin": "http://data.krx.co.kr",
              "User-Agent": "Mozilla/5.0",
              "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020104"} #daily부분 변경
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
                                '시가총액': 'mktcap', '상장주식수': "stock_avail", "최고가(종가)": 'high',
                                "최저가(종가)": "low", "거래량_합계": "total_volume", 
                                "거래대금_합계": "total_transactions", "거래량_일평균":"avg_volume",
                                "거래대금_일평균": "avg_transactions", "연월": "date"})
    cols = data.columns.to_list()
    cols = [cols[0]] + cols[-2:] + cols[1:-2]
    return data.reindex(columns=cols)


if __name__ == '__main__':
    # conn = hiroku()
    table = 'market_krx_daily'
    cols = 'date,ticker, name, open, high, low, close, volume, \
        daily_change, daily_return, transaction_amount, mktcap, stock_avail'

    cnt=0
    for i in tqdm(tickers.values):
        full_ticker, ticker, name = i[0], i[1], i[3]
        
        res = KRX_price_get(full_ticker, ticker, name)
        
        try:
            if cnt == 0:
                res.to_csv('./krx_market/krx_marketdata_monthly.csv', index=False, header = True)
                cnt = 1
            else:
                res.to_csv('./krx_market/krx_marketdata_monthly.csv', index=False, header =False, mode='a')
        except Exception as e:
            print(e)
            print(ticker, name)

# %%