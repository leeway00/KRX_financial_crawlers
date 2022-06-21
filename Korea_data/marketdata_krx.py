# %%
import requests
from io import BytesIO
import pandas as pd
import requests
import datetime
from tqdm import tqdm
import pickle as pkl

import sys
sys.path.append('/Users/hun/PersonalGit/KRX_marketdata')

ticker_path = './tickers/'
tickers = pd.read_csv(ticker_path + 'krx_tickers.csv')
tickers_merged = pd.read_csv(ticker_path + 'krx_tickers_merged.csv')

tickers_merged['ticker'] = tickers_merged.ticker.apply(
    lambda x: '0'*(6-len(str(x)))+str(int(x)))


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
                      'name_eng', 'listed_date', 'market', 'security_category',
                      'related_department', 'preferred', 'face_value', 'shares_available')
        return df
    else:
        print(f'Check conenction : {resp.status_code}')


# temp = KRX_tickers_get()
# temp.to_csv('krx_tickers.csv')

# tickers = pd.read_csv('./Korea_data/krx_tickers.csv')


def KRX_otp(full_ticker, ticker, name, start, end, type, transaction=False):
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"

    if type == 'daily':
        referer = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020103'
        data = {
            # "tboxisuCd_finder_stkisu0_0": f"{ticker}/{name}",
            "isuCd": f"{full_ticker}",
            "isuCd2": f"{full_ticker}",
            "codeNmisuCd_finder_stkisu0_0": f"{name}",
            "param1isuCd_finder_stkisu0_0": "ALL",
            "strtDd": f"{start}",
            "endDd": f"{end}",
            "share": '1',
            "money": '1',
            "csvxls_isNo": "false",
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT01701"
        }
        if transaction:
            data = {
                "inqTpCd": "2",
                "trdVolVal": "2",
                "askBid": "3",
                "isuCd": f"{full_ticker}",
                "isuCd2": f"{full_ticker}",
                "codeNmisuCd_finder_stkisu0_1": f"{name}",
                "param1isuCd_finder_stkisu0_1": "ALL",
                "strtDd": f"{start}",
                "endDd": f"{end}",
                "detailView": "1",
                "money": '1',
                "csvxls_isNo": "false",
                "name": "fileDown",
                "url": "dbms/MDC/STAT/standard/MDCSTAT02303"
            }
    else:
        referer = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020104'
        data = {
            # "tboxisuCd_finder_stkisu0_1": f"{ticker}/{name}",
            "isuCd": f"{full_ticker}",
            "isuCd2": f"{ticker}",
            "param1isuCd_finder_stkisu0_1": "ALL",
            "strtYymm": start,  # daily에서 바꿔야하는거
            "endYymm": end,  # daily에서 바꿔야하는거
            "csvxls_isNo": "false",
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT01801"  # daily주의 바꿔야하는부분
        }

    headers = {"Origin": "http://data.krx.co.kr",
               "Referer": referer,  # daily에서 바꿔야하는거
               "User-Agent": "Safari/537.36"}

    resp = requests.post(url, headers=headers, data=data)

    # if resp.json()['title'] == 'Access Denied':
    # raise Exception("otp problem, access denied")
    # else:
    try:
        return resp.text
    except:
        return (name, ticker, resp)


def KRX_price(full_ticker, ticker, name, start, end, type='daily'):
    otp = KRX_otp(full_ticker, ticker, name, start, end, type)
    url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    if type == 'daily':
        referer = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020103"
    else:
        referer = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020104"

    header = {"Connection": "keep-alive",
              "Host": "data.krx.co.kr",
              "Origin": "http://data.krx.co.kr",
              "User-Agent": "Mozilla/5.0",
              "Referer": referer}

    data = {'code': otp}
    resp = requests.post(url, headers=header, data=data)
    if resp.status_code == 200:
        df = pd.read_csv(BytesIO(resp.content), encoding='cp949')
        df['ticker'] = ticker
        df['name'] = name
        df = rename_KRX(df)
        return df
    else:
        print('failed connection :', resp.status_code)


def rename_KRX(data, type="daily"):
    data = data.rename(columns={'일자': 'date', '시가': 'open', '고가': 'high',
                                '저가': 'low', '종가': 'close', '거래량': 'volume',
                                '대비': "daily_change", '등락률': 'daily_return', '거래대금': 'transaction_amount',
                                '시가총액': 'mktcap', '상장주식수': "stock_avail", "최고가(종가)": 'highest_close',
                                "최저가(종가)": "lowest_close", "거래량_합계": "total_volume",
                                "거래대금_합계": "total_transactions", "거래량_일평균": "avg_volume",
                                "거래대금_일평균": "avg_transactions", "연월": "month",
                                "전체": "trans_balance"})
    cols = data.columns.to_list()
    cols = [cols[0]] + cols[-2:] + cols[1:-2]
    return data.reindex(columns=cols)



def KRX_transaction(full_ticker, ticker, name, start, end, type = 'daily'):
    otp = KRX_otp(full_ticker, ticker, name, start, end, type, transaction=True)
    url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    if type == 'daily':
        referer = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020103"
    else:
        referer = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020104"

    header = {"Connection": "keep-alive",
              "Host": "data.krx.co.kr",
              "Origin": "http://data.krx.co.kr",
              "User-Agent": "Mozilla/5.0",
              "Referer": referer}

    data = {'code': otp}
    resp = requests.post(url, headers=header, data=data)
    if resp.status_code == 200:
        df = pd.read_csv(BytesIO(resp.content), encoding='cp949')
        df['ticker'] = ticker
        df['name'] = name
        df = rename_KRX(df)
        df = df.replace([0], float('nan')).dropna(thresh= 11)
        return df.fillna(0)
    else:
        print('failed connection :', resp.status_code)
        
def cols(datatype):
    if datatype == 'daily_price':
        return ['date', 'ticker', 'name', 'close', 'daily_change', 'daily_return',
                'open', 'high', 'low', 'volume', 'transaction_amount', 'mktcap','stock_avail']
    elif datatype == 'trans':
        return ['date', 'ticker', 'name', '금융투자', '보험', '투신', '사모', '은행',
                '기타금융', '연기금 등', '기타법인', '개인', '외국인', '기타외국인', 'trans_balance']

if __name__ == '__main__':
    table = 'market_krx_daily'
    cols = 'date,ticker, name, open, high, low, close, volume, \
        daily_change, daily_return, transaction_amount, mktcap, stock_avail'

    tickers = KRX_tickers_get()
    stock_tickers = tickers[tickers.security_category == '주권']
    emergency = []
    
    
    start = '20100101'
    end = '20220620'
    cnt = False
    for i in tqdm(stock_tickers.values[:1]):
        full_ticker, ticker, name = i[0], i[1], i[3]

        res = KRX_price(full_ticker, ticker, name, start, end)
        trans = KRX_transaction(full_ticker, ticker, name, start, end)

        try:
            if not cnt:
                res.to_csv('./krx_market/krx_market_daily.csv',
                           index=False, header=True)
                trans.to_csv('./krx_market/krx_market_trans.csv',
                           index=False, header=True)
                cnt = True
            else:
                res.to_csv('./krx_market/krx_market_daily.csv',
                           index=False, header=False, mode='a')
                trans.to_csv('./krx_market/krx_market_daily.csv',
                           index=False, header=False, mode='a')
        except Exception as e:
            print(e)
            emergency.append((ticker, name))
    pkl.loads(emergency, open('./emergency.pkl', 'wb'))

# %%
