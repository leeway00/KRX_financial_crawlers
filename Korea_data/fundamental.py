# %%
from datetime import date
from unicodedata import name
from numpy.testing._private.utils import tempdir
import requests
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from xml.etree.ElementTree import parse
from tqdm import tqdm
# import dbconfig
# from hiroku import hiroku


tickers = pd.read_csv('krx_tickers.csv')
key = "126900466a1ceac2a36e14323c19781ddb66ce68"

# %%
# XML파싱 이용해서 해결하기
# tick = requests.get("https://opendart.fss.or.kr/api/corpCode.xml", params = {'crtfc_key':key})
# with ZipFile(BytesIO(tick.content)) as zfile:
#     zfile.extractall()
# 회사 이름으로 회사 고유번호 찾기
# tree = parse('CORPCODE.xml')
# root = tree.getroot()
# def find_corp_num(find_name):
#     for country in root.iter("list"):
#         if country.findtext("corp_name") == find_name:
#             return country.findtext("corp_code")

# pandas에 read_xml 사용해서 바로
# ccp = pd.read_xml('CORPCODE.xml').dropna().reset_index(drop = True)
# ccp.to_csv('corp_cdoes.csv')

ccp['stock_code'] = ccp.stock_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))

# %%


def getCompanyInfo(corp_code):
    resp = requests.get("https://opendart.fss.or.kr/api/company.json",
                        params={'crtfc_key': key, 'corp_code': corp_code})
    if resp.status_code ==200:
        if resp['status'] =='000':
            pass
# %%


def find_value(col, args):
    return tickers[tickers[col].isin([args])]

#%%
tickers['corp_code'] = [ccp[ccp.stock_code == i].corp_code.values for i in tqdm(tickers.ticker)]


# %%
corp_code_match = [(i, find_corp_num(i)) for i in tqdm(tickers.name_short)]
corp_code_match = pd.Series(
    [i[1] for i in corp_code_match], index=tickers.name_short)
corp_codes = corp_code_match.dropna()

# %%
corp_codes.to_csv('corp_codes0827.csv', index=True)

# %%

url = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"

# %%


def get_report(corp_code, year, quarter):
    if quarter == 1:
        code = '11013'
    elif quarter == 2:
        code = '11012'
    elif quarter == 3:
        code = '11014'
    elif quarter == 4:
        code = "11011"

    params = {
        'crtfc_key': key,
        'corp_code': corp_code,
        'bsns_year': year,
        'reprt_code': code
    }

    resp = requests.get(url, params=params).json()
    if resp['status'] == '000':
        return resp['list']
    else:
        raise Exception(resp['status'], resp['message'])


# %%
res = get_report(corp_codes['삼성전자'], 2020, 3)
res = pd.DataFrame(res)
# %%
res
# %%


def earning(data):
    for i in data.values:
        pass


# %%
def get_NAVERfin(ticker, name):
    URL = "https://finance.naver.com/item/main.nhn?code=" + ticker

    company = requests.get(URL)
    df = pd.read_html(company.text)[3]
    try:
        df.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
        df.index.rename('', inplace=True)
        df.columns = df.columns.droplevel(2)

        annual_finance = pd.DataFrame(df).xs('최근 연간 실적', axis=1)
        quarter_finance = pd.DataFrame(df).xs('최근 분기 실적', axis=1)

        def expectation(data):
            if len(data['date']) > 7:
                data['E'] = 1
                data['date'] = data['date'][:-3]
            else:
                data['E'] = 0
            return data

        def adjustment(data):
            data = data.transpose().reset_index().rename(
                columns={'index': "date"})
            data['ticker'] = ticker
            data['name'] = name

            data = data.apply(lambda x: expectation(x), axis=1)

            cols = data.columns.to_list()
            data = data[cols[-3:] + cols[:-3]]
            return data

        return adjustment(annual_finance), adjustment(quarter_finance)
    except:
        print(name)
        pass

# #%%
# cnt=0
# for i in tqdm(tickers.values):
#     ticker, name = i[2], i[3]
#     try:
#         annual, quarter = get_NAVERfin(ticker, name)
#         if cnt == 0:
#             header = True
#             cnt +=1
#         else:
#             header = False
#         annual.to_csv('./corp_num/annual_fin.csv', mode = 'a', index= False, header = header)
#         quarter.to_csv('./corp_num/quarter_fin.csv', mode = 'a', index= False, header = header)
#     except:
#         pass
