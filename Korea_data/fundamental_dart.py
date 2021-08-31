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


def find_value(col, args):
    return tickers[tickers[col].isin([args])]

# %%
# ### 1. XML파싱 이용해서 해결하기
# tick = requests.get("https://opendart.fss.or.kr/api/corpCode.xml", params = {'crtfc_key':key})
# with ZipFile(BytesIO(tick.content)) as zfile:
#     zfile.extractall()
# ###### 회사 이름으로 회사 고유번호 찾기
# tree = parse('CORPCODE.xml')
# root = tree.getroot()
# def find_corp_num(find_name):
#     for country in root.iter("list"):
#         if country.findtext("corp_name") == find_name:
            # return country.findtext("corp_code")

# ### 2. CORPCODE.xml pandas에 read_xml 사용해서 가져오기
# ccp = pd.read_xml('CORPCODE.xml').dropna().reset_index(drop = True)
# ccp['stock_code'] = ccp.stock_code.apply(
#     lambda x: '0'*(8-len(str(x)))+str(int(x)))
# ccp['corp_code'] = ccp.corp_code.apply(
#     lambda x: '0'*(8-len(str(x)))+str(int(x)))

# ### 3. 개별 회사정보 이용하기
# def getCompanyInfo(corp_code):
#     resp = requests.get("https://opendart.fss.or.kr/api/company.json",
#                         params={'crtfc_key': key, 'corp_code': corp_code})
#     if resp.status_code ==200:
#         res = resp.json()
#         if res['status'] =='000':
#             return [*res.values()]
# ###### 개별 회사정보 가져오고 저장하기
# col = getCompanyInfo(corp_codes.corp_code[3349]).index.to_list()
# results = []
# for i in tqdm(corp_codes.corp_code):
#     results.append(getCompanyInfo(i))
# codes2 = pd.DataFrame(results, columns = col)
# tem = pd.merge(corp_codes, codes2, how = 'left', on =['corp_code', 'stock_code'])
# tem.to_csv('corp_codes.csv', index = False)


# #%%
# tickers['corp_code'] = [ccp[ccp.stock_code == i].corp_code.values for i in tqdm(tickers.ticker)]

# corp_code_match = [(i, find_corp_num(i)) for i in tqdm(tickers.name_short)]
# corp_code_match = pd.Series(
#     [i[1] for i in corp_code_match], index=tickers.name_short)
# corp_codes = corp_code_match.dropna()



corp_codes = pd.read_csv('corp_codes.csv',  dtype = str)

# %%
url = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"

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

