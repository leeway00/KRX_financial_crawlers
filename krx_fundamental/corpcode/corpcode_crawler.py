# %%
import datetime
from unicodedata import name
from numpy.testing._private.utils import tempdir
import requests
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from xml.etree.ElementTree import parse
from tqdm import tqdm


## Personal Opendart key
key = "126900466a1ceac2a36e14323c19781ddb66ce68"

#%%
### krx_tickers_merged is a csv file including corporate code of CORPCODE.xml
### 0. If krx_tickers_merged is not available, better to generate corpcode through 1~3 steps below.
tickers = pd.read_csv('krx_tickers_merged.csv')
tickers['corp_code'] = tickers.corp_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))
tickers['ticker'] = tickers.ticker.apply(
    lambda x: '0'*(6-len(str(x)))+str(int(x)))


#%%
### 1. 기업 고유번호 가져오기 
tick = requests.get("https://opendart.fss.or.kr/api/corpCode.xml", params = {'crtfc_key':key})
with ZipFile(BytesIO(tick.content)) as zfile:
    zfile.extractall()


### 2-1. XML 파싱 이요해서 가져오고, 회사 이름으로 찾기
tree = parse('CORPCODE.xml')
root = tree.getroot()
def find_corp_num(find_name):
    for country in root.iter("list"):
        if country.findtext("corp_name") == find_name:
            return country.findtext("corp_code")

### 2-2. CORPCODE.xml pandas에 read_xml 사용해서 가져오기
ccp = pd.read_xml('CORPCODE.xml').dropna().reset_index(drop = True)
ccp['stock_code'] = ccp.stock_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))
ccp['corp_code'] = ccp.corp_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))


### 3. 개별 회사정보 이용하기
def getCompanyInfo(corp_code, index = False):
    resp = requests.get("https://opendart.fss.or.kr/api/company.json",
                        params={'crtfc_key': key, 'corp_code': corp_code})
    if resp.status_code ==200:
        res = resp.json()
        print(res)
        if res['status'] =='000':
            if index == True:
                return [*res.keys()][2:]
            else:
                return [*res.values()][2:]

#%%
###### 개별 회사정보 가져오고 저장하기
col = getCompanyInfo(ccp.corp_code[0], index = True)
results = []
for i in tqdm(ccp.corp_code):
    results.append(getCompanyInfo(i))
codes2 = pd.DataFrame(results, columns = col)
tem = pd.merge(ccp, codes2, how = 'left', on =['corp_code', 'stock_code'])
# tem.to_csv('corp_codes.csv', index = False)


tickers['corp_code'] = [ccp[ccp.stock_code == i].corp_code.values for i in tqdm(tickers.ticker)]

corp_code_match = [(i, find_corp_num(i)) for i in tqdm(tickers.name_short)]
corp_code_match = pd.Series(
    [i[1] for i in corp_code_match], index=tickers.name_short)
corp_codes = corp_code_match.dropna()
tickers2 = tickers.dropna()


#%%
# corp_codes = pd.read_csv('corp_codes.csv',  dtype = str)

# #%%
# match_indice = [corp_codes[corp_codes.stock_code ==i] for i in tqdm(tickers.ticker)]
# match_indice2 = [corp_codes[corp_codes.stock_name ==i] for i in tqdm(tickers.name_short)]
# #%%
# tickers['corp_code'] = [i.corp_code.to_list()[0] if len(i.corp_code) ==1 else np.NaN for i in match_indice ]
# # tickers['corp_code2'] = [i.corp_code.to_list()[0] if len(i.corp_code) ==1 else np.NaN for i in match_indice2 ]
# tickers['industry_code']= [i.induty_code.to_list()[0] if len(i.induty_code) ==1 else np.NaN for i in match_indice ]

# tickers2 = tickers[tickers['corp_code'].notnull()]
# tickers2.to_csv('tickers_merged.csv', index=False)


#%%
def find_value(data, col, args):
    return data[data[col].isin([args])]

def report_date(num):
    return datetime.datetime.strptime(num[:8], "%Y%m%d")

def remove(data):
    if data.fs_div.unique()==['CFS', 'OFS']:
        return data[data.fs_div == 'CFS']
    else:
        return data

def sort_ifrs(data):
    return data[~data['account_id'].isin(['-표준계정코드 미사용-'])]


#%%

url1 = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
url2 = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

def get_report(corp_code, ticker, stock_name, year, quarter, url = 1, fs_div='CFS'):
    if quarter == 1:
        code = '11013' #1분기
    elif quarter == 2:
        code = '11012' #반기
    elif quarter == 3:
        code = '11014' #3분기
    elif quarter == 4:
        code = "11011" #사업보고서

    if url ==1:
        urls = url1
        params = {
        'crtfc_key': key,
        'corp_code': corp_code,
        'bsns_year': year,
        'reprt_code': code
        }
        
    else:
        urls = url2
        params = {
        'crtfc_key': key,
        'corp_code': corp_code,
        'bsns_year': year,
        'reprt_code': code,
        'fs_div': fs_div
        }
    
    resp = requests.get(urls, params=params).json()
    if resp['status'] == '000':
        res = pd.DataFrame(resp['list'])
        if url != 2:
            return res

        elif url ==2:
            res = sort_ifrs(res)
            # name = res[['reprt_code','bsns_year','corp_code']].loc[0]
            # name['ticker'] = ticker
            # name['name'] = stock_name
            # temp = pd.Series(res.thstrm_amount.values, 
            #                 index = res.account_id.values)
            # print(len(temp))
            return res
            # return pd.concat([name, temp]).to_frame().transpose()
    else:
        print(f"{corp_code} at {year}Y, {quarter}Q : status {resp['status']}, {resp['message']}")
        return pd.DataFrame()



# %%
### Testing
if __name__ == "__main__":
    res = get_report(tickers.corp_code[0], tickers.ticker[0], tickers.name_short[0], 
                     year= 2017, quarter = 2, url = 1)

    
    # with tqdm(total=total, position=0, leave=True) as pbar:
    #     for i in tqdm((foo_, ), position=0, leave=True):
    #         pbar.update()
