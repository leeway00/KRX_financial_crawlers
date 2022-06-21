# %%
import requests
import pandas as pd
import numpy as np
import json
import re

def get_encparam(code="005930", main = True):
    re_enc = re.compile("encparam: '(.*)'", re.IGNORECASE) 
    if main:
        re_id = re.compile("id: '([a-zA-Z0-9]*)' ?", re.IGNORECASE)
        url = f"http://companyinfo.stock.naver.com/v2/company/c1010001.aspx?cmp_cd={code}"
        html = requests.get(url).text
        encparam = re_enc.search(html).group(1)
        encid = re_id.search(html).group(1)
        return [encparam, encid]
    else:
        url = f"https://navercomp.wisereport.co.kr/company/c1030001.aspx?cmp_cd={code}&cn="
        html = requests.get(url).text
        return [re_enc.search(html).group(1), None]


def format_data(data):
    yrs = pd.DataFrame(data['categories'])[0]
    base = pd.DataFrame(data['series'])
    return pd.DataFrame([np.append(np.delete(j.copy(), [2,3,4]), [j[2][num], yrs[num]]) \
        for j in base.values for num in range(len(j[2]))])

#%%
def get_data(ticker, freq = 'Y', div ='IS', main = True):
    encparam, id = get_encparam(ticker, main = main)
    
    if main:
        referer = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={ticker}"
        params = {
            "cmp_cd": ticker,
            "fin_typ": "0", #주제무제표, 1:K-GAAP (개별), 2:K-GAAP (연결), 3:IFRS (개별), 4:IFRS (연결)
            "freq_typ": freq,
            "encparam": encparam,
            "id": id
        }
        url = 'https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?'
    
    else:
        if freq == 'Y':
            freq = "1"
        elif freq == 'Q':
            freq = "2"
            
        if div == 'IS':
            div = 0
        elif div == 'BS':
            div = 1
        elif div == 'CF':
            div = 2

        referer = f'https://navercomp.wisereport.co.kr/v2/company/c1030001.aspx?cmp_cd={ticker}&cn='
        params = {
            "cmp_cd": ticker,
            "frq": freq,
            "rpt": div,
            "finGubun": "MAIN",
            "frqTyp": '0',
            "cn": '',
            "encparam": encparam
        }
        url = f"https://navercomp.wisereport.co.kr/v2/company/cF3002.aspx"
    
    header = {'User-Agent': 'Mozilla/5.0',
                "Referer": referer}
    rq_text = requests.get(url, params = params, headers =header).text
    if main:
        return pd.read_html(rq_text)[1]
    else:
        return json.loads(rq_text)


#%%
if __name__=='__main__':
    # sample
    ticker = '005930'
    div = 'CF'
    res = get_data(ticker, div = div, main = True)


#%%
#---------------------------------------------#
## Test sample 1
### 재무현황 summary 정보
ticker = "005930"
encparam, id = get_encparam(ticker)
referer = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={ticker}"
header = {'User-Agent': 'Mozilla/5.0',
          "Referer": referer}
params = {
    "cmp_cd": "053270",
    "fin_typ": "0",
    "freq_typ": "Q",
    "encparam": encparam,
    "id": id
}

url = 'https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?'
rq_text = requests.get(url, params = params, headers =header).text
res = pd.read_html(rq_text)[1]


# %%

## Test sample 2
### 세부 계정별 정보
ticker = "005930"
encparam, id = get_encparam(ticker, main=False)
referer = f'https://navercomp.wisereport.co.kr/v2/company/c1030001.aspx?cmp_cd={ticker}&cn='
header = {'User-Agent': 'Mozilla/5.0',
          "Referer": referer}

params = {
    "cmp_cd": ticker,
    "frq": '0',
    "rpt": "1",
    "finGubun": "MAIN",
    "frqTyp": '0',
    "cn": '',
    "encparam": encparam
}

url = f"https://navercomp.wisereport.co.kr/v2/company/cF3002.aspx"
rq_text = requests.get(url, params = params, headers =header).text
res = json.loads(rq_text)

# %%
