# %%
import requests
import pandas as pd
import numpy as np
import re

def format_data(data):
    yrs = pd.DataFrame(data['categories'])[0]
    base = pd.DataFrame(data['series'])
    return pd.DataFrame([np.append(np.delete(j.copy(), [2,3,4]), [j[2][num], yrs[num]]) \
        for j in base.values for num in range(len(j[2]))])

def get_data(ticker, div ='IS', freq = 'Y', main = True):
    """get_data _summary_

    :param div: 종류
    :type div: IS, BS, CF
    :param freq: 연간, 분기
    :type freq: Y, Q
    :param main: main은 네이버 재무 화면내역대로, False이면 파싱 전 데이터 (ecnparam을 넣었는데도 현재 작동이 안됨)
    
    :Returns: fs_main: 재무제표 항목 및 값, fs_ratio 각 재무제표 기반의 재무비율
    """
    
    URL = f'https://navercomp.wisereport.co.kr/company/chart/c1030001.aspx?cmp_cd={ticker}&frq={freq}&rpt={div}M&finGubun=MAIN&chartType=svg'
    response_data = requests.get(URL)
    if response_data.status_code == 200:
        response_data = response_data.json()
        try:
            fs_main = format_data(response_data['chartData1'])
            fs_ratio = format_data(response_data['chartData2'])
        except Exception as e:
            print(e)
    else:
        raise Exception('Response failed')
    return fs_main, fs_ratio



    
#%%
if __name__=='__main__':
    # sample
    ticker = '005930'
    div = 'CF'
    fs_main, fs_ratio = get_data(ticker, div)
    print(fs_main, fs_ratio)

