#%%
from fundamental_naverfin_spec import get_data
import pandas as pd

def extract_div(respd):
    ls = list(respd.ACC_NM)
    dicres = {}
    cnt1 = 0
    cnt2 = 0
    cnt3 = 0
    cnt4 = 0
    for i in ls:
        if '............' in i:
            cnt4+=1
        elif '........' in i:
            cnt3+=1
            cnt4=0
        elif '....' in i:
            cnt2+=1
            cnt3 = cnt4 = 0
        else:
            cnt1+=1
            cnt2 = cnt3 = cnt4 = 0
        dicres[i] = f'{cnt1}.{cnt2}.{cnt3}' 
    return dicres
        
# %%
tickers = pd.read_csv('krx_tickers.csv')

res = get_data('005930', div = 'IS', main = False)
respd = pd.DataFrame(res['DATA'])
temp2 = extract_div(respd)

#%%
for i in range(100):
    ticker = tickers.values[i][1]
    res = get_data(ticker, div = 'IS', main = False)
    respd = pd.DataFrame(res['DATA'])
    temp = extract_div(respd)
    
    if temp != temp2:
        print(i)

# %%
