#%%
import datetime
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
from fin_utils import *

key = "126900466a1ceac2a36e14323c19781ddb66ce68"
key2 = "af43a64cd36a06f38ee873de4be55d4deab7c243"

url_format = "https://opendart.fss.or.kr/api/xbrlTaxonomy.json"


@check_response
def get_accounts(key, sj_div):
    # sj_div from account_types.csv
    
    url_format = "https://opendart.fss.or.kr/api/xbrlTaxonomy.json"
    params = {'crtfc_key': key, 'sj_div': sj_div}

    resp = requests.get(url_format, params=params).json()
    if resp['status'] == '000':
        res = pd.DataFrame(resp['list'])
        return res
    else:
        raise Exception(resp)
    

#%%
if __name__=="__main__":
    
    # get all account types
    types = pd.read_csv('account_types.csv')
    for i in types.type:
        temp = get_accounts(key, i)
        temp.to_csv("accounts19/"+i+'.csv', index=False)


    # print common account_ids of each types
    import os
    lr = os.listdir('accounts19')
    res = {}
    for i in lr:
        res[i] = pd.read_csv('accounts19/'+i)

    temp = pd.Series(res.keys())
    keys = pd.Series([i[:-5] for i in res.keys()]).unique()
    res2 = {}
    for i in keys:
        res2[i] = [temp.pop(j) for j in temp[temp.str.contains(i)].index]

    summary = {}
    for i in keys:
        account_data = [res[j] for j in res2[i]]
        cnt = 0 
        temp_res = pd.DataFrame()
        for j in res2[i]:
            if cnt == 0:
                temp_res = res[j].account_id
                cnt += 1
            else:
                temp_res = temp_res[temp_res.isin(res[j].account_id)]
        summary[i] = temp_res.reset_index(drop=True)

    pd.DataFrame(summary).to_csv('accounts19_common.csv', index=False)

# %%
