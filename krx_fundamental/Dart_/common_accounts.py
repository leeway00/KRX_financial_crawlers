#%%
import os
os.chdir('..')
#%%
from fundamental_bottomup import Dart
import fund_utils as fu
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
import time
import random



tickers = pd.read_csv('./krx_tickers_merged.csv')
tickers['corp_code'] = fu.FundUtils.str_format(tickers, 'corp_code', 8)
tickers['ticker'] = fu.FundUtils.str_format(tickers, 'ticker', 6)

key = "126900466a1ceac2a36e14323c19781ddb66ce68"
key2 = "af43a64cd36a06f38ee873de4be55d4deab7c243"
loader = Dart(key)

sj = [['BS'], ['IS','CIS'], ['CF']]

def get_sj(data):
    ifrs, dart = {}, {}
    for sj_div in sj:
        ifrs[sj_div[0]] = data[(data["sj_div"].isin(sj_div)) & 
                              (data.account_id.str.contains('ifrs'))].account_id.unique()
        dart[sj_div[0]] = data[(data["sj_div"].isin(sj_div)) & 
                              (data.account_id.str.contains('dart'))].account_id.unique()
    return ifrs, dart


def loop(stock, year, conn):
    ifrs, dart = {}, {}
    for q in range(1,5):
        try:
            ifrs_temp, dart_temp = get_sj(conn.report_all(stock.corp_code, stock.ticker, 
                                                        stock.name_short, year, q))
            ifrs = sj_dict(ifrs, ifrs_temp)
            dart = sj_dict(dart, dart_temp)
        except:
            global call_err
            call_err = con = conn.report_all(stock.corp_code, stock.ticker, 
                                    stock.name_short, year, q)
            pass
    try:
        return sj_count(ifrs), sj_count(dart)
    except:
        return stock.ticker, stock.name_short
    
    
def sj_count(main):
    for sj_div in sj:
        main[sj_div[0]] = pd.DataFrame(main[sj_div[0]]).value_counts()
    return main


def sj_dict(dic, reprt):
    if 'BS' in dic.keys():
        for sj_div in sj:
            dic[sj_div[0]] = np.intersect1d(dic[sj_div[0]], reprt[sj_div[0]])
    else:
        for sj_div in sj:
            dic = reprt
    return dic

def sj_append(main, new):
    for sj_div in sj:
        if sj_div[0] in main.keys():
            main[sj_div[0]] = main[sj_div[0]].add(new[sj_div[0]], fill_value=0)
        else:
            try:
                main[sj_div[0]] = new[sj_div[0]]
            except:
                global err
                err = new
    return main

#%%
if __name__ == "__main__":
    # first_year = max(int(stock.listed_data[:4]), 2015) - 1
    
    for year in tqdm([2021, 2022]):
    # year = 2020
        ifrs, dart = {}, {}
        for i in tqdm(range(len(tickers))):
            stock = tickers.loc[i]
            ifrs_, dart_ = loop(stock, year, conn = loader)
            if type(ifrs_) == str:
                continue
            ifrs, dart = sj_append(ifrs, ifrs_), sj_append(dart, dart_)
            time.sleep(random.randint(1,2))
        print(loader.failed)
        
        
        final = pd.DataFrame()
        for key in ifrs.keys():
            final = pd.concat([final, 
                            pd.DataFrame({'ifrs_'+key+'_nm': [i[0] for i in ifrs[key].index],
                                            'ifrs_'+key+'_cnt': ifrs[key].values.tolist()})], 
                            axis = 1)
        for key in dart.keys():
            final = pd.concat([final, 
                            pd.DataFrame({'dart_'+key+'_nm': [i[0] for i in dart[key].index],
                                            'dart_'+key+'_cnt': dart[key].values.tolist()})], 
                            axis=1)        
        final.to_csv(f'./krx_fundamental/accounts/common_account19_{year}_{str(datetime.now())[8:16]}.csv', index=False)
        loader.clear_failed()


# %%
