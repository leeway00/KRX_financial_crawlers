# %%
import datetime
from sqlite3 import DatabaseError
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
import fund_utils as fu


class Dart:
    def __init__(self, key):
        self.key = key
        self.failed = []

    def code_quarter(self, quarter):
        if quarter == 1:
            return '11013'  # 1분기
        elif quarter == 2:
            return '11012'  # 반기
        elif quarter == 3:
            return '11014'  # 3분기
        elif quarter == 4:
            return "11011"  # 사업보고서

    def get_report(self, url, params, *args):
        resp = requests.get(url, params=params).json()
        if resp['status'] == '000':
            res = pd.DataFrame(resp['list'])
            return res
        else:
            # print(f"{[*args]} at Y{params['bsns_year']}, Q{params['reprt_code']} \n \
            #     status {resp['status']}, {resp['message']}")
            self.failed.append(
                [*args, params['bsns_year'], params['reprt_code']])
            return pd.DataFrame()

    def clear_failed(self):
        self.failed = []

    def report_all(self, corp_code, ticker, stock_name, year, quarter, fs_div='CFS'):
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        code = self.code_quarter(quarter)
        params = {
            'crtfc_key': self.key,
            'corp_code': corp_code,
            'bsns_year': year,
            'reprt_code': code,
            'fs_div': fs_div
        }
        resp = self.get_report(url, params, ticker, stock_name)
        if len(resp) == 0:
            return resp
        else:
            resp = fu.FundUtils.sort_ifrs(resp)
            return resp
            name = pd.Series([ticker, stock_name, *resp.values[0][0:4]],
                            index=['ticker', 'stock_name', 'reprt_no', 'reprt_code', 'bsns_year', 'corp_code'])
            temp = pd.Series(resp.thstrm_amount.values,
                            index=resp.account_id.values)
            return pd.concat([name, temp])

    def report_main(self, corp_code, ticker, stock_name, year, quarter, fs_div='OFS'):
        url = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
        code = self.code_quarter(quarter)
        params = {
            'crtfc_key': self.key,
            'corp_code': corp_code,
            'bsns_year': year,
            'reprt_code': code
        }
        resp = self.get_report(url, params, ticker, stock_name)
        fs_div = resp.fs_div.unique()
        print(fs_div)
        name = resp.iloc[0:len(fs_div),0:4]
        name[['ticker','stock_name']] = ticker, stock_name
        
        temp = pd.DataFrame()
        for i in fs_div:
            res = resp[resp.fs_div == i]
            temp2 = pd.DataFrame([res.thstrm_amount.tolist()],
                    columns=res.account_nm.values, index = [i])
            temp = pd.concat([temp, temp2], axis=0)
        return pd.concat([name, temp.reset_index()], axis = 1)
        


# %%
if __name__ == "__main__":
    tickers = pd.read_csv('krx_tickers_merged.csv')
    tickers['corp_code'] = fu.FundUtils.str_format(tickers, 'corp_code', 8)
    tickers['ticker'] = fu.FundUtils.str_format(tickers, 'ticker', 6)

    key = "126900466a1ceac2a36e14323c19781ddb66ce68"
    key2 = "af43a64cd36a06f38ee873de4be55d4deab7c243"
    loader = Dart(key)
    # res = loader.report_main(tickers.corp_code[4], tickers.ticker[4], tickers.name_short[4],
    #                         year='2020', quarter=4)
    
# %%

    res = {}
    for i in tqdm(range(100)):
        stock = tickers.loc[i]
        corp_code = stock.corp_code
        ticker = stock.ticker
        name = stock.name_short
        # first_year = max(int(stock.listed_data[:4]), 2015) - 1
        year = '2020'

        for q in range(1, 5):
            # quarter = q
            try:
                if ticker not in res.keys():
                    res[ticker] = [loader.report_all(corp_code, ticker, name, year, q)]
                else:
                    res[ticker].append(loader.report_all(corp_code, ticker, name, year, q))
            except Exception as e:
                # print(f"{ticker} at {year}, {q}")
                # print(e)
                pass
    print(loader.failed)

#%%
    divs = {}
    sj = [['BS'], ['IS','CIS'], ['CF']]
    for sj_div in sj:
        temp = pd.DataFrame()
        for i in tqdm(res.keys()):
            for num in range(len(res[i])):
                j = res[i][num]
                if len(j) == 0:
                    continue
                elif len(temp) == 0:
                    temp = j[j.sj_div.isin(sj_div)][['account_id', 'account_nm']].rename({
                        'account_id': f'id', 'account_nm': f'nm'}, axis=1)
                else:
                    temp = pd.merge(temp, j[j.sj_div.isin(sj_div)][['account_id', 'account_nm']].rename({
                        'account_id': f'id_{i}_{num}', 'account_nm': f'nm_{i}_{num}'}, axis=1),
                                    left_on = 'id', right_on = f'id_{i}_{num}', how = 'outer')
                temp = temp.drop_duplicates(['id']).dropna()[['id', 'nm']]
        divs[sj_div[0]] = temp.drop_duplicates(['id'])[['id','nm']]
#%%
    tt = pd.DataFrame()
    for key in divs.keys():
        tt = pd.concat([tt, divs[key].rename({
            'id': f'{key}_id', 'nm': f'{key}_nm'}, axis=1)], axis=1, ignore_index=True)
    tt.to_csv('./accounts/common_19_2.csv', index=False)


#%%
        
        # cnt = 0
        # if first_year <= 2015:
        #     try:
        #         for y in range(2021, first_year, -1):
        #             for q in range(1, 5):
        #                 if (y == 2021) & (q in [3, 4]):
        #                     continue
        #                 elif (y == 2015) & (q in [1, 2, 3]):
        #                     continue
        #                 else:
        #                     temp = get_report(corp_code, ticker, name,
        #                                     year=y, quarter=q, url=1)
        #                     if len(temp) == 0:
        #                         cnt += 1
        #                     else:
        #                         res = pd.concat([res, temp], axis=0)
        #                 if cnt == 3:
        #                     break
        #             if cnt == 3:
        #                 break
        #             res.reset_index(drop=True)
        #     # try:
        #     #     if cnt==0:
        #     #         cnt = 1
        #     #     else:
        #     #         res.to_csv('./krx_market/krx_fundamental_sum.csv', index=False, header =False, mode='a')
        #     except Exception as e:
        #         print(e)
        #         print(ticker, name)
        # else:
        #     pass

