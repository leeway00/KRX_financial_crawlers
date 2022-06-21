# %%
import datetime
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm

tickers = pd.read_csv('krx_tickers_merged.csv')
tickers['corp_code'] = tickers.corp_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))
tickers['ticker'] = tickers.ticker.apply(
    lambda x: '0'*(6-len(str(x)))+str(int(x)))

key = "126900466a1ceac2a36e14323c19781ddb66ce68"
key2 = "af43a64cd36a06f38ee873de4be55d4deab7c243"

url1 = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
url2 = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

#%%
BS = ["ifrs_Assets", "ifrs_CashAndCashEquivalents", "ifrs_TradeAndOtherCurrentReceivables",
      "ifrs_CurrentAssets",
      "ifrs_CashAndCashEquivalents",
      "ifrs_TradeAndOtherCurrentReceivables",
      "dart_ShortTermTradeReceivable",
      "ifrs_CurrentTaxAssets",
      "ifrs_Inventories",
      "ifrs_NoncurrentAssets",
      "ifrs_PropertyPlantAndEquipment",
      "ifrs_InvestmentProperty",
      "ifrs_DeferredTaxAssets",
      "ifrs_LiabilitiesAbstract",
      "ifrs_CurrentLiabilities",
      "ifrs_TradeAndOtherCurrentPayables",
      "ifrs_CurrentTaxLiabilities",
      "ifrs_NoncurrentLiabilities",
      "ifrs_DeferredTaxLiabilities",
      "ifrs_Equity",
      "ifrs_RetainedEarnings"]

IS = ["ifrs_Revenue",
      "ifrs_CostOfSales",
      "ifrs_GrossProfit",
      "dart_TotalSellingGeneralAdministrativeExpenses",
      "dart_OtherLosses",
      "ifrs_FinanceIncome",
      "ifrs_FinanceCosts",
      "ifrs_ProfitLossBeforeTax",
      "ifrs_ProfitLoss",
      "ifrs_EarningsPerShareAbstract",
      "ifrs_DilutedEarningsLossPerShare"]

CF = ["ifrs_CashFlowsFromUsedInOperatingActivities",
      "ifrs_DividendsPaidClassifiedAsOperatingActivities",
      "ifrs_InterestPaidClassifiedAsOperatingActivities",
      "ifrs_InterestReceivedClassifiedAsOperatingActivities",
      "ifrs_CashFlowsFromUsedInInvestingActivities",
      "ifrs_CashFlowsFromUsedInFinancingActivities"]

BS19 = ["ifrs-full_Assets", "ifrs-full_CashAndCashEquivalents", "ifrs-full_TradeAndOtherCurrentReceivables",
      "ifrs-full_CurrentAssets",
      "ifrs-full_CashAndCashEquivalents",
      "ifrs-full_TradeAndOtherCurrentReceivables",
      "dart_ShortTermTradeReceivable",
      "ifrs-full_CurrentTaxAssets",
      "ifrs-full_Inventories",
      "ifrs-full_NoncurrentAssets",
      "ifrs-full_PropertyPlantAndEquipment",
      "ifrs-full_InvestmentProperty",
      "ifrs-full_DeferredTaxAssets",
      "ifrs-full_LiabilitiesAbstract",
      "ifrs-full_CurrentLiabilities",
      "ifrs-full_TradeAndOtherCurrentPayables",
      "ifrs-full_CurrentTaxLiabilities",
      "ifrs-full_NoncurrentLiabilities",
      "ifrs-full_DeferredTaxLiabilities",
      "ifrs-full_EquityAbstract",
      "ifrs-full_RetainedEarnings"]

IS19 = ["ifrs-full_Revenue",
      "ifrs-full_CostOfSales",
      "ifrs-full_GrossProfit",
      "dart_TotalSellingGeneralAdministrativeExpenses",
      "dart_OtherLosses",
      "ifrs-full_FinanceIncome",
      "ifrs-full_FinanceCosts",
      "ifrs-full_ProfitLossBeforeTax",
      "ifrs-full_ProfitLoss",
      "ifrs-full_EarningsPerShareAbstract",
      "ifrs-full_DilutedEarningsLossPerShare"]

CF19 = ["ifrs-full_CashFlowsFromUsedInOperatingActivities",
      "ifrs-full_DividendsPaidClassifiedAsOperatingActivities",
      "ifrs-full_InterestPaidClassifiedAsOperatingActivities",
      "ifrs-full_InterestReceivedClassifiedAsOperatingActivities",
      "ifrs-full_CashFlowsFromUsedInInvestingActivities",
      "ifrs-full_CashFlowsFromUsedInFinancingActivities"]


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

def get_report(corp_code, ticker, stock_name, year, quarter, url = 1, fs_div='CFS'):
    
    def code_gen(quarter):
        if quarter == 1:
            return '11013' #1분기
        elif quarter == 2:
            return '11012' #반기
        elif quarter == 3:
            return '11014' #3분기
        elif quarter == 4:
            return "11011" #사업보고서
    code = code_gen(quarter)

    # def url_gen(url)

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
        if url == 1:
            name = pd.Series([ticker, stock_name, year, quarter], 
                             index = ['ticker','name','year','quarter'])
            fs_div = res.fs_div[0]
            res = res[res.fs_div == fs_div]
            temp = pd.Series(res.thstrm_amount.values, 
                            index = res.account_nm.values)
            # print(temp.to_frame().transpose())
            return pd.concat([name, temp]).to_frame().transpose()

        elif url ==2:
            res = sort_ifrs(res)
            name = res[['reprt_code','bsns_year','corp_code']].loc[0]
            name['ticker'] = ticker
            name['name'] = stock_name
            temp = pd.Series(res.thstrm_amount.values, 
                            index = res.account_id.values)
            
            return pd.concat([name, temp])
    else:
        print(f"{corp_code}, {ticker}, {stock_name} at {year}Y, {quarter}Q : status {resp['status']}, {resp['message']}")
        return pd.DataFrame()
    
#%%
res = get_report(tickers.corp_code[4], tickers.ticker[4], tickers.name_short[4], 
                 year= 2017, quarter = 4, url = 1)
#%%
res = pd.DataFrame()
for i in tqdm(range(len(tickers))):
    stock = tickers.loc[i]
    corp_code =  stock.corp_code
    ticker = stock.ticker
    name = stock.name_short
    first_year = max(int(stock.listed_data[:4]), 2015) -1
    
    cnt=0
    if first_year <= 2015:
        try:
            for y in range(2021, first_year, -1):
                for q in range(1,5):
                    if (y == 2021) & (q in [3,4]):
                        continue
                    elif (y == 2015) & (q in [1,2,3]):
                        continue
                    else:
                        temp = get_report(corp_code, ticker, name,
                                        year= y, quarter = q, url =1)
                        if len(temp)==0:
                            cnt+=1
                        else:
                            res = pd.concat([res, temp], axis=0)
                    if cnt ==3:
                        break
                if cnt==3:
                    break
                res.reset_index(drop=True)
        # try:
        #     if cnt==0:
        #         cnt = 1
        #     else:
        #         res.to_csv('./krx_market/krx_fundamental_sum.csv', index=False, header =False, mode='a')
        except Exception as e:
            print(e)
            print(ticker, name)
    else:
        pass

res.to_csv('./krx_market/krx_fundamental_sum.csv', index=False, header = True)    
                


