# %%
import datetime
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
import dart_fss as dart

tickers = pd.read_csv('krx_tickers_merged.csv')
tickers['corp_code'] = tickers.corp_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))
tickers['ticker'] = tickers.ticker.apply(
    lambda x: '0'*(6-len(str(x)))+str(int(x)))


key = "126900466a1ceac2a36e14323c19781ddb66ce68"
# key = "af43a64cd36a06f38ee873de4be55d4deab7c243"

url1 = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
url2 = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"


# %%
dart.set_api_key(key)

corp_list = dart.get_corp_list()

corp_code = tickers.corp_code[1]

fs = dart.fs.extract(corp_code=corp_code, bgn_de='20000101')


# %%
for i in tqdm(range(1)):  # len(tickers))):
    stock = tickers.loc[i]
    corp_code = stock.corp_code
    ticker = stock.ticker
    name = stock.name_short
    first_year = max(int(stock.listed_data[:4]), 2015) - 1

    res = pd.DataFrame()
    for y in range(2021, first_year, -1):
        for q in range(1, 5):
            if y == 2021 & (q in [3, 4]):
                continue







#%%
BS = ["ifrs-full_Assets", "ifrs-full_CashAndCashEquivalents", "ifrs-full_TradeAndOtherCurrentReceivables",
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

IS = ["ifrs-full_Revenue",
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

CF = ["ifrs-full_CashFlowsFromUsedInOperatingActivities",
      "ifrs-full_DividendsPaidClassifiedAsOperatingActivities",
      "ifrs-full_InterestPaidClassifiedAsOperatingActivities",
      "ifrs-full_InterestReceivedClassifiedAsOperatingActivities",
      "ifrs-full_CashFlowsFromUsedInInvestingActivities",
      "ifrs-full_CashFlowsFromUsedInFinancingActivities"]


