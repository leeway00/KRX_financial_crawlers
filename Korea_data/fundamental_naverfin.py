# %%
import datetime
from unicodedata import name
import requests
import pandas as pd
from tqdm import tqdm
# import dbconfig
# from hiroku import hiroku

tickers = pd.read_csv('krx_tickers.csv')

# %%


def get_NAVERfin(ticker, name):
    URL = "https://finance.naver.com/item/main.nhn?code=" + ticker

    company = requests.get(URL)
    df = pd.read_html(company.text)[3]
    try:
        df.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
        df.index.rename('', inplace=True)
        df.columns = df.columns.droplevel(2)

        annual_finance = pd.DataFrame(df).xs('최근 연간 실적', axis=1)
        quarter_finance = pd.DataFrame(df).xs('최근 분기 실적', axis=1)

        def expectation(data):
            if len(data['date']) > 7:
                data['E'] = 1
                data['date'] = data['date'][:-3]
            else:
                data['E'] = 0
            return data

        def adjustment(data):
            data = data.transpose().reset_index().rename(
                columns={'index': "date"})
            data['ticker'] = ticker
            data['name'] = name

            data = data.apply(lambda x: expectation(x), axis=1)

            cols = data.columns.to_list()
            data = data[cols[-3:] + cols[:-3]]
            return data

        return adjustment(annual_finance), adjustment(quarter_finance)
    except:
        print(name)
        pass


if __name__ == '__main__':

    cnt = 0
    for i in tqdm(tickers.values):
        ticker, name = i[2], i[3]
        try:
            annual, quarter = get_NAVERfin(ticker, name)
            if cnt == 0:
                header = True
                cnt += 1
            else:
                header = False
            annual.to_csv('./corp_num/annual_fin.csv',
                          mode='a', index=False, header=header)
            quarter.to_csv('./corp_num/quarter_fin.csv',
                           mode='a', index=False, header=header)
        except:
            pass
