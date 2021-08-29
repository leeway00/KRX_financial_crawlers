from financials_factor import *
from technical_factor import *
from tqdm import tqdm
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://quantry:"+"quantry"+"@localhost:3306/quantry_marketdata?charset=utf8", encoding='utf-8')
conn = engine.connect()

def factor_to_sql(ticker_list, fundamental_tiingo, fundamental_reported, price_df, daily_financials):
    """
    펀더맨탈 팩터를 sql에 집어넣는 코드
    미리 fs, fs_reported, small_price, daily_fs 를 지정해줘야한다
    :param ticker_list:
    :param fundamental_tiingo:
    :param fundamental_reported:
    :param price_df:
    :param daily_financials:
    :return: None
    """
    price_after_2005 = price_df[price_df.price_date > datetime.date(2004, 12, 31)]
    fs, fs_reported, small_price, daily_fs = fundamental_tiingo.copy(), fundamental_reported.copy(), price_after_2005.copy(), daily_financials.copy()
    for ticker in tqdm(ticker_list):
        try:
            merged_fs = clean_fs(fs, fs_reported, ticker)
            daily_df = ratio_calc(merged_fs)
            temp_factors = calc_price_factors(daily_df, daily_fs, small_price, ticker)
            # inf 값을 nan 으로 변경해준다
            temp_factors = temp_factors.replace([np.inf, -np.inf], np.nan)
            # nan 이나 inf 값이 있다면 표시하고 backfill 해준다
            if temp_factors.isna().sum().sum() > 0:
                print(ticker, temp_factors.isna().sum().sum())
                temp_factors.fillna(method='backfill')
            temp_factors.to_sql(name='financials_factor', con=engine, if_exists='append', index=False)
        except Exception as e:
            print(ticker, e)

def technical_to_sql(ticker_list):
    """
    :param ticker_list:
    :return: mysql에 technical factors 집어넣기
    """
    for ticker in tqdm(ticker_list):
        try:
            technical_df = technical_factors(ticker,price)
            technical_df.to_sql(name='technical_factor', con=engine, if_exists='append', index=False)

        except Exception as e: print(ticker, e)

