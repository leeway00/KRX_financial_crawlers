import pandas as pd
import datetime

def clean_fs(fs,fs_reported,ticker):
    """
    Financial Statements 정리해서 하나의 데이터 프레임 형태로 합치기
    :param fs:
    :param fs_reported:
    :param ticker:
    :return: (pd.DataFrame)
    """

    # 두 개의 데이터 프레임 합치기 위한 key 만들기
    fs['date_key_1'] = fs['year'].astype(str)+"-"+fs['quarter'].astype(str)
    fs_reported['date_key_2'] = fs_reported['year'].astype(str)+"-"+fs_reported['quarter'].astype(str)
    # for ticker in tickers:
    fs_df = fs[fs['Ticker']==ticker].copy()
    fs_df_reported = fs_reported[fs_reported['Ticker']==ticker][['date','date_key_2']].copy()
    fs_df_reported = fs_df_reported.rename(columns = {"date":"reported_date"})
    fs_df = fs_df.set_index('date_key_1')
    fs_df_reported = fs_df_reported.set_index('date_key_2')
    # 두 데이터 프레임 합치기
    merged_fs = fs_df_reported.merge(fs_df, how='inner', left_index=True, right_index=True)
    merged_fs = merged_fs.reset_index(drop=True)
    # 필요없는 컬럼 드랍
    merged_fs = merged_fs.drop(['id'], axis=1)
    # 날짜 순으로 정렬 (최신순)
    merged_fs = merged_fs.sort_values(['date'], ascending=False)

    # 계산하는 데 필요한 column들
    cols_factor = ['Ticker', 'reported_date', 'year', 'quarter', 'CF_freeCashFlow', 'CF_ncfo', 'CF_payDiv',
                   'IS_netinc','IS_eps', 'IS_ebitda','IS_costRev', 'IS_revenue', 'IS_grossProfit','IS_ebit',
                   'BS_totalAssets', 'BS_equity', 'BS_inventory','BS_liabilitiesCurrent', 'BS_liabilitiesNonCurrent',
                   'BS_totalLiabilities','BS_debt', 'BS_assetsNonCurrent', 'BS_accoci','BS_investments',
                   'BS_assetsCurrent', 'BS_acctPay', 'BS_acctRec', 'BS_sharesBasic', 'BS_cashAndEq']

    # 연간 보고서(quarter = 0) 인 row 삭제
    annual_index = merged_fs[merged_fs['quarter'] == 0].index
    merged_fs = merged_fs.drop(annual_index)
    # 필요한 column(cols_factor)만 남기기
    merged_fs = merged_fs[cols_factor]
    # 12개월 누적 net income, dividend column 추가하기
    merged_fs['TTM_netinc'] = merged_fs[::-1]['IS_netinc'].rolling(4).sum()[::-1]
    merged_fs['TTM_dividend'] = merged_fs[::-1]['CF_payDiv'].rolling(4).sum()[::-1]
    merged_fs['TTM_eps'] = merged_fs[::-1]['IS_eps'].rolling(4).sum()[::-1]

    # Financial Statement 자체에 NaN 값이 있는 애들은 애초에 잘 못 되었다고 말해줌
    if merged_fs.isna().sum().sum() > 9:
        print(ticker, "Financial Statment 에 NaN 값 있음")

    return merged_fs

def ratio_calc(merged_fs):
    """
    Fundamental Ratios 계산
    :param merged_fs:
    :return: (pd.DataFrame)
    """
    # Ratio 계산
    temp = merged_fs.copy()
    Ticker = temp.iloc[0].Ticker
    # 필요한 column name assign
    # 제무재표
    total_asset = 'BS_totalAssets'
    cash = 'BS_cashAndEq'
    market_securities = 'BS_investmentsCurrent'
    account_receivables = 'BS_acctRec'
    current_asset = 'BS_assetsCurrent'
    current_liabilities = 'BS_liabilitiesCurrent'
    inventory = "BS_inventory"
    equity = "BS_equity"
    debt = "BS_debt"

    # 손익계산서
    sales = 'IS_revenue'
    cogs = 'IS_costRev'
    TTM_netinc = 'TTM_netinc'
    gross_profit = 'IS_grossProfit'
    ebit = 'IS_ebit'
    eps = 'IS_eps'
    TTM_eps = 'TTM_eps'
    ebitda = 'IS_ebitda'

    # 현금흐름표
    cfo = 'CF_ncfo'
    TTM_dividend = "TTM_dividend" # 이건 음수 값이 나오기 때문에 조심
    free_cf = "CF_freeCashFlow"

    # Liquidity Ratios
    current_ratio = temp[current_asset] / temp[current_liabilities]
    quick_ratio = (temp[cash] + temp[market_securities] + temp[account_receivables]) / temp[current_liabilities]
    cfo_ratio = temp[cfo] / temp[current_liabilities]

    # Activity Ratios
    receivable_to = temp[sales] / temp[account_receivables]
    inventory_to = temp[cogs] / temp[inventory]
    asset_to = temp[sales] / temp[total_asset]

    # Profitability Ratios
    ROE = temp[TTM_netinc] / temp[equity]
    ROA = temp[TTM_netinc] / temp[total_asset]
    net_margin = temp[TTM_netinc] / temp[sales]
    gross_margin = temp[gross_profit] / temp[sales]
    op_margin = temp[ebit] / temp[sales]
    eps = temp[eps]
    payout = -1 * (temp[TTM_dividend] / temp[TTM_netinc])

    # Solvency Ratios
    debt_asset = temp[debt] / temp[total_asset]
    debt_equity = temp[debt] / temp[equity]
    financial_leverage = temp[total_asset] / temp[equity]

    # 그 외 나중 ratio 를 계산하기 위해 필요한 것들
    TTM_netinc = temp[TTM_netinc]
    TTM_eps = temp[TTM_eps]
    equity = temp[equity]
    sales = temp[sales]
    free_cf = temp[free_cf]
    dividend = temp[TTM_dividend]
    ebitda = temp[ebitda]

    # Make Columns
    cols = ['current_ratio', 'quick_ratio', 'cfo_ratio','receivable_to', 'inventory_to','asset_to', 'ROE', 'ROA', 'net_margin',
            'gross_margin', 'op_margin','eps', 'payout', 'debt_asset', 'debt_equity', 'financial_leverage','TTM_netinc',
           'TTM_eps','equity', 'sales', 'free_cf', 'dividend', 'ebitda']

    vals = [current_ratio, quick_ratio, cfo_ratio,receivable_to, inventory_to,asset_to, ROE, ROA, net_margin,
            gross_margin, op_margin,eps, payout, debt_asset, debt_equity, financial_leverage,TTM_netinc,
           TTM_eps, equity, sales, free_cf, dividend, ebitda]

    ticker_factor = pd.DataFrame(vals).T
    ticker_factor.columns = cols
    ticker_factor.insert(0, 'reported_date', temp.reported_date)

    # Quarterly -> Daily 데이터로 바꿔주기
    daily_df = ticker_factor.copy()
    start_date = daily_df.reported_date.min()
    # 가장 최근 공시일까지만 일단 만드는 것으로. 추후 추가할 때 이 날짜부터 추가하면 됌
    end_date = daily_df.reported_date.max()
    dates = pd.date_range(start_date, end_date, freq="D")[::-1]
    daily_df = daily_df.set_index('reported_date')
    # 새로 만든 dates 라는 인덱스로 재설정 후 backfill
    daily_df = daily_df.reindex(dates, method='backfill').reset_index()
    # 10개의 컬럼에서 non-na 값이 있으면 drop 안하고 그것보다 적으면 drop
    daily_df = daily_df.dropna()
    daily_df = daily_df.rename(columns={'index': 'Date'})
    # 만든 시간 Timestamp
    now = datetime.datetime.now()
    daily_df.insert(0, 'created_date', now)
    # 티커 추가
    daily_df.insert(0, 'Ticker', Ticker)

    return daily_df

def calc_price_factors(daily_df,daily_fs,small_price,ticker):
    """
    Price 가 들어간 Factor 들 계산하기
    :param daily_df:
    :param daily_fs:
    :param small_price:
    :param ticker:
    :return: (pd.DataFrame)
    """
    # Daily Financials 가 가장 짧으니 그거의 날짜를 기준으로 합치기

    temp = daily_df.copy()
    ticker_daily_fs = daily_fs[daily_fs['ticker']==ticker]
    # 과거부터 위로 와있어서 거꾸로 뒤집어 줌
    ticker_daily_fs = ticker_daily_fs[::-1]
    # 필요한 columns 만 가져오기
    ticker_daily_fs = ticker_daily_fs[['date','market_cap','enterprise_val']]
    # 0 이랑 NaN 값들 다 채워 넣기(한 분기 끝날 때마다만 값이 없음)
    ticker_daily_fs['market_cap'] =ticker_daily_fs['market_cap'].replace(to_replace=0, method='backfill')
    ticker_daily_fs['enterprise_val']= ticker_daily_fs['enterprise_val'].fillna(method = 'backfill')
    # 날짜로 인덱싱 해준다음에 merge
    ticker_daily_fs = ticker_daily_fs.set_index('date')
    temp = temp.set_index('Date')
    ticker_daily_factors = ticker_daily_fs.merge(temp,how='inner',left_index=True,right_index=True)

    # 가격 거를 daily financials 에 합치기
    temp_price = small_price.copy()
    temp_price = temp_price[temp_price['ticker'] == ticker]
    # 과거부터 위로 와있어서 거꾸로 뒤집어 줌
    temp_price = temp_price[::-1]
    # 필요한 columns 만 가져오기
    temp_price = temp_price[['price_date', 'adj_close_price']]
    temp_price = temp_price.set_index('price_date')
    daily_factors = ticker_daily_factors.merge(temp_price, how='inner', left_index=True, right_index=True)
    daily_factors = daily_factors.reset_index()
    daily_factors = daily_factors.rename(columns={'index': 'Date'})

    # Price 관련 Factor 만들기
    temp_factors = daily_factors.copy()
    temp_factors['div_yield'] = -1 * temp_factors['dividend'] / temp_factors['market_cap']
    temp_factors['PER'] = temp_factors['adj_close_price'] / temp_factors['TTM_eps']
    temp_factors['BM'] = temp_factors['equity'] / temp_factors['market_cap']
    temp_factors['PSR'] = temp_factors['market_cap'] / temp_factors['sales']
    temp_factors['P_FCF'] = temp_factors['market_cap'] / temp_factors['free_cf']
    temp_factors['ev_ebitda'] = temp_factors['enterprise_val'] / temp_factors['ebitda']

    # 다 만들고 필요없는 column drop
    cols_drop = ['TTM_netinc', 'TTM_eps', 'equity', 'sales', 'free_cf', 'dividend', 'ebitda']
    temp_factors = temp_factors.drop(columns=cols_drop)

    # 만약 nan 값 하나라도 있으면 이상하다고 말해주기!
    if temp_factors.isna().sum().any():
        print(ticker, "NaN Values 있음!")

    return temp_factors

