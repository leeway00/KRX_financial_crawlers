import pandas as pd
import math
import numpy as np
import datetime

def SMA(data, period):
    """
    Calculating Simple Moving Average
    :param data:
    :param period:
    :return: (pd.Series)
    """
    return data.rolling(period).mean().dropna().rename(level=0, index='SMA'+str(period))

def EMA(data, period):
    """
    Calculating Exponential Moving Average
    :param data:
    :param period:
    :return: (pd.Series)
    """
    return data.ewm(period).mean().dropna().rename(level=0, index='EMA'+str(period))

def WMA(data, period, weight=[]):
    """
    Calculating Weighted Moving Average
    :param data:
    :param period:
    :param weight:
    :return: (pd.Series)
    """
    if weight==[]:
#         weight=[i for i in range(1,period+1)]
        weight=np.arange(1,period+1)
    ##weight should be list like type with length=period
    r=data.rolling(period).apply(lambda prices: np.dot(prices, weight)/sum(weight), raw=True).dropna()
    return r.rename(level=0, index='WMA'+str(period))

def ichimoku_cloud(price):
    """
    Calculating Ichimoku Cloud. Lagging Span 은 미래의 가격을 보기때문에 제외
    :param price:
    :return: (pd.DataFrame)
    """
    #price를 데이터로 주어야함
    #baseline확인하는 지표. return하는 column들의 관계와 논리성을 잘 살펴야함.
    def conversionline(data):
        return ((data.rolling(window=9).max()+data.rolling(window=9).min())/2).rename(level=0, index='conversion_line9')
    def baseline(data):
        return ((data.rolling(window=26).max()+data.rolling(window=26).min())/2).rename(level=0, index='base_line26')
#     def lagging(data):
#         return data.shift(-26).rename(level=0, index='lagging_span')
    def leadingA(data):
        return ((conversionline(data)+baseline(data))/2).rename(level=0, index='leading_spanA')
    def leadingB(data):
        return ((data.rolling(window=52).max()+data.rolling(window=52).min())/2).rename(level=0, index='leading_spanB')
    res=pd.concat([conversionline(price), baseline(price),
                   leadingA(price), leadingB(price)], axis=1)
    #leading A>B이면 uptrend
    trend=(res.leading_spanA>res.leading_spanB).astype(int).rename(index='clound_trend' )
    return pd.concat([res, trend], axis=1)

def VWMA(data, column, days, weight='volume'):
    """
    Calculating Volume Weighted Moving Average
    :param data:
    :param column:
    :param days:
    :param weight:
    :return: (pd.Series)
    """
    pri=data[column]
    vol=data[weight]
    weipri=pri*vol
    volsum=vol.rolling(days).sum()
    weiprisum=weipri.rolling(days).sum()
    return (weiprisum/volsum).rename(level=0, index='VWMA'+str(days))

def HMA(data, period):
    """
    Calculating Hull Moving Average
    :param data:
    :param period:
    :return: (pd.Series)
    """
    half_length=period // 2
    sqrt_length=int(math.sqrt(period))
#     tem=pd.DataFrame()
    tem=2*WMA(data, period=half_length)-WMA(data, period=period)
    return WMA(tem, period=sqrt_length).rename(level=0, index='HMA'+str(period))


def RSI(price: pd.DataFrame, period=14, adjust=True) -> pd.Series:
    """
    Calculating RSI
    :param price:
    :param period:
    :param adjust:
    :return: (pd.Series)
    """
    # input으로 ajd_close_price
    delta = price.diff()
    # positive gains (up) and negative gains (down) Series
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0

    # EMAs of ups and downs
    _gain = up.ewm(alpha=1.0 / period, adjust=adjust).mean()
    _loss = down.abs().ewm(alpha=1.0 / period, adjust=adjust).mean()

    RS = _gain / _loss
    return pd.Series(100 - (100 / (1 + RS)), name="RSI" + str(period))

def StochK(price: pd.DataFrame, period = 14) -> pd.Series:
    """
    Calculating Stochastic Oscillator %K
    :param price:
    :param period:
    :return: (pd.Series)
    """
    highest_high = price.adj_high_price.rolling(center=False, window=period).max()
    lowest_low = price.adj_low_price.rolling(center=False, window=period).min()

    STOCH = pd.Series((price.adj_close_price - lowest_low) / (highest_high - lowest_low) * 100,
                      name="StochK"+str(period))
    return STOCH

def StochD(price:pd.DataFrame, period=3)->pd.Series:
    """
    Calculating Stochastic Oscillator %D
    :param price:
    :param period:
    :return: (pd.Series)
    """
    return StochK(price).rolling(period).mean().rename('StochD'+str(period))

def CCI(price:pd.DataFrame, period=20):
    """
    Calculating Commodity Channel Index
    :param price:
    :param period:
    :return: (pd.Series)
    """
    #종목 dataframe전체를 input으로
    typical_price=(price.adj_high_price+price.adj_low_price+price.adj_close_price)/3
    ma=typical_price.rolling(period).mean() #moving average
    typical_price=typical_price[period-1:]
    #mean deviation
    md=(typical_price-ma).rolling(period).apply(lambda x: sum([abs(i)for i in x]))
    return ((typical_price-ma)/(md*0.015)).rename('CCI'+str(period))

def MACD(data, t1=12, t2=26):
    """
    Calculating Moving Average Convergence Divergence
    :param data:
    :param t1:
    :param t2:
    :return: (pd.Series)
    """
    def EMA(data, period):
        return data.ewm(period).mean().dropna()
    return (EMA(data, t1)-EMA(data, t2)).rename('MACD'+str(t1)+'_'+str(t2))

def KLength(data_open, data_close):
    """
    Calculating K Length
    :param data_open:
    :param data_close:
    :return:(pd.Series)
    """
    #Input으로 close, open price 데이터 주가
    return (data_close-data_open).rename('KLength')

def KUpperLength(data_open, data_close, data_high):
    """
    Calculating K upper length
    :param data_open:
    :param data_close:
    :param data_high:
    :return: (pd.Series)
    """
    tem=pd.DataFrame([data_open, data_close]).max(axis=0)
    return (data_high-tem).rename('KUpperLength')

def Bias(data_close, period=20):
    return data_close.rolling(period+1).apply(lambda x: x[-1]-sum(x[:-1])/(period)).rename('Bias'+str(period))

def ROC(data_close, period=20):
    return data_close.rolling(period+1).apply(lambda x: (x[-1]-x[0])/x[0]).rename('ROC'+str(period))

def MeanAmplitude(data_close, data_high, data_low, period=5):
    tem=(data_high-data_low)/data_close
    return tem.rolling(period).mean().rename('MeanAmplitude'+str(period))



def technical_factors(ticker,price):
    """
    한 티커에 대해 모든 Technical Factor 계산하기
    :param ticker:
    :param price:
    :return: (pd.DataFrame)
    """
    # 티커 가격 가져오기
    tem_p = price[price.ticker == ticker]

    # 하나의 티커에 대한 price값이 여러개 들어가있는경우
    if tem_p.index.is_unique == False:
        tem_p = tem_p[~tem_p.index.duplicated(keep='first')]

    pce = tem_p.adj_close_price
    high = tem_p.adj_high_price
    low = tem_p.adj_low_price
    opp = tem_p.adj_open_price
    time = (5, 10, 20, 30, 100, 200)

    # 일단은 MA1은 SMA, EMA만. iterative하게 돌아가는 것들.
    MA1 = pd.DataFrame()
    for j in time:
        sma = SMA(pce, j)
        ema = EMA(pce, j)
        ma_merged = pd.merge(sma, ema, right_index=True, left_index=True)
        MA1 = pd.concat([MA1, ma_merged], axis=1)

    wma20 = WMA(pce, 20)
    wma100 = WMA(pce, 100)
    vwma = VWMA(tem_p, 'adj_close_price', 20)
    hma = HMA(pce, 9)
    cloud = ichimoku_cloud(pce)
    # 여기부터 oscillator
    cci = CCI(tem_p)
    rsi = RSI(pce, period=10)
    stK = StochK(tem_p)
    stD = stK.rolling(3).mean().rename('StochD' + str(3))
    macd = MACD(pce)
    kl = KLength(opp, pce)
    kul = KUpperLength(opp, pce, high)
    bias = Bias(pce)
    roc = ROC(pce)
    mal = MeanAmplitude(pce, high, low)

    # MA1 wma, vwma, hma 넣기. reset_index하고, 기존 index열은 date로 이름 바꿈
    res = pd.concat([MA1, wma20, wma100, vwma, hma, cloud,
                     rsi, cci, stK, stD, macd, kl, kul, bias, roc, mal], axis=1).sort_index()
    res = res.reset_index().rename(columns={"index": "date"})

    # 만든 시간 Timestamp
    now = datetime.datetime.now()
    res.insert(0, 'created_date', now)
    # 티커 column 추가
    res.insert(0, 'ticker', ticker)
    res = res.dropna().reset_index(drop=True)

    return res


