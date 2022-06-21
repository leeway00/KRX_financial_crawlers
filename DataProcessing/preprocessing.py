import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.tsa.api as tsa

class finOLS:
    '''
    재무데이터 제거한 residual 및 OLS parameter를 저장해놓기 위한 클래스
    -------------
    y: price
    X: 재무팩터
    -------------
    self.res : statsmodels regression 모델
    self.sum : 모델 summary
    self.param : 모델 parameter
    self.resid : 재무팩터 제외하고 residual
    self.lenght : 회귀돌린 기간
    '''

    def __init__(self, y, X) -> None:
        def reg(y, X):
            X = sm.add_constant(X).astype('float')
            return sm.OLS(y, X, hasconst = True).fit()
        self.res = reg(y, X)
        self.sum = self.res.summary()
        self.param = self.res.params
        self.resid = self.res.resid.to_numpy()
        self.lengh = len(y)
    
    def test(self, ytest, Xtest):
        Xtest['const'] = 1
        self.pred = np.matmul(self.param.values, Xtest.values.transpose())
        self.test_resid = ytest - self.pred
        return self.test_resid
    
def fastOLS(y, X) -> np.array:
    """
    return only residual of OLS
    """
    X = sm.add_constant(X).astype('float')
    return sm.OLS(y, X, hasconst = True).fit().resid.to_numpy()