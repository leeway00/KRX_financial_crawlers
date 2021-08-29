import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
        

class CrossCorr:
    '''
    # variables
    self.y_label
    self.data
    self.cc2d

    # methods
    self.__init__:
        # args
        data: 전체 데이터
        y_label: 가격 라벨
    
    self.CC2D: 2dimension에서의 cross correlation을 결과값으로 줌.
        return의 correlation을 보고싶으면 ret에 날짜를 넣으면 됨.
        self.cc2d에 결과값을 저장
        # args
        maxDelay: 변수 간 correlation을 구할 delay
        ret: 0이면 값 그대로. 1이면 daily, 5이면 weekly 등
    
    self.plot: cc2d를 플로팅
        # args
        figsize = plt.figure의 figsize와 같음 len=2의 tuple


    '''
    from scipy.stats import pearsonr
    
    def __init__(self, data, y_label = 'close_price', ):
        self.y_label = y_label
        self.data = data.astype('float')
    
#     def corr(self, y, X, label, maxDelay):
#         res=[]
#         x = X.label
#         for lag in range(-1*maxDelay, maxDelay+1):
#             res.append(pearsonr(y, x))
        
    
    def CC2D(self, maxDelay = 5, ret = 1):
        '''
        ret: 0이면 가격의 correlation을 계산함. 값이 있으면 해당일 동안의 pct change를 기준으로 계산.
        '''
        res = []
        if ret == 0:
            X = self.data
        else:
            X = self.data.pct_change(ret)
        X=X.replace([np.inf, -np.inf], np.nan).dropna()
        y = X[self.y_label]
        
        labels = X.columns.to_list()
        for label in labels:
            x = X[label]
            temp = []
#             print(y, x)
            for lag in range(-1*maxDelay, maxDelay+1):
                if lag <0:
#                     print(label, lag, y[:lag].isna().sum(), x.shift(lag)[:lag].isna().sum())
                    temp.append(pearsonr(y[:lag], x.shift(lag)[:lag])[0])
                else:
#                     print(label, lag, y[lag:].isna().sum(), x.shift(lag)[lag:].isna().sum())
                    temp.append(pearsonr(y[lag:], x.shift(lag)[lag:])[0])
            res.append(temp)
        self.cc2d = np.array(res)
        return self.cc2d
        
        
    def CC3D(self, maxDelay = 50, maxTrend = 60, Yret = 1):
        '''
        ret: 0이면 가격의 correlation을 계산함. 값이 있으면 해당일 동안의 pct change를 기준으로 계산.
        '''
        from tqdm import tqdm
        
        res = []
        y = self.data[self.y_label].pct_change(Yret)
        
        for time in tqdm(range(1,maxTrend+1, 10)):
            X = self.data.pct_change(time)
            X=X.replace([np.inf, -np.inf], np.nan)
            
            time = []
            labels = X.columns.to_list()
            for label in labels:
                x = X[label]
                
                delay = []
                y_isna = y.isna()
                for lag in range(-1*maxDelay, maxDelay+1):
                    xShift = x.shift(lag)
                    x_isna = xShift.isna()
                    
                    delay.append(pearsonr(y[~(y_isna | x_isna)], xShift[~(y_isna | x_isna)])[0])
#                     if lag <0:
#     #                     print(label, lag, y[ret:lag].isna().sum(), x.shift(lag)[ret:lag].isna().sum())
#                         delay.append(pearsonr(y[ret:lag], x.shift(lag)[ret:lag])[0])
#                     else:
#     #                     print(label, lag, y[ret+lag:].isna().sum(), x.shift(lag)[ret+lag:].isna().sum())
#                         delay.append(pearsonr(y[ret+lag:], x.shift(lag)[ret+lag:])[0])
                time.append(delay)
            res.append(time)
        
        return np.array(res)


        def plot(self, figsize = (5,5)):
            cc = self.cc2d
            delay = np.shape(cc)//2

            plt.figure(figsize = figsize)
            for i in cc:
                plt.plot([i for i in range(-1*delay,delay+1)],i)
            plt.legend(tem.columns.to_list())
            plt.ylabel('Cross-Correlation with repect to close_price pct change')
            plt.xlabel('Delay')
            plt.show()
            pass

if __name__ == '__main__':
    pass