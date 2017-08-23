# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 22:18:19 2016
test
@author: lywen
"""

import pandas as pd
import numpy as np
from scipy import stats
from help.help import timedelta
def gettestdata():
    
        colname = [
                      'id',                 
                     'trade_id',
                     'currency_pair',
                     'sell_currency',
                     'buy_currency',
                     'sell_amount',
                     'buy_amount',
                     'trade_date',##交易日期
                     'determined_date',##厘定日期
                     'delivery_date',##交割日期
                     'trp',##目标收益
                     'rate', ##锁定汇率
                     'determined_date_rate',
                     'type',
                        ]
        
        data = [

                ['id_6','trade_id_2','USDCNH','USD','CNH',1,6.68,'2016-09-24','2016-10-24','2016-10-25',0.05,6.68,None,2],
                ['id_7','trade_id_2','USDCNH','USD','CNH',1,6.68,'2016-10-24','2016-11-25','2016-11-25',0.05,6.68,None,2],
                ['id_8','trade_id_2','USDCNH','USD','CNH',1,6.68,'2016-11-24','2016-12-25','2016-12-25',0.05,6.68,None,2],
               ]

        data = pd.DataFrame(data,columns=colname)
        data['trade_date'] = data['trade_date'].astype(np.datetime64)
        data['determined_date'] = data['determined_date'].astype(np.datetime64)

        data['delivery_date'] = data['delivery_date'].astype(np.datetime64)

        return data.to_dict('records')
        
        
def getdayspot(Now):
    """
    获取汇率对历史记录时间序列
    code:汇率对
    mongo:数据库连接实例
    
    """
    date = pd.date_range('2016-01-01',Now)
    n = date.shape[0]
    
    st = stats.norm(loc=6.68,scale=0.1)
    value = st.rvs(n)
    spot = pd.DataFrame({'Time':date,'Close':value})
    return spot[['Time','Close']]
    
def lagdata(spot,lags=30):
    """
    spot：每天外汇收益率时间序列
    lags：时间长度
    序列指定时间长度的收益率
    """
    #spot = datafill(spot)
    
    spot['Close_%d'%lags] = np.repeat(None,lags).tolist() + spot['Close'].values[:-lags].tolist()
    spot =  spot.dropna()
    
    
    spot['Close_%d_rate'%lags] = (spot['Close'] - spot['Close_%d'%lags])/spot['Close_%d'%lags]
    
    return spot['Close_%d_rate'%lags].values/1.0/lags
    
    
def getDataFromMongo(Now):
        """
        from mongo get the currency_pairs and bank_rate
        """
        def getS():
                   st = stats.norm(loc=6.68,scale=0.1)
                   value = st.rvs(1) 
                   return value[0]
             
        S=getS()##获取实时汇率
        trfdata={}
        data = gettestdata()
        dayspot = getdayspot('2016-10-05')
        spot = lagdata(dayspot,30)
        if data !=[]:
           
           
           #mongo = mongodb()
           trfdata = {}
           for lst in data:
               ##获取厘定日汇率，未到立定日，以None填充
               
               SellRate = 1.5
               BuyRate  = 3.22
               sell_amount = float(lst['sell_amount'])
               buy_amount = float(lst['buy_amount'])
               if   lst['type']==1:
                        amount = buy_amount
               elif lst['type']==2:
                        amount = sell_amount
               elif lst['type']==3:
                  if lst['sell_currency']=='USD':
                            amount = buy_amount
                            
                  else:
                      amount = sell_amount
               else:
                    amount = None ##其他交易类型，暂时无法计算
                
               
                
               if trfdata.get(lst['trade_id']) is None:
                   lags = (lst['delivery_date'] -lst['trade_date']).days
                   trfdata[lst['trade_id']] = {'orderlist':[],
                                                'spotList':spot,##lags时间段收益时间序列
                                                 'S':S,##实时汇率
                                                 'SellRate':SellRate,##卖出货币拆解利率
                                                 'BuyRate':BuyRate,##买入货币拆解利率
                                                 'K':float(lst.get('rate')),##锁定汇率
                                                 'TIV':float(lst.get('trp')),##目标收益
                                                 'lags':lags,##每期时间间隔
                                                 'Now':Now,##损益计算时间
                                                 'amount':amount
                                                 }
               trfdata[lst['trade_id']]['orderlist'].append(lst)
        return trfdata,data