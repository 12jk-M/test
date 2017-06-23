# coding:utf-8
"""
Created on Wed Sep 21 13:33:24 2016

@author: PRui
"""

import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
from pymongo import MongoClient
from pylab import * 

def OrdinaryForward(SellRate,BuyRate,T,LockedRate,currentRate):
    if T<0:
        T=0
    SellRate_update = SellRate/360.0
    BuyRate_update =  BuyRate/360.0
    S0 = LockedRate* np.exp(-(BuyRate_update - SellRate_update  ) *T)
    St = currentRate
    return (St - S0)/currentRate

def dateRange(beginDate, endDate):
    dates = []
    t = dt.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        t = t + dt.timedelta(1)
        date = t.strftime("%Y-%m-%d")
    return dates

def getdayMax(beginDate,endDate):
        code = 'USDCNY'
        db = mongodb()
        data = db.find('kline',{'type':'5','code':code,'Time':{'$lt':endDate,'$gte':beginDate}})
        if data ==[]:
            return []
        else:
            return map(lambda x:{x.get('Time').split()[0]:x.get('High')/1.0/x.get('PriceWeight')},data)
        
        return None
    
class mongodb(object):
    """
    数据库操作
    """
    def __init__(self):
        """
        初始化数据库
        """
        self.connect()
    def connect(self):
       """
       连接数据库
       """
       try:
           client=MongoClient('10.4.32.21',27017)
           db=client.fes
           self.__conn=db
       except:
           self.__conn=None
    def find(self,collection_name,condition):
        """
        向已有的collection查询数据操作，返回查询数据列表
        collection_name：数据库文档名称
        condition：查询条件        
        示例：find('test_collection',{'a':'b'})
        """
        if collection_name in self.__conn.collection_names():        
            collection = self.__conn[collection_name]
            lst=list()
            for item in collection.find(condition):
                lst.append(item)
            if len(lst)>0:
                return lst
            else:
                print 'no records'
        else:
            print 'collection does not exist'

#SellRate,BuyRate,deliverydate,LockedRate = 0.022380,0.004332,'2016-12-01',6.701
SellRate,BuyRate,deliverydate,LockedRate = 0.004332,0.022380,'2016-12-01',6.701

dayRate = getdayMax('2016-10-1','2016-11-1')
now = map(lambda x:x.keys()[0],dayRate)
currentRate = map(lambda x,y:x[y],dayRate,now)
#now = dateRange("2016-01-01", "2017-01-01")
deliverydate = dt.datetime.strptime(deliverydate,'%Y-%m-%d')
x = map(lambda t:(deliverydate - dt.datetime.strptime(t,'%Y-%m-%d')).total_seconds()/1.0/60/60/24,now)
y = map(lambda t,k:OrdinaryForward(SellRate,BuyRate,t,LockedRate,k),x,currentRate)
T = map(lambda t:dt.datetime.strptime(t,'%Y-%m-%d'),now)
pylab.plot_date(pylab.date2num(T),y, linestyle='-')  
pylab.show()