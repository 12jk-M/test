# -*- coding: utf-8 -*-
"""
Created on Wed Dec 14 11:13:19 2016

@author: PRui
"""
import pandas as pd
from help.help import timedelta
from database.mongodb import  RateExchange
from database.database import mongodb## mongo connect

def simulate(code,start_time,end_time):    
    m = mongodb()
    start_time = start_time+' 00:00:00'
    end_time = end_time+' 00:00:00'
    data = m.select('kline',{'code':code,'type':'5','Time':{'$gte':start_time,'$lte':end_time}})
    if data ==[]:
        return []
    else:
        rate = map(lambda x:x.get('Close')/1.0/x.get('PriceWeight'),data)
    

simulate('USDCNY','2016-11-01','2016-11-30')