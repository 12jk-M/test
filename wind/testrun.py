# -*- coding: utf-8 -*-
"""
Created on Tue Jun 06 17:29:05 2017

@author: PRui
"""

import logging
from config.xmlpath import XML
from main.thread import threadKlineExcute
from WindPy import *
import numpy as np
from database.Mongo import ResultDBs
import threading
import datetime as dt
import time

class runJob(object):
    '''
        rt_latest : 最新成交价
        rt_time : 时间
        rt_pct_chg : 涨跌幅
        rt_chg : 涨跌
        rt_date : 日期
        rt_swing : 振幅
        rt_pre_close : 昨日收盘价
        rt_high : 最高
        rt_open : 今开
        rt_low : 最低 
        rt_vol : 成交量
        '''
    def __init__(self):
        self.xml =XML()
        self.klinepair = self.xml.getkline()[1]
        self.rlock =threading.RLock()
        self.IndexReplace = {'RT_LATEST':'Close','RT_HIGH':'High','RT_LOW':'Low','RT_PRE_CLOSE':'LastClose','RT_OPEN':'Open','RT_VOL':'Volume'}
    
    def Kline_new(self):
        w.start()
        index = ['rt_chg','rt_latest','rt_date','rt_time','rt_high','rt_low','rt_pre_close','rt_open','rt_swing','rt_pct_chg','rt_vol']
        w.wsq(self.klinepair.keys(), ','.join(index),func=self.WSQCallback)

    def WSQCallback(self,indata):
        """
        回调函数，处理数据存入数据库        
        """
        try:
            if indata.ErrorCode==0:
                #lstr= '\nIn DemoWSQCallback:\n' + str(indata);
                #print(lstr)
                Now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                transposeArr = np.array(indata.Data).T
                #code = map(lambda x:{'code':x.replace('.FX','')},indata.Codes)
                #des = map(lambda x:self.klinepair.get(x.get('code')+'.FX'),code)
                des = map(lambda x:self.klinepair.get(x),indata.Codes)
                code = map(lambda x:{'code':x.replace('.FX','').replace('.IB','')},indata.Codes)
                Fields = map(lambda x:self.IndexReplace.get(str(x),str(x)),indata.Fields)
                reDict = map(lambda x,y:dict(x,**dict(zip(Fields,y))),code,transposeArr)
                #reDict['Time'] = str(indata.Times[0])
                reDict = map(lambda x:dict(x,**{'Time':indata.Times[0].strftime('%Y-%m-%d %H:%M:%S'),'DataDate':Now}),reDict)
                reDict = map(lambda x:dict(x,**{'Amount':0,'PriceWeight':1,'type':'0'}),reDict)
                reDict = map(lambda x,y:dict(x,**{'des':y}),reDict,des)  
                
                result = map(lambda x,y:(dict(x,**{'type':y.get('type')}),y),code,reDict) 
                if self.rlock.acquire():
                    try:    
                            database = 'fes'
                            indexlist=["code",'type']                    
                            sql = ResultDBs('kline_new_wind',database,indexlist)
                            sql.save(result)
                            self.rlock.release()     
                            #print self.result
                    except Exception,e:
                        print Exception,':',e
                    self.rlock.release()
        except:
            return


if __name__ =='__main__':
    runjob=runJob()
    if runjob.xml.getrunFlag()=='1':
        runjob.Kline_new()
        while 1:
            time.sleep(1)
