# -*- coding: utf-8 -*-

import sys
sys.path.append('..')

from model import modelrun,getdailysum
import numpy as np
import json

import pandas as pd

def cleandata(data):
    """对传入参数进行异常处理"""
  
    sendData = None
    try:
              data = json.loads(data)
              if type(data) is not dict or 'param' not in data or 'data' not in data:
                    error='The data format is error:{}'.format(json.dumps(data))##判断传入的数据格式是否正确
                    sendData = {"errCode":3, "data":None,'errMsg':error}
             
    except:
               error='The data format is error:{}'.format(data)
               sendData =  {"errCode":1, "data":None,'errMsg':error}
    if sendData is None:
        return {"errCode":0, "data":data,'errMsg':None}
    else:
        return sendData
        
def datamap(sendData):
    """
    请求数据与模型字段映射
    """
    requestnames =  ['market','type','lockRate','sell','tradeDate','deliverDate','currencyPair','sellCurrency']
    modelnames   =  ['market','type','lockRate','sellMoney','tradeDate','deliverDate','currencyPair','sellCurrency']
    if  sendData['errCode']==0:
         sendData['data'] =  sendData['data'][requestnames]
         sendData['data'].columns = modelnames
    return sendData
    
        

def cleancloumn(sendData):
    """
    对数据进行清洗
    判断所需字段是否存在
    数据类型是否正确
    """
    
    if sendData['errCode']==0:
        data = pd.DataFrame(sendData['data'])
        ###判断数据是否有重复
        if data.drop_duplicates(keep='first').shape!=data.shape:
            ERROR ="The data is duplicates:\n{},try upload the data again!\n".format(data)
            sendData.update({'errMsg':ERROR,'errCode':1,'data':None})
        
        
        requestnames =    ['market','type','lockRate','sell','tradeDate','deliverDate','currencyPair','sellCurrency']
        ##判断所需的字段是否在所传的数据中
        for colname in requestnames:
            if colname not in data.columns:
                ERROR ="The columns:{} not have,try upload the data again!\n".format(colname)
                sendData.update({'errMsg':ERROR,'errCode':2,'data':None})
        
        ##判断锁定汇率字段数据类型是否正确
        try:

           data['lockRate']= data['lockRate'].astype(np.float64)
        except:
            sendData.update({'errMsg':'the columns:"lockRate" type is error!','errCode':5,'data':None})
        
        ##判断锁定汇率字段数据类型是否正确
        try:

            data['sell']= data['sell'].astype(np.float64)

        except:
             sendData.update({'errMsg':'the columns:"money" type is error!','errCode':5,'data':None})
        if  sendData['errCode']==0:                    
            if data['sell'].min()<0.0 or data['sell'].isnull().sum()>0:
                sendData.update( {'errMsg':'the columns:"sell" is <0 or is null!','errCode':5,'data':None})
                             
        try:

            data['tradeDate'] = data['tradeDate'].astype(np.datetime64)
        except:
            sendData.update( {'errMsg':'the columns:"tradeDate" type is error!','errCode':5,'data':None})

        try:

            data['deliverDate'] = data['deliverDate'].astype(np.datetime64)
        except:
                             
            sendData.update( {'errMsg':'the columns:"deliverDate" type is error!','errCode':5,'data':None} )
                             
        if sendData['errCode']==0:
            sendData.update({'data':data})
    return sendData
       



   

def request(data):
    sendData = cleandata(data)##判断数据格式是否错误
    if sendData['errCode']==0:
        if type(sendData['data']['data']) is not list:
                error = 'The data format is error:{}'.format(sendData['data'])##判断传入的数据格式是否正确
                sendData.update({"errCode":3, "data":None,'errMsg':error})
                return json.dumps(sendData)
            
        if sendData['data']['param'] ==0:## 风险工具中的Var
           sendData['data'] = sendData['data']['data']
           
                
           sendData =  cleancloumn(sendData)##字段清洗
           sendData =  datamap(sendData)##字段映射
           if sendData['errCode']==0:
              sendData = modelrun(sendData)##获取组合结构性产品的收益序列                  
              if sendData['errCode']==0:##交易辅助中的var
                 result = sendData['data'].get('varData')
                 price  = sendData['data'].get('values')##资产总值
                 
                 value = histogram(result)
                 varPrice = value['frsVar']['x']
                 varPrice = round(varPrice,2)
                 value['frsVar']['x'] =varPrice
                 frsDes = u"当前远期产品的人民币价值为:%s  "%moneyTotype(price)
                 frsDes+=u"基于VaR模型(95%%的置信区间)，未来的人民币价值可能变为:%s  "%moneyTotype(price+varPrice)
                 if varPrice<0:
                     
                     frsDes+=u"公司面临的损失为:%s"%moneyTotype(-varPrice)
                 else:
                      frsDes+=u"公司将获得的收益:%s"%moneyTotype(varPrice)
                      
                 value.update({'frsDes':frsDes})
                 sendData.update({'data':value})
           
        elif sendData['data']['param'] ==1:## 
             #result = sendData['data']['data']
             result = getdailysum(sendData['data']['data'])
             try:
                result = np.float64(result)
                result = result.tolist()
             except:
                error = 'The data format is error:{}'.format(sendData['data'])##判断传入的数据格式是否正确
                sendData.update({"errCode":3, "data":None,'errMsg':error}) 
                return json.dumps(sendData)
             value = histogram(result)
             varPrice = value['frsVar']['x']
             
             frsDes=u"基于VaR模型(95％的置信区间),"
             if varPrice<0:
                     
                      frsDes+=u"公司可能面临的人民币损失为:%s;"%moneyTotype(-varPrice)
             else:
                      frsDes+=u"公司将获得的收益:%s;"%moneyTotype(varPrice)
                    
             if np.mean(result)<0:
                     
                      frsDes+=u"公司的平均人民币损益为:%s"%moneyTotype(-np.mean(result))
             else:
                      frsDes+=u"公司的平均人民币收益为:%s"% moneyTotype(np.mean(result))
             
             value.update({'frsDes':frsDes})
             sendData.update({'data':value})
        else:
            error = 'The data format is error:{}'.format(sendData['data'])##判断传入的数据格式是否正确
            sendData.update({"errCode":3, "data":None,'errMsg':error}) 
        
        
    return json.dumps(sendData)
    
##概率密度曲线
def histogram(result):
        y_value,x_value = np.histogram(result,bins='scott',density=True)
        space = (x_value[1]-x_value[0])
        x_value = x_value[:-1]
        result = sorted(result)
        alpha = 0.05
        var = {"x":result[int(alpha*(len(result)-1))],"y":alpha/space}
        value = {"x":x_value.tolist(),"y":y_value.tolist()}
        value = {'frsVar':var,'frsValue':value}
        return value

    
def moneyTotype(value):
    try:
        if np.abs(value/10000/10000)>1:
            return u'{}亿元'.format(round(value/10000.0/10000.0,2))
        elif np.abs(value/10000)>1:
            return  u'{}万元'.format(round(value/10000.0,2))
        else:
            return  u'{}元'.format(round(value,2))
    except:
        return u'0元'
        
    
    
