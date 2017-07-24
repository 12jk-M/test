# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 15:11:13 2016
config
@author: lywen
"""
import sys
sys.path.append('..')
from database.database import mongodb,postgersql
from database.mongodb import BankRate
import pandas as pd
import numpy as np

from help.help import strTostr,getNow
from help.help import getcurrency,getlocalflag
import datetime as dt
#import traceback


def getSpot(code):
    """
    获取汇率对的实时汇率,用于把本金转化为美元
    """
    if code.strip().upper()=='USD':
        return 1.0
    else:
        code1 = 'USD'+code.strip().upper()
        code2 = code.strip().upper() + 'USD'
        mongo = mongodb()
        data = mongo.select('kline_new',{'type':'0','code':{'$in':[code1,code2]}})
        mongo.close()
        
        if data!=[]:
            data = data[0]
            if data['code'][:3]=='USD':
                return 1.0/(data['Close']/data['PriceWeight'])
            else:
                return data['Close']/1.0/data['PriceWeight']
            
            
        else:
             return None   
             
             
def getCNHCNY(_type):
    """
    获取美元对人民币的汇率
    _type.upper()=='CNH' ###离岸人民币
    _type.upper()=='CNY' ###在岸人民币
       
    """
    

    code = 'USD'+_type.strip().upper()
    mongo = mongodb()
    data = mongo.select('kline_new',{'type':'0','code':code})
    mongo.close()
        
    if data!=[]:
            data = data[0]
            return  data['Close']/1.0/data['PriceWeight']
    else:
            return None
         
            
def gethistory(Currency):
    """获取汇率对的历史收盘价"""
    data =[]
    try:
       mongo = mongodb()
       data = mongo.select('kline',{'type':'5','code':Currency})
       mongo.close()
    except:
        pass
    if data!=[]:
        data = pd.DataFrame(data)
        data = data[['Time','Close','PriceWeight']]
        data['Close'] = data['Close']/data['PriceWeight']
        #data['High'] = data['High']/data['PriceWeight']
        #data['Open'] = data['Open']/data['PriceWeight']
        #data['LastClose'] = data['LastClose']/data['PriceWeight']
        #data['Low'] = data['Low']/data['PriceWeight']
        
        data['Time'] = data['Time'].map(lambda x:strTostr(x,'%Y-%m-%d %H:%M:%S','%Y-%m-%d'))
        data = data[['Time','Close']]
        
    return data


def timedelta(s,day,format='%Y-%m-%d'):
    """在当前日期上加减天数，返回对应的日期"""
    try:
      return (dt.datetime.strptime(s,format)+dt.timedelta(day)).strftime(format)
    except:
       return None
    
def dateseris(mindate,maxdate):
        """生成日期序列"""
        date = timedelta(mindate,0)
        series = []
        while maxdate>=date:
            series.append(date)

            date = timedelta(date,1)

        return series
def datafill(spots):
    """
    填充无交易日期的汇率
    以上一个交易日的的收盘价进行填充
    """
    Now = getNow(format='%Y-%m-%d')
    spot = spots.copy()
    spot = pd.merge(pd.DataFrame(dateseris(spot['Time'].min(),Now),columns=['Time']),spot[['Time','Close']],on=['Time'],how='left' )
    spot = spot.sort_values('Time')
    for i in range(spot.shape[0]):
        if spot['Close'].values[i].__str__()=='nan':
            spot['Close'].values[i] = spot['Close'].values[i-1]
    return spot

def lagdata(spots,lags=[30,60,180]):
    """
    spot：每天外汇收益率时间序列
    lags：时间长度
    序列指定时间长度的收益率
    """
    spot = datafill(spots)
    #for lag in lags:
    lag = lags[0]  
    spot['Close_%d'%lag] = np.repeat(None,lag).tolist() + spot['Close'].values[:-lag].tolist()
    spot['Close_%d_rate'%lag] = (spot['Close'] - spot['Close_%d'%lag])/spot['Close_%d'%lag]
        ##加入时间衰减因子
        #spot['Close_%d'%lag] = spot['Close_%d'%lag]*np.exp(-0.01*lag)
    spot =  spot.dropna()
    #return spot[['Time','Close_%d_rate'%lag]]
    return spot[['Time','Close_%d_rate'%lag]]


def getuniondata(data):
    
    global ERROR
    


    uniondata = None
    for lst,index in zip(data[['currencyPair','deliverDate','tradeDate']].values,data.index.values):
        currency = lst[0]
        lag = (lst[1] -lst[2]).days

        spots = gethistory(currency)
        
        try:
           
    
            spots = lagdata(spots,[lag])
            spots.columns = ['Time',currency+'_'+str(lag)+'_'+str(index)]
            if uniondata is None:
                    uniondata = spots
            else:
                    uniondata = pd.merge(uniondata,spots,on=['Time'])
        except:
             ERROR+= 'The currencypair:%s is not found!\n'%currency
                

    return uniondata

def getdatarange(data):
    """
    未来每天的每远期的本金
    """
    dataframe = None
    data = data.sort_values('tradeDate')
    for lst,index in zip(data.to_dict('records'),data.index.values):
        Currency = lst['currencyPair']
        TradeDate = lst['tradeDate'].strftime('%Y-%m-%d')
        DeliverDate = lst['deliverDate'].strftime('%Y-%m-%d')
        #weight = lst['weight']
        weight = lst['CNYMoney']
        temp = pd.DataFrame(dateseris(TradeDate,DeliverDate),columns=['Time'])
        temp[Currency+'_'+str(index)] = weight
        if dataframe is None:
            dataframe = temp
        else:
            dataframe = pd.merge(dataframe,temp,on='Time',how='outer')
    dataframe = dataframe.fillna(0)
    

    return dataframe

def repeat(data,times):
    """
    对数据重复times
    """
    tempdata = data.copy()
    if times==1:
        return tempdata
    for i in range(times-1):
        tempdata = tempdata.append(data)
    tempdata.index =range(len(tempdata))
    return tempdata
    
    

                

def getR(Currency):
    """
     获取货币对的最新汇率
    """
    error = None
    #sendData=({'errMsg':error,'errCode':0,'data':None})
    try:
        mongo = mongodb()
        R = mongo.select('kline_new',{'type':'0','code':{'$in':Currency}})
        R = reduce(lambda x,y:dict(x,**y),map(lambda lst:{lst['code']:lst['Close']/1.0/lst['PriceWeight']},R))
        mongo.close()
    except:
        R = None
    if R is None:
        ## 判断是否找到数据
       sendData = {'errMsg':'{} not fund!\n'.format(Currency),'errCode':4,'data':None}##汇率缺失
       return sendData
    if len(R)!=len(Currency):
        error = 'The currencyPair not fund:\n'
        for currency in Currency:
            if currency not in R.keys():
                if error  is None:
                    error = "%s not fund!\n"%currency
                else:
                    error += "%s not fund!\n"%currency
                 
        sendData = {'errMsg':error,'errCode':4,'data':None}##汇率缺失
    else:
        sendData={'errMsg':None,'errCode':0,'data':R}
    return sendData


def getmodeldata(currency):
    """获取货币对的历史汇率数据"""
    #print currency
    
    data = gethistory(currency)
    if type(data) is not list:
        
        data = datafill(data)
        logData = lagdata(data,[1])
        logData.columns = ['Time',currency]
        sendData={'errMsg':None,'errCode':0,'data':logData}
    else:
        sendData={'errMsg':"%s not fund!\n"%currency,'errCode':4,'data':None}
    
    
    return sendData


def gendataframe(Currency):
    """拼接所有汇率数据"""
    data = None
    for currency in Currency:
        sendData = getmodeldata(currency)
        temp = sendData['data']
        if temp is None:
            return sendData
        if data is None:
            data = temp.copy()
        else:
            data = pd.merge(data, temp, on=['Time'])
    return {'errMsg':None,'errCode':0,'data':data}



def getsimulationdata(Currency):
    """
    获取模拟所需的实时数据及汇率的历史数据
    """
    #Currency = 
    spot = getR(np.unique(Currency).tolist())
    if spot['data'] is None:
        return spot
    else:
        spot = spot['data']
        R = []
        for currency in Currency :
            R.append(spot[currency])
        R = np.array(R)
        historyData =gendataframe(np.unique(Currency).tolist())##货币对历史数据
        if historyData['data'] is None:
            return historyData
        else:
            historyData = historyData['data']##dataframe
            historyData = historyData[Currency].values
        return {'errMsg':None,'errCode':0,'data':{'S':R,'Rate':historyData}}## Rate 所有汇率对的历史数据、R实时汇率

from random import choice
def simulationspot(S,Rate,days):
    """
    模拟未来days天的汇率收盘价
    S:当前即期价格
    rateList:收益率列表
    days:模拟未来天数
    """
    
    Spot = None
    newRate = Rate.tolist()
    for i in range(days):
         R = choice(Rate)## 模拟当天的收益率
         S = S*(1+R)
         
         if  Spot is None :
             Spot = np.array([S])
         
            
            
         else:
            Spot = np.append(Spot,[S],axis=0)
    return Spot 


def getbanrate(x):
    r = BankRate(getcurrency(x ),'').getMax()
    if r ==[]:
        return None
    else:
        r = r[0]
        return r['rate']/100.0
    
    
def getdataforsimulation(sendData):
    """
    通过从接口获取数据，返回待模拟的数据
    """
    ##modelnames   =  ['market','type','lockRate','sell','tradeDate','deliverDate','currencyPair','sellCurrency']
    if sendData['data'] is None:
        return sendData
    else:
        data = sendData['data']
        global ERROR
        ERROR = ''
        #data['code'] = data['moneyCurrency']
        #data['sellCurrency'] = data['moneyCurrency']
        
        ##买入货币币种
        data['buyCurrency'] = map(lambda x:x[1][3:] if x[0]==x[1][:3] else x[1][:3],  data[['sellCurrency','currencyPair']].values)
        
        ##卖出货币金额
        data['buyMoney'] = map(lambda x: (x[0]*x[3]) if x[1]==x[2][:3] else (x[0]/1.0/x[3]),  data[['sellMoney','sellCurrency','currencyPair','lockRate']].values)
        ##本金币种 结汇2：卖出币种，购汇1：买入币种，互换3：非美元币种
        data['moneyCurrency'] = map(lambda x:
                                            x['buyCurrency']  if x['type']=='1' ##购汇
                                            else ( x['sellCurrency'] if x['type']=='2' ##结汇
                                                                     else##互换
                                                                         ( x['buyCurrency'] if x['sellCurrency']=='USD' else x['sellCurrency'])
                                            )
                   ,data[['buyCurrency','sellCurrency','type']].to_dict('records'))
        ##本金
        data['money'] = map(lambda x:
                                            x['buyMoney']  if x['type']=='1' ##购汇
                                            else ( x['sellMoney'] if x['type']=='2' ##结汇
                                                                     else##互换
                                                                         ( x['buyMoney'] if x['sellCurrency']=='USD' else x['sellMoney'])
                                            )
                   ,data[['buyCurrency','sellCurrency','type','sellMoney','buyMoney']].to_dict('records'))
        ##转换为美元金额
        data['USDMoney'] = data['moneyCurrency'].map(lambda x:getSpot(x))
        data['USDMoney'] = data['USDMoney'] *data['money'] 

        
        ##汇率对之间的拆借利率
        data['sellRate'] =data['currencyPair'].map(lambda x:getbanrate(x[:3]))
        data['buyRate'] =data['currencyPair'].map(lambda x:getbanrate(x[3:]))

        
        if data['USDMoney'].isnull().sum()>0:
                ERROR+="'moneyCurrency' type is error!\n"
                sendData.update({'errMsg':ERROR,'errCode':5,'data':None})
                return sendData
                               
        ## 拆借利率差        
        data['r'] = data['buyRate'] - data['sellRate']
        
        ##美元对人民币汇率         
        data['CNY'] = data['market'].map(lambda x:getCNHCNY(x))
        ##人民币金额
        data['CNYMoney'] = data['USDMoney']*data['CNY']
        Now = getNow('%Y-%m-%d')
        ## 未来每笔产品的人民币，当天该笔交易未开始或者已经结束，其金额为0，后面算损益也为0
        datarange = getdatarange(data)
        
        datarange = datarange[datarange['Time']>=Now]
        datarange.index = datarange['Time']

        datarange = datarange.drop('Time',axis=1)
        
        Currency =map(lambda x:x.split('_')[0],datarange.columns)
        index = map(lambda x:int(x.split('_')[1]),datarange.columns)
        
        ##获取模拟所需的实时数据及汇率的历史数据
        sendData = getsimulationdata(Currency)
        Rate = sendData['data']['Rate']##历时收益率
        S = sendData['data']['S']##即期价格
        
        days = len(datarange)##最后一笔交割日期截止当天剩余时间
        lockRate = data['lockRate'].values[index]
        ##剩余交割期
        T = np.array(map(lambda x:[x],range(1,days+1))[::-1])
        r = data['r'].values[index]##拆借利率差
        ##损益方向
        lostFalg = map(lambda x:getlocalflag(x['type'],x['sellCurrency'],x['currencyPair']),data[['type','sellCurrency','currencyPair']].to_dict('records'))
        sendData.update({'data':{'S':S,
                                 'Rate':Rate,
                                 'days':days,
                                 'T':T,
                                 'lockRate':lockRate,##锁定汇率
                                 'lostFalg':lostFalg,
                                 'datarange':datarange.values,
                                 'r':r,##拆借利率
                                 'type':data['type'],##交易类型：结汇，购汇、互换
                                 'data':data
                                  }})
        return sendData
    
    
def simulationvar(sendData,times):
    """
    模拟损益
    times:模拟次数
    """
    if sendData['data'] is None:
        return sendData
    else:
        data = sendData['data']
        lockRate = data['lockRate']##锁定汇率
        S = data['S']##实时汇率
        days = data['days']##剩余交割时间
        T = data['T']##
        r = data['r']##利率
        Rate = data['Rate']##历史每天的收益率
        #tUsdCurrency = data['tUsdCurrency']##汇率对USD的转换汇率
        datarange = data['datarange']##各种远期外汇每天所占的比例
        lostFalg = data['lostFalg']#损益方向
        varData = []## 损益
        for i in range(times):
            temp = simlationtime(lockRate,S,Rate,days,T,r,datarange,lostFalg)
            varData.extend(temp.tolist())
        
        #return {"errCode":0, "data":{'varData':varData,'money':datarange.sum(axis=1)},'errMsg':None}
        return {"errCode":0, "data":{'varData':varData,'values':datarange[0].sum()},'errMsg':None}
def simlationtime(lockRate,S,Rate,days,T,r,datarange,lostFalg):
    return (lostFalg*(simulationspot(S,Rate,days)  - lockRate*np.exp(-T*r/360.0))/lockRate*datarange).sum(axis=1)

def modelrun(sendData):
    times = 1000
    sendData = getdataforsimulation(sendData)
    sendData = simulationvar(sendData,times)
    return sendData

def getdailysum(com_code):
    data =[]
    try:
       postgre = postgersql()
       if len(com_code) == 0:
           data = postgre.view('''select sum(gal) from frs_t_daily_position group by date''',['sum'])
       elif len(com_code) == 1:
           where = com_code[0]
           data = postgre.view('''select sum(gal) from frs_t_daily_position where com_code in ('%s') group by date'''%where,['sum'])
       else:
           where = str(tuple(map(str,com_code)))
           data = postgre.view('''select sum(gal) from frs_t_daily_position where com_code in %s group by date'''%where,['sum'])
       postgre.close()
    except:
        traceback.print_exc()
        return None
    if data!=[]:
        data = map(lambda x:x.get('sum'),data)
        
    return data
        
