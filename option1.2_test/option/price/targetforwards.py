# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 14:32:34 2016
Target Redemption Forward
@author: lywen

"""
from job import option
from help.help import getNow,getRate,strTodate,dateTostr,strTostr
from help.help import getcurrentbankrate,gethistorybankrate,gethistoryrate
from config.postgres  import  table_frs_option,table_write_tmp
from database.mongodb import  RateExchange
#from database.mongodb import  BankRate
from database.database import postgersql,mongodb
#from numpy import float64
from main.targetforward import  TargetRedemptionForward
import pandas as pd
import numpy as np

from help.help import timedelta
from config.config import getrate
from log.logs import logs
testlag = False##是否调用测试数据
Flag = True

class TargetRedemptionForwards(object):
    
    def __init__(self,Lock):
        self.table = table_frs_option
        self.querytable = table_write_tmp
        self.Lock = Lock
        if Flag:
            self.do_all()
        else:
            self.Now = '2016-12-01 16:30:00'
            TargetRedemptionForwards_oneday(self.Lock,self.Now)
    
    def do_all(self):
        """
        执行所有历史数据
        """
        self.getTime()        
        if self.begintime and self.endtime is not None:
            Now = self.begintime
            while (Now>=self.begintime and Now<=self.endtime):
                self.Now = strTostr(Now)
                TargetRedemptionForwards_oneday(self.Lock,self.Now)
                Now = timedelta(Now,1)
                
    def getTime(self):
        """
        获取最小起始日期和min{最大交割日日期,当前日期}
        """
        post = postgersql()
        colname = ['trade_date','delivery_date','currency_pair','dates']
        sql = """select op.trade_date,op.delivery_date,op.currency_pair,
                case
                when t.dates<op.delivery_date then date(t.dates)+1
                when t.dates=op.delivery_date then current_date
                end as dates
                from %s op
                left join 
                (select max(pt.date) as dates,pt.der_id from %s pt
                group by pt.der_id) t
                on op.trade_type||'_'||op.id = t.der_id
                """%(self.table,self.querytable)
        time = post.view(sql,colname)
        if time==[] or time==None:
            self.begintime = None
            self.endtime = None
        else:
            time = filter(lambda x:x['currency_pair'] in getrate() and dateTostr(x['delivery_date'])>=getrate()[x['currency_pair']],time)
            #print len(time)
            for lst in time:
                if lst['dates'] is None:
                    lst['dates'] = dateTostr(lst['trade_date']) if dateTostr(lst['trade_date'])>getrate()[lst['currency_pair']] else getrate()[lst['currency_pair']]
                else:
                    lst['dates'] = dateTostr(lst['dates'])
            begintime = time[0]['dates'] if len(time)==1 else min(map(lambda x:x['dates'],time))
            self.begintime = begintime if getNow('%Y-%m-%d')>=begintime else None
            endtime = dateTostr(time[0]['delivery_date'] if len(time)==1 else max(map(lambda x:x['delivery_date'],time)))
            self.endtime = endtime if getNow('%Y-%m-%d')>endtime else timedelta(getNow('%Y-%m-%d'),-1)

class TargetRedemptionForwards_oneday(option):
    """
    目标可赎回式远期
    """

    def __init__(self,Lock,Now):
        #Now = getNow()
        #Now = strTodate(Now,'%Y-%m-%d %H:%M:%S')
        option.__init__(self)
       # self.delta  =delta
        self.table = table_frs_option
        self.writetable = table_write_tmp
        self.Lock = Lock
        #self.Now = Now
       
        if testlag:##调用测试数据
            from test.test import getDataFromMongo
            
#            self.trfdata, self.data = getDataFromMongo(Now)
            self.cumputeLost()
            #self.cumputeLost()
            
        else:
            
            self.mongo = mongodb()
            self.Now = strTodate(Now,'%Y-%m-%d %H:%M:%S')
            self.getDataFromPostgres()##从post提取数据
                    #self.getDataFromMongo()##从mongo提取数据并更新损益
                    #print '期权类型','ID','货币对','成交日期','交割日','损益'
                    #self.cumputeLost()
        
    def getDataFromMongo(self):
        """
        from mongo get the currency_pairs and bank_rate
        """
        trfdata = {}
        if self.data !=[]:
           
           S = {}##货币对最新汇率
           spot  = {}
           Time = dateTostr(self.Now)
           #mongo = mongodb()
           
           for lst in self.data:
               ##获取厘定日汇率，未到立定日，以None填充
               
               code = lst.get('currency_pair')
               date = lst.get('determined_date').strftime('%Y-%m-%d')
               #determined_date_rate = getkline(code,date,self.mongo)##获取厘定日汇率
               if Time<date:
                   determined_date_rate = None
               else:
                   determined_date_rate = getkline(code,date,self.mongo)##获取厘定日汇率
            #SetRate = RateExchange(currency_pair).getdayMax(Setdate)##厘定日汇率
               lst.update({'determined_date_rate':determined_date_rate})
               
               sell_currency = lst['currency_pair'][:3]
               buy_currency  = lst['currency_pair'][3:]
               
               #ratetype = getRate((lst['delivery_date'] -lst['trade_date']).days)
               ratetype ='12月'
            
               #SellRate,BuyRate = getcurrentbankrate(sell_currency,buy_currency,ratetype)
               SellRate,BuyRate = gethistorybankrate(sell_currency,buy_currency,ratetype,Time)##获取银行历史拆借利率
                   
               if S.get(code) is None:
                   for i in range(100):
                       RE = RateExchange(code).getdayMax(Time)
                       if RE is not None and RE !=[]:
                           S[code] = RE
                           break
                       Time = timedelta(Time,-1) 
                   #RE = RateExchange(code).getMax()
                   #if RE is not None and RE !=[]:
                      #S[code] =  RE[0].get('Close')##获取实时汇率
               #if sell_currency+buy_currency!=code:
               #    BuyRate,SellRate=SellRate,BuyRate##判断卖出买入货币与汇率对的对应情况
                   
               if spot.get(code) is None:
                   dayspot = getdayspot(code,self.mongo)
                   dayspot = datafill(dayspot)
                   spot[code] = dayspot
               
               sell_amount = np.float64(lst['sell_amount'])
               buy_amount = np.float64(lst['buy_amount'])
               
               
                     
               if   lst['type']==u'1':
                        amount = buy_amount
                        currency = lst['buy_currency']
               elif lst['type']==u'2':
                        amount = sell_amount
                        currency = lst['sell_currency']
               elif lst['type']==u'3':
                  if lst['sell_currency']=='USD':
                            amount = buy_amount
                            currency = lst['buy_currency']
                            
                  else:
                      amount = sell_amount
                      currency = lst['sell_currency']
               else:
                    amount = None ##其他交易类型，暂时无法计算
                    currency = None
                
               if trfdata.get(lst['trade_id']) is None:
                   try:
                       lags = getLags(self.data,lst['trade_id'])
                   except:
                       continue
                   trfdata[lst['trade_id']] = {'orderlist':[],
                                                'spotList':lagdata(spot[code],lags),##lags时间段收益时间序列
                                                 'S':S[code],##实时汇率
                                                 'SellRate':SellRate,##卖出货币拆解利率
                                                 'BuyRate':BuyRate,##买入货币拆解利率
                                                 'K':float(lst.get('rate')),##锁定汇率
                                                 'TIV':float(lst.get('trp')),##目标收益
                                                 'lags':lags,##每期时间间隔
                                                 'Now':self.Now,##损益计算时间
                                                 'amount':amount,##本金
                                                 'consign_entity':lst['consign_entity'],
                                                 'trade_type':lst['trade_type'],
                                                 'id':lst['id'],
                                                 'currency':currency
                                                 }
               trfdata[lst['trade_id']]['orderlist'].append(lst)
        self.trfdata = trfdata
                
    
    def getDataFromPostgres(self):
        """
        获取结构性产品的订单数据
        """
        post = postgersql()
        colname = [
              'id',                 
             'trade_id',
             'currency_pair',
             'sell_currency',
             'buy_currency',
             'sell_amount',
             'buy_amount',
             'trade_date',
             'determined_date',
             'delivery_date',
             'trp',
             'rate',
             'type',
             'consign_entity',
             'trade_type'
                ]
        #wherestring = None
        #orderby = "order by trade_id, delivery_date"
        #Now = getNow('%Y-%m-%d')
        Now = dateTostr(self.Now)
        sql = """select %s from %s t join 
                  (select trade_id from %s group by trade_id  having max(delivery_date)>=date'%s' ) b
                  on t.trade_id=b.trade_id
                  order by t.trade_id, t.delivery_date
                  """%(','.join(map(lambda x:'t.'+x,colname)),
                       self.table,
                       self.table,
                       Now
                       )##过滤已交割完成的结构产品
        #self.data = post.view(sql,colname)
        optiondata = post.view(sql,colname)
        optiondata = filter(lambda x:x['currency_pair'] in getrate() and Now>=getrate()[x['currency_pair']],optiondata)
        colname2 = ['der_id']
        wherestring2 = """date='{}'""".format(Now)
        savedata = map(lambda x:x['der_id'],post.select(self.writetable ,colname2,wherestring2))
        data = []
        for lst in optiondata:
            if lst['trade_type']+'_'+str(lst['id']) not in savedata:
                data.append(lst)
        if data is not None and data !=[]:
            self.data = data
            self.getDataFromMongo()##从mongo提取数据并更新损益
            self.cumputeLost()
        #self.data = post.select(self.table,colname,wherestring,orderby)
        
        
    def  cumputeLost(self):
        """
        
        """
        Time = dateTostr(self.Now)
        Lost =[]
        self.forwarddict = []
        for trade_id in self.trfdata:            
            spotList  = self.trfdata[trade_id]['spotList']
            orderlist = self.trfdata[trade_id]['orderlist']
            S         = self.trfdata[trade_id]['S']
            K         = self.trfdata[trade_id]['K']
            SellRate  = self.trfdata[trade_id]['SellRate']
            BuyRate   = self.trfdata[trade_id]['BuyRate']
            Now       = self.trfdata[trade_id]['Now']
            
            lags      = self.trfdata[trade_id]['lags']
            TIV       = self.trfdata[trade_id]['TIV']
            amount    = self.trfdata[trade_id]['amount']
            #print 'knockoptions',lst['id'],currency_pair,dateTostr(lst['trade_date']),deliverydate,
            TRF = TargetRedemptionForward(spotList,orderlist,S,K,SellRate,BuyRate,lags,Now,TIV)##计算损益值
            gal = 0
            for lst in   TRF:
                lst['price'] = (lst['price']*amount) if lst['price'] is not None else lst['price']
                gal += lst['price']
            if TRF is not None:
                
               Lost.extend(TRF)
            """
            'date','com_code','der_id','cap_currency','cap_amount','gal','exrate','data_date':
            日期，公司代码，衍生品编号，本金币种，本金金额，汇兑损益，当天的本金/人民币汇率，数据生成日期
            """           
            forwarddict= {}
            forwarddict['date'] = Time
            forwarddict['com_code'] = self.trfdata[trade_id]['consign_entity']
            forwarddict['der_id'] = self.trfdata[trade_id]['trade_type']+'_'+str(self.trfdata[trade_id]['id'])
            forwarddict['gal'] = gal
            forwarddict['cap_amount'] = amount
            forwarddict['cap_currency'] = self.trfdata[trade_id]['currency']
            currencypair = forwarddict['cap_currency']+'CNY'
            forwarddict['exrate'] = gethistoryrate([currencypair],Time)[currencypair] if currencypair!='CNYCNY' else 1.0
            forwarddict['data_date'] = getNow('%Y-%m-%d %H:%M:%S') 
            logs('compute','targetforwards','cumputeLost',forwarddict,infoFalg=True)
            self.forwarddict.append(forwarddict)
        self.updateData()  
            
        #self.updateDataToPostgres(Lost)
        #self.Lost =Lost


    def updateData(self):
        """
        全量更新
        'date','com_code','der_id','cap_currency','cap_amount','gal','exrate','data_date':
        日期，公司代码，衍生品编号，本金币种，本金金额，汇兑损益，当天的本金/人民币汇率，数据生成日期
        """
        from database.database import postgersql
        column = ['date','com_code','der_id','cap_currency','cap_amount','gal','exrate','data_date']
        columnname = '(date,com_code,der_id,cap_currency,cap_amount,gal,exrate,data_date)'
        insertdata = map(lambda x:map(lambda y:x[y],column),self.forwarddict)
        self.Lock.acquire()
        post = postgersql()
        #post.deltetable(self.writetable)
        post.insert(self.writetable,columnname,insertdata)
        post.close()
        self.Lock.release()

##找到合适的lags 
def getLags(data,trade_id):
    temp =[]
    for lst in data:
        if lst['trade_id']==trade_id:
            temp.append(lst['determined_date'])
            
    return   np.diff(temp)[-1].days
    
    


def getkline(code,date,mongo):
    """
    code：汇率对
    date：日期
    获取指定日期指定汇率对的汇率
    """
   
    date = date+" 00:00:00"
    reslut =  mongo.select('kline',{'type':'5','code':code,'Time':date})
    
    if reslut!=[]:
        
        return reslut[0].get('High') /1.0 / reslut[0].get('PriceWeight')
    else:
        return None
        
        
        

def getdayspot(code,mongo):
    """
    获取汇率对历史记录时间序列
    code:汇率对
    mongo:数据库连接实例
    
    """
    spot = mongo.select('kline',{'type':'5','code':code})
    
    spot = pd.DataFrame(spot)
    spot = spot[['Time','Close','PriceWeight']]
    spot['Close'] = spot['Close']/spot['PriceWeight']
    spot['Time'] = spot['Time'].astype(np.datetime64).dt.strftime('%Y-%m-%d')
    return spot[['Time','Close']]
    
    
    
def datafill(spot):
    """填充无交易日期的汇率
        以上一个交易日的的收盘价进行填充
    """
    def dateseris(mindate,maxdate):
        
        date = timedelta(mindate,0)
        series = []
        while maxdate>=date:
            series.append(date)

            date = timedelta(date,1)

        return series

    spot = pd.merge(pd.DataFrame(dateseris(spot['Time'].min(),spot['Time'].max()),columns=['Time']),spot[['Time','Close']],on=['Time'],how='left' )
    spot = spot.sort_values('Time')
    for i in range(spot.shape[0]):
        if spot['Close'].values[i].__str__()=='nan':
            spot['Close'].values[i] = spot['Close'].values[i-1]
    return spot

def lagdata(spot,lags=30):
    """
    spot：每天外汇收益率时间序列
    lags：时间长度
    序列指定时间长度的收益率
    """
    spot = datafill(spot)
    #print lags
    
    spot['Close_%d'%lags] = np.repeat(None,lags).tolist() + spot['Close'].values[:-lags].tolist()
    spot =  spot.dropna()
    
    
    spot['Close_%d_rate'%lags] = (spot['Close'] - spot['Close_%d'%lags])/spot['Close_%d'%lags]
    
    return spot['Close_%d_rate'%lags].values/1.0/lags
    
    

    
#for i in range(len(trf.data)):trf.data[i]['trade_id'] = i   
#trf.data = filter(lambda x:x['currency_pair']!='EUREUR',trf.data)
