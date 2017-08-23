# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 13:49:09 2016
swapoption 定价
@author: lywen
"""

from job import option
from help.help import getNow,dateTostr,getbankrate,strTodate,timedelta,strTostr,getbankrateoneday
from help.help import getcurrentrate,getcurrentbankrate,gethistoryrate,gethistorybankrate
from help.help import chooselocalmoney ##计算本金损益
from config.postgres   import  table_swaps_option,table_write_tmp##table name
from database.mongodb  import  RateExchange
from database.mongodb  import  BankRate
from database.database import postgersql
from numpy import float64,array
from help.help import interestway ##利息支付方式

from main.swapoption  import SwapOption
from config.config import getrate
from log.logs import logs
Flag = True

class SwapOptions(object):
    
    def __init__(self,Lock):
        self.table = table_swaps_option
        self.querytable = table_write_tmp
        self.Lock = Lock
        if Flag:
            self.do_all()
        else:
            self.Now = '2016-03-29 16:30:00'
            SwapOptions_oneday(self.Lock,self.Now)
            #for Now in ['2016-10-28 16:30:00','2016-11-29 16:30:00']:
                #self.Now = Now
                #SwapOptions_oneday(self.Lock,self.Now)
                
    def do_all(self):
        """
        执行所有历史数据
        """
        self.getTime()        
        if self.begintime and self.endtime is not None:
            Now = self.begintime
            while (Now>=self.begintime and Now<=self.endtime):
                self.Now = strTostr(Now)
                SwapOptions_oneday(self.Lock,self.Now)
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
            
class SwapOptions_oneday(option):
    """
    区间式货币掉期or货币互换or封顶式期权:
        Setdate:厘定日
        SetRate:厘定日汇率
        deliverydate:交割日
        strikeLowerRate: 执行汇率下限
        strikeUpperRate:执行汇率上限
        currentRate:实时汇率
        SellRate:本币汇率
        BuyRate:外币汇率
        delta:汇率波动率
    """

    def __init__(self,Lock,Now,delta=0.1):
        option.__init__(self)
        #self.Now = Now
        self.table = table_swaps_option
        self.writetable = table_write_tmp
        self.delta = delta##波动率
        self.Lock = Lock
        self.Now = Now
        self.getDataFromPostgres()
                #print '期权类型','ID','货币对','计息方式', '交易类型','起息日','交割日','本金','损益'
                #self.getDataFromMongo()
                
    def getDataFromMongo(self):
        """
        from mongo get the currency_pairs and bank_rate
        """
        ## currency_pairs
        currency_pairs = list(set(map(lambda x:x['currency_pair'],self.data)))
        Time = self.Now.split()[0]
        #currency_dict = getcurrentrate(currency_pairs)
        currency_dict = gethistoryrate(currency_pairs,Time)
        
        ##bank_rate
        self.forwarddict = []        
        for lst in self.data:
            forwarddict= {}
            ##交易本币与外币
            ##支付固定利率
            #print '计算订单{}\n'.format(lst)
            payFixRate = None if lst['pay_fix_rate'] is None else float64(lst['pay_fix_rate']) ##支付固定利率
            chargeFixRate  =  None if lst['charge_fix_rate'] is None else  float64(lst['charge_fix_rate'])##收取浮动利息固定部分
            value_date = dateTostr(lst['value_date'])
            delivery_date = dateTostr(lst['delivery_date'])
            trade_date = dateTostr(lst['trade_date'])
            #print lst['id']
            if lst['interest_pay_way'] == '3':
                ##到期一次性支付
                br = BankRate(lst['buy_currency'],interestway(lst['interest_pay_way'],lst['charge_float_libor']))
                if  delivery_date> Time:
                     date = {dateTostr(lst['delivery_date']):br.getday(Time)}
                else:
                     
                   date = {dateTostr(lst['delivery_date']):br.getday(delivery_date)}
                   
            elif lst['interest_pay_way']== '0' or lst['trade_type']=='4':
                ##提前支付
                br = BankRate(lst['buy_currency'],interestway(lst['interest_pay_way'],lst['charge_float_libor']))
                if (lst['trade_date'] - lst['value_date']).days>180:
                    
                     date = {dateTostr(lst['delivery_date']):br.getday(trade_date)}## value_date trade_date
                else:
                    date = {dateTostr(lst['delivery_date']):br.getday(value_date)}## value_date trade_date
            else:
                #print lst
                if (lst['trade_date'] - lst['value_date']).days>180:
                      date = getbankrateoneday(lst['buy_currency'],trade_date,delivery_date,lst['interest_pay_way'],lst['charge_float_libor'],Time)
                else:
                    date = getbankrateoneday(lst['buy_currency'],value_date,delivery_date,lst['interest_pay_way'],lst['charge_float_libor'],Time)
            #payFixRateDict = {}              
            if  chargeFixRate is  None:
                chargeFixRate=0.0
            
            for t in date:
                
                    date[t] = date[t]/100.0+chargeFixRate##货币浮动利率及支付利息时间
                   
            Fix = array(sorted(date.items(),key=lambda x:x[0]))
            FixDate = Fix[:,0]
            FixRate =  Fix[:,1][:]
            FixRate = FixRate.astype(float64)
            Rate = (FixDate,payFixRate,FixRate)##利息支付日期，支付卖出货币利息，收取浮动利率利息
            
            
       
            currency_pair = lst['currency_pair']
            
            ##货币对之间本币与外币
            sellCurrency  = currency_pair[:3]
            buyCurrency  = currency_pair[3:]
            ratetype ='12月'
            #SellRate,BuyRate = getcurrentbankrate(sellCurrency,buyCurrency,ratetype)
            SellRate,BuyRate = gethistorybankrate(sellCurrency,buyCurrency,ratetype,Time)##获取银行历史拆借利率
            
                
            LockedRate = float64(lst['exe_exrate'])##执行汇率
            capped_exrate  = None if lst['capped_exrate'] is None else float64(lst['capped_exrate']) ##封顶汇率
            ##获取厘定日汇率
            ##获取厘定日的汇率，如果还未到厘定日，那么汇率返回None
            Setdate = dateTostr(lst['determined_date'])##厘定日
            if Time<Setdate:
                SetRate = None
            else:
                SetRate = RateExchange(currency_pair).getdayMax(Setdate)##厘定日汇率
            #SetRate = RateExchange(currency_pair).getdayMax(Setdate)##厘定日汇率
            if SetRate ==[]:
                SetRate = None## 还未到厘定日
            
            currentRate = currency_dict.get(currency_pair) ## 实时汇率
            if currentRate is None:
                print "{} not fund!".format(currency_pair)
                continue
            #deliverydate = dateTostr(lst['delivery_date'])## 交割日期
            trade_type = lst['trade_type']##交易类型
            rateway = lst['interest_pay_way']##利息支付方式
            #sell_amount  = float64(lst['sell_amount'])
            #buy_amount  = float64(lst['buy_amount'])
            #2,3,4分别表示:区间式货币掉期(利率进行互换+固定补贴)、货币掉期（利率互换）、封顶式期权(固定补贴)
            #print 'SwapOptions', lst['id'],currency_pair,rateway,'区间式货币掉期' if trade_type=='2' else ('货币掉期' if trade_type=='3' else '封顶式期权'),value_date,delivery_date,
            temp = self.cumputeLost(Setdate,SetRate,value_date,delivery_date,currentRate,SellRate,BuyRate,LockedRate,rateway,self.delta,capped_exrate,trade_type,Rate)
            #if sell_currency+buy_currency !=currency_pair:
            #    forwarddict[lst['id']] = forwarddict[lst['id']]/currentRate
            
            #if forwarddict[lst['id']] is not  None:
                #forwarddict[lst['id']] = chooselocalmoney(lst,forwarddict[lst['id']])
            """
            'date','com_code','der_id','cap_currency','cap_amount','gal','exrate','data_date':
            日期，公司代码，衍生品编号，本金币种，本金金额，汇兑损益，当天的本金/人民币汇率，数据生成日期
            """        
            if temp is not  None:
                forwarddict['gal'],forwarddict['cap_amount'],forwarddict['cap_currency'] = chooselocalmoney(lst,temp)
            
            forwarddict['date'] = Time
            forwarddict['com_code'] = lst['consign_entity']
            forwarddict['der_id'] = lst['trade_type']+'_'+str(lst['id'])
            currencypair = forwarddict['cap_currency']+'CNY'
            forwarddict['exrate'] = gethistoryrate([currencypair],Time)[currencypair] if currencypair!='CNYCNY' else 1.0
            forwarddict['data_date'] = getNow('%Y-%m-%d %H:%M:%S') 
            logs('compute','swaoption','getDataFromMongo',forwarddict,infoFalg=True)
            #if lst['id']==1170:
                #print Setdate,SetRate,value_date,delivery_date,currentRate,SellRate,BuyRate,LockedRate,rateway,self.delta,capped_exrate,trade_type,Rate,forwarddict['gal']
            self.forwarddict.append(forwarddict)
            #print forwarddict
        #self.forwarddict = forwarddict
        self.updateData()    
                
    
    def getDataFromPostgres(self):
        
        
        #Now = getNow('%Y-%m-%d')
        Now = self.Now.split()[0]
  
        post = postgersql()
        colname1 = ['id',
                   'trade_id',
                   'currency_pair',
                   'sell_currency',
                   'buy_currency',
                   'trade_date',
                   'value_date',
                   'determined_date',
                   'delivery_date',
                   'sell_amount',
                   'buy_amount',
                   'exe_exrate',
                   'capped_exrate',
                   'pay_fix_rate',##支付固定利率
                   'charge_fix_rate',##收取固定利率固定部分
                   'interest_pay_way',##付息方式
                   'charge_float_libor',##收取固定利率浮动部分
                   'trade_type',##交易类型,
                   'type',
                   'consign_entity'
                   ]
        wherestring1 =  """ delivery_date>='{}' and trade_date<='{}'""".format(Now,Now)
        optiondata = post.select(self.table ,colname1,wherestring1)
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
        #self.data = post.select(self.table,colname,wherestring)
        
    def  cumputeLost(self,Setdate,SetRate,valuedate,deliverydate,currentRate,SellRate,BuyRate,LockedRate,rateway,delta,capped_exrate,trade_type,FixRate):
       if SellRate in [None,[]] or BuyRate in [None,[]]:
           return None
       else:
           return SwapOption(Setdate,SetRate,valuedate,deliverydate,currentRate,SellRate,BuyRate,LockedRate,rateway,delta,capped_exrate,trade_type,FixRate,self.Now)
      
        
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

