# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 17:56:36 2016
forwards
@author: lywen
修改本金计算逻辑 
"""
from job import option
from help.help import getNow,getRate,dateTostr,strTostr,strTodate,timedelta
from help.help import getcurrentrate,getcurrentbankrate,gethistoryrate,gethistorybankrate
from help.help import chooselocalmoney ##计算本金损益
from config.postgres  import  table_comlong_term,table_write_tmp
#from database.mongodb import  RateExchange
from database.database import postgersql
from main.forward     import OrdinaryForward
from config.config import getrate
from log.logs import logs
Flag = True

class forwards(object):
    
    def __init__(self,Lock):
        self.table = table_comlong_term
        self.querytable = table_write_tmp
        self.Lock = Lock
        if Flag:
            self.do_all()
        else:
            self.Now = '2016-12-05 00:00:00'
            forwards_oneday(self.Lock,self.Now)
    
    def do_all(self):
        """
        执行所有历史数据
        """
        self.getTime()        
        if self.begintime and self.endtime is not None:
            Now = self.begintime
            while (Now>=self.begintime and Now<=self.endtime):
                self.Now = strTostr(Now)
                forwards_oneday(self.Lock,self.Now)
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

class forwards_oneday(option):
    """
    普通外汇远期定价
    """

    def __init__(self,Lock,Now):
        option.__init__(self)
        #self.Now = Now
        self.table = table_comlong_term
        self.writetable = table_write_tmp
        self.Lock = Lock
        self.Now = Now
        self.getDataFromPostgres()##从post提取数据
        #print '期权类型','ID','货币对','成交日期','交割日','损益'

        #self.getDataFromMongo()##从mongo提取数据并更新损益
        
        
    def getDataFromMongo(self):
        """
        from mongo get the currency_pairs and bank_rate
         #卖出币种如果是人民币(CNY/CNH)，那么买入币种是本金，否则就是卖出币种
        """
        ## currency_pairs
        ##--------------------获取汇率对的实时汇率
        currency_pairs = list(set(map(lambda x:x['currency_pair'],self.data)))
        Time = self.Now.split()[0]
        #currency_dict = getcurrentrate(currency_pairs)
        currency_dict = gethistoryrate(currency_pairs,Time)
        ##--------------------获取汇率对的历史汇率
        
        self.forwarddict = []
        
        for lst in self.data:
            ##
            forwarddict= {}
            sell_currency = lst['currency_pair'][:3]
            buy_currency  = lst['currency_pair'][3:]
            currency_pair = lst['currency_pair']
            #ratetype = getRate((lst['delivery_date'] -lst['trade_date']).days)
            ratetype = '12月'
            
            #SellRate,BuyRate = getcurrentbankrate(sell_currency,buy_currency,ratetype)##获取银行拆借利率
            SellRate,BuyRate = gethistorybankrate(sell_currency,buy_currency,ratetype,Time)##获取银行历史拆借利率
              
            LockedRate   = float(lst['rate'])##锁定汇率
            
            currentRate  = currency_dict.get(currency_pair)##即期实时汇率
            if currentRate is None:
                print "{} not fund!\n".format(currency_pair)
                continue
            
            deliverydate = dateTostr(lst['delivery_date'])##交割日期
            
           
            #print 'forwards',lst['id'],currency_pair,dateTostr(lst['trade_date']),deliverydate,

            temp = self.cumputeLost(SellRate,BuyRate,deliverydate,LockedRate,currentRate)
            #print temp
            
            ## 计算本金损益
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
            logs('compute','forwards','getDataFromMongo',forwarddict,infoFalg=True)
            #forwarddict[lst['consign_entity']]
            self.forwarddict.append(forwarddict)
            
        #self.forwarddict = forwarddict
        self.updateData()    
                
    
    def getDataFromPostgres(self):
        
        
        #Now = getNow('%Y-%m-%d')
        Now = self.Now.split()[0]
        post = postgersql()
        colname1 = ['id','trade_id','currency_pair','trade_date','delivery_date','rate','sell_currency','sell_amount','buy_currency','buy_amount','type','consign_entity','trade_type']
        wherestring1 = """ delivery_date>='{}' and trade_date<='{}'""".format(Now,Now)
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
        #self.data = post.select(self.table ,colname,wherestring)
        
    def  cumputeLost(self,SellRate,BuyRate,deliverydate,LockedRate,currentRate):
       if SellRate in [None,[]] or BuyRate in [None,[]]:
           return None
       else:
           return OrdinaryForward(SellRate,BuyRate,deliverydate,LockedRate,currentRate,self.Now)
              
        
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
        post.insert(self.writetable,columnname,insertdata)        
        post.close()
        self.Lock.release()
