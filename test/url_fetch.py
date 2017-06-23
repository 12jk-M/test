# -*- coding: utf-8 -*-
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities 
import pandas as pd
import urllib
import time
import traceback
from pymongo import MongoClient
import threading
import pymongo
class ResultDBs():
    

    def __init__(self,url, project,database='resultdb',indexlist=["code",'type','Time']):
        #url = 'mongodb://resultdb:resultdb@10.4.32.21:27017/resultdb'
        self.conn = MongoClient(url)
        self.conn.admin.command("ismaster")
        self.database = self.conn[database]
        self.project =project
        
        if project not in self.database.collection_names():
            self._create_project(project,indexlist)
            
        self.collection_name = self.database[project]
        
        

    def _create_project(self, project,indexlist):
        collection_name = self.database[ self.project]
        indexs = map(lambda x:(x,pymongo.ASCENDING),indexlist)
        collection_name.create_index(indexs,unique=True)
        
        
    def select(self,where):
        """
        where:{'a':1}
        """
        collection = self.collection_name
        try:     
            data = collection.find(where)
            return [lst for lst in data]
        except:
            traceback.print_exc()
            
    def delete(self,sdate,edate):
        collection_name = self.collection_name
        try:
            collection_name.remove({'date': { '$gte': sdate, '$lte': edate}})
        except:
            traceback.print_exc()        
        

    def save(self, result):
        """
        更新collectname 给定条件的数据
        data =[(element,wh)]
        element:更新数据列表
        wh:更新条件列表
        example:
               element = {'a':1,'b':3}
               wh = {'c':3}
               
        """
        
        obj = result
        
        if result is not None:           
            collection = self.collection_name
            for wh,element in result:
                try:
                   
                   collection.update_many(wh,{'$set': element},upsert =True)
                except:
                   traceback.print_exc()
                    
            self.conn.close() 
def fetch_url(url):
    dcap = dict(DesiredCapabilities.PHANTOMJS)
    dcap["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.23 Mobile Safari/537.36" 
    driver = webdriver.PhantomJS(desired_capabilities=dcap) 
    driver.set_page_load_timeout(30)
    try:
        driver.get(url)
        soup = BeautifulSoup(driver.page_source,"xml")
        driver.quit()
    except:
        driver.quit()
        traceback.print_exc()
        return None
    url_soup = soup.find('div',class_="col-xs-10 search_name search_repadding2")
    try:
        url_data = url_soup.find('a').get('href')
        name = url_soup.find('span').text
        print url_data,name
        return (url_data,name)
    except:
        traceback.print_exc()
        return None

def get_info(url,id,name,semaphore,rLock):    
    if semaphore.acquire():
        iter = 0
        while iter<3:            
            data = fetch_url(url) 
            if data is not None:
                result = [({'id':id},dict({'id':id,'search_name':name},**{'url':data[0],'company_name':data[1]}))]
                #on_result(result)
                break
            else:
                iter=iter+1
                time.sleep(20)
        semaphore.release()
        time.sleep(1)
        
def on_result(result):
    if result is not  None: 
        url = 'mongodb://localhost:27017'
        database = 'test'
        indexlist=['id']
        project_name = 'url'
        sql = ResultDBs(url,project_name,database,indexlist)
        sql.save(result)

df = pd.read_csv('C:\Users\PRui\Downloads\query_result.csv',dtype=str)
df['partner_name']= df['partner_name'].apply(lambda x:str(x).replace('(删除)',''))
url_list = ['http://www.tianyancha.com/search?key=%s&checkFrom=searchBox'%(urllib.quote(search_name),) for search_name in df['partner_name']]
ids = list(df['partner_id']+'_'+df['partner_type'])
semaphore = threading.Semaphore(2)
rLock = threading.RLock()
thds = []
for url,id,name in zip(url_list,ids,list(df['partner_name'])):     
    thds.append(threading.Thread(target=get_info,args=(url,id,name,semaphore,rLock)))
for thd in thds:
    thd.start()
for thd in thds:
    thd.join()    