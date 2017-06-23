# -*- coding: utf-8 -*-
"""
Created on Wed Mar 01 15:36:08 2017
 * mongo存储与update
 * 存储数据
@author: candy
"""

from pymongo import MongoClient,ASCENDING
from config.xmlpath import XML
import traceback


class ResultDBs():
    

    def __init__(self,project,database='resultdb',indexlist=["code",'type','Time']):
#        url = 'mongodb://resultdb:resultdb@10.4.32.21:27017/resultdb'
    
        self.ps = XML().getdatabaseconfig()
        url = 'mongodb://'+self.ps[1]+':'+self.ps[2]+'@'+self.ps[3]+':'+str(self.ps[4])+'/'+self.ps[0]
        self.conn = MongoClient(url)
        self.conn.admin.command("ismaster")
        self.database = self.conn[database]
        self.project =project
        
        if project not in self.database.collection_names():
            self._create_project(project,indexlist)
            
        self.collection_name = self.database[project]
        
        

    def _create_project(self, project,indexlist):
        collection_name = self.database[ self.project]
        indexs = map(lambda x:(x,ASCENDING),indexlist)
        collection_name.create_index(indexs,unique=True)
        
        

        
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
        

        if result is not None:
            collection = self.collection_name
            for wh,element in result:
                try:
                   
                   collection.update_many(wh,{'$set': element},upsert =True)
                except:
                   traceback.print_exc()
                    
            self.conn.close()




     

     




