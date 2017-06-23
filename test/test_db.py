# -*- coding: utf-8 -*-
"""
Created on Thu Sep 08 14:02:51 2016

@author: PRui
"""
import traceback
from pymongo import MongoClient
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
           client=MongoClient('localhost',27017)
           db=client.test_db
           self.__conn=db
       except:
           traceback.print_exc()
           self.__conn=None
    def insert(self,collection_name,list_data):
        """
        向已有的collection插入数据操作
        collection_name：数据库文档名称
        list_data：插入的数据，格式为列表        
        示例：insert('test_collection',[{'a':'b'}])
        """
        if collection_name in self.__conn.collection_names():
            collection=self.__conn[collection_name]
            try:
                collection.insert_many(list_data)
            except:
                traceback.print_exc()
        else:
            print 'collection does not exist'
    def delete(self,collection_name,dict_data):
        """
        向已有的collection删除数据操作,返回删掉数据的个数
        collection_name：数据库文档名称
        dict_data：删除的数据，格式为字典        
        示例：delete('test_collection',{'a':'b'})
        """
        if collection_name in self.__conn.collection_names():
            collection=self.__conn[collection_name]
            try:
                result=collection.delete_many(dict_data)
                return result.deleted_count
            except:
                traceback.print_exc()
        else:
            print 'collection does not exist'
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
    def update(self,collection_name,dict_data1,dict_data2):
        """
        向已有的collection修改数据操作，返回修改数据个数
        collection_name：数据库文档名称
        dict_data1：修改前的数据，格式为字典
        dict_data2：修改后的数据，格式为字典
        示例：update('test_collection',{'a':'b'},{'a':'c'})
        """
        if collection_name in self.__conn.collection_names():
            collection = self.__conn[collection_name]
            try:
                result=collection.update_many(dict_data1,{'$set':dict_data2})
                return result.modified_count
            except:
                traceback.print_exc()
        else:
            print 'collection does not exist'
    def close(self):
        """
        关闭连接
        """
        self.__conn.client.close()
        
