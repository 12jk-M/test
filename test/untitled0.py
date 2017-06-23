# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 16:50:39 2016

@author: PRui
"""
import pymongo
import datetime
from pymongo import MongoClient
client = MongoClient()
db = client.test_database
collection = db.test_collection
post = {"author": "Mike","text": "My first blog post!","tags": ["mongodb", "python", "pymongo"],"date": datetime.datetime.utcnow()}
posts = db.posts
post_id = posts.insert_one(post).inserted_id
