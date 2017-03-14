#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 12:55:32 2017

@author: adam
"""
import pprint
#Connect to local mongoDB
def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


#Use this method when process aggregate functions.  This returns the documents
def aggregate(db, pipeline):
    return [doc for doc in db.osm.aggregate(pipeline)]

#Returns a pipeline function for users sorted in descending order
def return_user_data():
    command = [
                { "$group": {
                "_id": {"UserName": "$created.user"},
                "count": { "$sum": 1 }}},
                        {"$sort" : {"count" : -1}}]
    
    return command

#Returns a pipeline function for types sorted in descending order
def return_types_data():
    command = [
                { "$group": {
                "_id": {"type": "$type"},
                "count": { "$sum": 1 }}},
                        {"$sort" : {"count" : -1}}]
    
    return command

def return_number_of_collections(db):
    return db.osm.find().count()

#Returns the number of aggregated users
def return_number_of_types():
    pipeline = return_types_data()
    result = aggregate(db, pipeline)

    return result

#Returns the number of aggregated users
def return_number_of_users():
    pipeline = return_user_data()
    result = aggregate(db, pipeline)

    return len(result)


#Main execution - set all of the items we want to return by calling the various methods to do so.          
if __name__ == "__main__":
    db = get_db('phoenix')
    print "Number of users: " + str(return_number_of_users())
    pprint.pprint(return_number_of_types())
    print "Number of collections: " + str(return_number_of_collections(db))