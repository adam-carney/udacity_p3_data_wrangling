#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 09:57:37 2017
This is the main code to do the data investigation

@author: adam
"""

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

#Returns a command that will pass into mongoDB that will return a list of cities with counts
def return_city_data():
    
    pipeline = [                
            { "$match":  {"address.city": {"$exists":1}}},
                { "$group": {
                "_id": "$address.city", "count": { "$sum": 1}}},
                        {"$sort" : {"count" : -1}}]
    return pipeline


#Returns the mongoDB command for top n street address data.
def return_street_adresss_data(n):
    
    pipeline = [                
            { "$match":  {"address": {"$exists":1}}},
                { "$group": {
                "_id": "$address", "count": { "$sum": 1}}},
                        {"$sort" : {"count" : -1}}, {"$limit": n}]
    return pipeline

#Returns the mongoDB command for top n amenity data.
def return_amenity_data(n):
    
    pipeline = [                
            { "$match":  {"amenity": {"$exists":1}}},
                { "$group": {
                "_id": "$amenity", "count": { "$sum": 1}}},
                        {"$sort" : {"count" : -1}}, {"$limit": n}]
    return pipeline

#city data worker, prints off the city data after command is passed into mongoDB
def return_address_city_data():
    pipeline = return_city_data()
    result = aggregate(db, pipeline)

    import pprint

    pprint.pprint(len(result))
    pprint.pprint(result)

#address data worker, prints off the address data after command is passed into mongoDB    
def return_address_top_n_data():
    return_count = 100
    pipeline = return_street_adresss_data(return_count)
    result = aggregate(db, pipeline)

    import pprint

    pprint.pprint(len(result))
    if return_count != 0:
        pprint.pprint(result[0:return_count])
    else:
        pprint.pprint(result)

#amenity data worker, prints off the amenity data after command is passed into mongoDB 
def return_amenity_top_n_data():
    return_count = 10
    pipeline = return_amenity_data(return_count)
    result = aggregate(db, pipeline)

    import pprint

    pprint.pprint(len(result))
    if return_count != 0:
        pprint.pprint(result[0:return_count])
    else:
        pprint.pprint(result)

#Main execution - set all of the items we want to return by calling the various methods to do so.          
if __name__ == "__main__":
    db = get_db('phoenix')
    return_address_top_n_data()
    return_address_city_data()
    return_amenity_top_n_data()
