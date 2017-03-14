#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 10:10:08 2017

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

#Returns a command that will pass into mongoDB that will return top n user contribution data.
def return_top_n_user_data(n):
    command = [
                { "$group": {
                "_id":  "$created.user",
                "count": { "$sum": 1 }}},
                        {"$sort" : {"count" : -1}}, { "$limit" : n }]
    
    return command

# top n user contribution worker, returns the top n user contribution data after command is passed into mongoDB
def return_top_n_users():
    user_count = 20
    pipeline = return_top_n_user_data(user_count)
    result = aggregate(db, pipeline)

    return result

# top n user count worker, returns the top n user count data after command is passed into mongoDB    
def top_n_user_count_int(user_results):
    totals = 0
    for i in user_results:
        totals = totals +  i['count']
    
    return totals

#Returns the number of aggregated users
def return_number_of_users():
    pipeline = return_user_data()
    result = aggregate(db, pipeline)

    return len(result)

#Returns the number of documents in the collection       
def return_number_of_collections(db):
    return db.osm.find().count()


#Main method for running all of the code we want to review.
if __name__ == "__main__":
    db = get_db('phoenix')
    results = return_top_n_users()
    import pprint
    #Printing the top  users
    pprint.pprint(results)
    top_n_user_contrib_count = top_n_user_count_int(results)
    print "Total Contributions for Top n users: " + str(top_n_user_contrib_count)
    total_collection = return_number_of_collections(db)
    print "Collection Count: "  + str(total_collection)
    print float(top_n_user_contrib_count)/float(total_collection)
    print return_number_of_users()
    
    
