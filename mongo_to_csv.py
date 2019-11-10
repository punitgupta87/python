#!/usr/bin/env

##################################
# Create by  : Punit Gupta
# Created on : 11-Sep-2018
# how will you traverse document within a document in mongodb
# Update Comment
#   {
#       "_id": "5b90c577a6182a5c0a954db3",
#       "student_id": 0,
#       "scores": [
#           {
#               "type": "exam",
#               "score": 64.28198778931606
#           },
#           {
#               "type": "quiz",
#               "score": 58.53710443088568
#           },
#           {
#               "type": "homework",
#               "score": 96.1418585189822
#           },
#           {
#               "type": "homework",
#               "score": 15.623922827382265
#           }
#       ],
#       "class_id": 433
#   }
##################################
# http://api.mongodb.com/python/current/migrate-to-pymongo3.html#mongoclient-connects-asynchronously

import pymongo 

def mongodb_conn():
    conn = pymongo.MongoClient("mongodb://localhost")
    return conn

def mongo_to_csv():
    # connect to collection
    # call an aggregate function which will return a cursor
    print("Fetching data from MongoDB")
    connection = mongodb_conn()  # connect to database
    db = connection.school
    collection = db.students

    try:
        cursor_db = collection.aggregate([
            {"$unwind": "$scores"},
            {"$match": {"scores.type": "homework"}},
            {"$group": {
                "_id": "$student_id",
                "arrPush": {"$push": "$scores.score"},
                "class": {"$addToSet": "$class_id"},
                "score": {"$sum": "$scores.score"}
            }
            },
            {"$sort": {"scores": -1}}
        ],
            allowDiskUse=True
        )
    except pymongo.errors.ConnectionFailure as e:
        print("Connection error, query will not execute, ",e)
        return
    except pymongo.errors.OperationFailure as e:
        print("Invalid query, ",e)
        return

    print("Writing data to file")
    file = open("mongo_to_csv", "w+")
    file.write('"{0}","{1}"\n'.format("id", "score_homework"))
    for i in cursor_db:
        file.write('"{0}",''"{1}"\n'.format(int(i['_id']), round(i['score'],2)))
    file.close()

    print("file is created")
    return True

if __name__ == '__main__':
    mongo_to_csv()
