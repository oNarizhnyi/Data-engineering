import mysql.connector
from pymongo import MongoClient, UpdateOne
import json
import csv
from datetime import datetime

def event_migration():

    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="root",
        port=3307,
        password="password",
        database="my_db"
    )

    query = '''
        SELECT 
            camp.*,
            e.eventID,
            e.userID,
            l.location,
            d.device_type,
            e.timeStamp,
            e.clickTimestamp
        FROM(SELECT
            c.campaignID,
            c.campaignName, 
            a.advertiserName, 
            tc.minAge, 
            tc.maxAge, 
            i.interest,
            l.location
        FROM campaign c
        JOIN advertiser a ON a.id = c.advertiserID
        JOIN targetingCriteria tc ON tc.campaignID = c.campaignID
        JOIN interests i ON i.id = tc.interest_id
        JOIN locations l ON l.id = tc.location_id) AS camp
        JOIN event e ON camp.campaignID = e.campaignID
        JOIN locations l ON l.id = e.location_id
        JOIN device d ON d.id = e.device_id;
    '''


    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute(query)
    events = []
    batch_size = 5000

    for row in cursor:
        addedData = UpdateOne(
            {"_id": row["userID"]},
            {"$push": {"events": row}}
        )
        events.append(addedData)
        
        if len(events) >= batch_size:
            mongo_collection.bulk_write(events, ordered=False)
            events = []


    if events:
        mongo_collection.bulk_write(events, ordered=False)
        events = []

    cursor.close()
    mysql_conn.close()
    mongo_client.close()


def user_migration():

    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="root",
        port=3307,
        password="password",
        database="my_db"
    )

    query = '''
        SELECT u.id AS _id, u.age, u.gender, l.location, 
            GROUP_CONCAT(i.interest SEPARATOR ', ') AS interests
        FROM users u
        JOIN locations l ON u.location_id = l.id
        JOIN user_interests ui ON ui.user_id = u.id
        JOIN interests i ON i.id = ui.interest_id
        GROUP BY u.id, u.age, u.gender, l.location;
    '''


    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]


    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute(query)
    batch_size = 1000
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        
        mongo_collection.insert_many(rows)


    cursor.close()
    mysql_conn.close()
    mongo_client.close()

def user_interaction_history(userID):

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    user_data = mongo_collection.find_one(
        {"_id": userID}, 
        {"events": 1, "_id": 0})
    

    for i in user_data["events"]:

        i["timeStamp"] = str(i["timeStamp"])
        i["clickTimestamp"] = str(i["clickTimestamp"])

    with open("user_interaction_history.json", "w", encoding="utf-8") as file:
        json.dump(user_data, file, indent=4)


    mongo_client.close()

#user_interaction_history(1)

def sort_events():

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    mongo_collection.update_many(
        {},
        {
            "$push": {
                "events":{
                    "$each": [],
                    "$sort": {"timeStamp": -1}
                }
            }}
    )

    mongo_client.close()

#sort_events()

def last_5_user_events(userID):

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    user_data = next(mongo_collection.aggregate([
        {"$match": {"_id": userID}}, 
        {"$project": {"_id": 0, "events": {"$slice": ["$events", 5]}}}]), None)

    for i in user_data["events"]:

        i["timeStamp"] = str(i["timeStamp"])
        i["clickTimestamp"] = str(i["clickTimestamp"])

    print(user_data)

    with open("last_5_user_events.json", "w", encoding="utf-8") as file:
        json.dump(user_data, file, indent=4)

    mongo_client.close()

#last_5_user_events(1555)

def get_click_24h(AdvertiserName, startDate, endDate):

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    query = [
        {"$unwind": "$events"},

        {"$match": {
            "events.advertiserName": AdvertiserName,
            "events.clickTimestamp": {
                "$ne": None,
                "$gte": startDate,
                "$lt": endDate
            }
        }},

        {"$group":{
            "_id":{
                "campaignName": "$events.campaignName",
                "hour": {"$hour": "$events.clickTimestamp"}
            },
            "click_count": {"$sum": 1}
        }},

        {"$sort": {
            "_id.campaignName": 1,
            "_id.hour": 1
        }}
    ]

    data = mongo_collection.aggregate(query)
    
    with open(f"{AdvertiserName}'s campaign.csv", "w", newline="", encoding="utf-8") as file:
         writer = csv.writer(file)

         writer.writerow(["campaignName", "hours", "clicks_count"])

         for i in data:  
             writer.writerow([
                 i["_id"]["campaignName"],
                 str(i["_id"]["hour"]) + "-" + str(int(i["_id"]["hour"])+ 1),
                 i["click_count"]
             ])

    

# st = datetime(2024, 11, 10, 0, 0, 0)
# ed = datetime(2024, 11, 11, 0, 0, 0)
# get_click_24h("Advertiser_82", st, ed)

def never_clicked():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    query = [
        {"$unwind": "$events"},

        {"$group":{
            "_id":{
                "campaignName": "$events.campaignName",
                "userID": "$_id"
            },
            "impression_count": {"$sum": 1},
            "click_count": {"$sum": {"$cond": [
                {"$eq": ["$events.clickTimestamp", None]},
                0,
                1
            ]}}
        }},

        {"$match": {
            "click_count": 0,
            "impression_count": {"$gte": 5}
        }}
    ]

    data = mongo_collection.aggregate(query)
    
    with open("never_clicked.csv", "w", newline="", encoding="utf-8") as file:
         writer = csv.writer(file)

         writer.writerow(["userID", "campaignName", "impression_count"])

         for i in data:  
             writer.writerow([
                 i["_id"]["userID"],
                 i["_id"]["campaignName"],
                 i["impression_count"]
             ])

#never_clicked()

def top_category(userID):

    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_collection = mongo_client["target_db"]["target_collection"]

    query = [
        {"$match": {
            "_id": userID}},

        {"$unwind": "$events"},

        {"$match": {
            "events.clickTimestamp": {"$ne": None}
        }},

        {"$group":{
            "_id": "$events.interest",
            "count": {"$sum": 1}
        }},

        {"$sort": {
            "count": -1
        }},

        {"$limit": 3}
    ]
    
    data = mongo_collection.aggregate(query)

    with open("top_category.csv", "w", newline="", encoding="utf-8") as file:
         writer = csv.writer(file)

         writer.writerow(["userID", "category", "impression_count"])

         for i in data:  
             writer.writerow([
                 userID,
                 i["_id"],
                 i["count"]
             ])


    mongo_client.close()

#top_category(470)