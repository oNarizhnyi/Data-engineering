import mysql.connector
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import datetime

def CTR_and_lastAds():

    mysql_conn = mysql.connector.connect(
        host="localhost", 
        port=3307,
        user="root",
        password="password",
        database="my_db"
    )
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT e.eventID, e.timeStamp, e.clickTimestamp, adv.campaignName, adv.advertiserName, e.adCost, e.userID, l.location
        FROM (SELECT a.advertiserName, c.campaignName, c.campaignID
        FROM campaign c
        JOIN advertiser a ON a.id = c.advertiserID) as adv
        JOIN event e ON e.campaignID = adv.campaignID
        JOIN locations l ON l.id = e.location_id;
    """

    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect() 

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS CTR (
            campaignName text,
            timeStamp_date timestamp,
            eventID text,
            clickTimestamp timestamp,
            PRIMARY KEY ((campaignName, timeStamp_date), eventID)
        );
    """)

    query_CTR = session.prepare("""
        INSERT INTO CTR (campaignName, timeStamp_date, eventID, clickTimestamp)
        VALUES (?, ?, ?, ?)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS last_ads (
            userID int,
            timeStamp timestamp,
            wasCliked boolean,
            campaignName text,
            PRIMARY KEY ((userID), timeStamp, campaignName)
        ) WITH CLUSTERING ORDER BY (timeStamp DESC);
    """)

    query_last_ads = session.prepare("""
        INSERT INTO last_ads (userID, timeStamp, wasCliked, campaignName)
        VALUES (?, ?, ?, ?)
    """)
    cursor.execute("SET SESSION net_read_timeout = 3600;")
    cursor.execute("SET SESSION net_write_timeout = 3600;")
    cursor.execute(sql_query)

    batch_size = 2000
    count = 0

    while True:
        rows = cursor.fetchmany(batch_size)
        
        if not rows:
            break

        params_CTR = [(
            row['campaignName'],
            row['timeStamp'],      
            str(row['eventID']),    
            row['clickTimestamp']
        ) for row in rows]

        params_last_ads = [(
            row['userID'],
            row['timeStamp'],
            True if row['clickTimestamp'] is not None else False,  
            row['campaignName']
        ) for row in rows]

        execute_concurrent_with_args(session, query_CTR, params_CTR, concurrency=100)
        execute_concurrent_with_args(session, query_last_ads, params_last_ads, concurrency=100)
        
        count += 1
        print(count * batch_size)

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#CTR_and_lastAds()

def top_advertisers_by_month():

    mysql_conn = mysql.connector.connect(
        host="localhost", 
        port=3307,
        user="root",
        password="password",
        database="my_db"
    )
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT 
            SP_BY_CAMP.month_bucket,
            a.advertiserName, 
            ROUND(SUM(SP_BY_CAMP.cost), 2) AS total_cost
        FROM (
            SELECT 
            DATE_FORMAT(timeStamp, '%Y-%m') AS month_bucket,
            campaignID, 
            SUM(adCost) AS cost
            FROM event
            GROUP BY 1, 2
            ) AS SP_BY_CAMP
        JOIN campaign c ON SP_BY_CAMP.campaignID = c.campaignID
        JOIN advertiser a ON a.id = c.advertiserID
        GROUP BY 1, 2;
    """

    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect() 

    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS top_advertisers_by_month (
            date text,
            total_cost float,
            advertiserName text,
            PRIMARY KEY ((date), total_cost, advertiserName)
        ) WITH CLUSTERING ORDER BY (total_cost DESC, advertiserName ASC);
    """)

    query_top_advertisers_by_month = session.prepare("""
        INSERT INTO top_advertisers_by_month (date, total_cost, advertiserName)
        VALUES (?, ?, ?)
    """)

    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchall()
        
        if not rows:
            break

        params = [(
                row['month_bucket'],
                float(row['total_cost']),
                row['advertiserName']) for row in rows]

        execute_concurrent_with_args(session, query_top_advertisers_by_month, params, concurrency=100)
        

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#top_advertisers_by_month()