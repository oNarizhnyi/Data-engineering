import mysql.connector
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from datetime import datetime, timedelta
from collections import defaultdict
import os

def create_conn():
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    
    return conn

def create_cluster():
    cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    cluster = Cluster([cassandra_host], port=9042)

    return cluster

def CTR():

    mysql_conn = create_conn()
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT 
            DATE_FORMAT(timeStamp, '%Y-%m-%d') AS date,
            c.campaignName, 
            (COUNT(e.clickTimestamp) / COUNT(*) ) * 100 as CTR
	        FROM event e
	        JOIN campaign c ON c.campaignID = e.campaignID
            GROUP BY 1, 2;
        """

    cluster = create_cluster()
    session = cluster.connect() 

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS CTR (
            campaignName text,
            date text,
            CTR float,
            PRIMARY KEY (date, campaignName)
        );
    """)

    query_CTR = session.prepare("""
        INSERT INTO CTR (campaignName, date, CTR)
        VALUES (?, ?, ?)
    """)

    
    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchmany(2000)
        
        if not rows:
            break

        params_CTR = [(
            row['campaignName'],
            row['date'],      
            float(row['CTR'])
        ) for row in rows]


        execute_concurrent_with_args(session, query_CTR, params_CTR, concurrency=100)

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#CTR()

def get_CTR(campaign, date):
    
    cluster = create_cluster()
    session = cluster.connect() 
    session.set_keyspace('ad_analytics')

    cql = session.prepare('''
                          select * from ctr 
                          where campaignname = ? and date = ?;''')
    
    response = session.execute(cql, (campaign, date))
    for row in response:
        print(f"{row[1]} has CTR {round(row[2], 2)}")
    cluster.shutdown()

#get_CTR("Campaign_266", "2024-12-28")


def user_events():

    mysql_conn = create_conn()
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT e.eventID, e.userID, e.timeStamp, e.clickTimestamp, c.campaignName
        FROM event e
        JOIN campaign c ON e.campaignID = c.campaignID
        """

    cluster = create_cluster()
    session = cluster.connect() 

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS user_events (
            eventID text,
            userID int,
            timeStamp timestamp,
            wasClicked boolean,
            campaignName text,
            PRIMARY KEY ((userID), timeStamp, eventID)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, eventID ASC);
    """)

    query = session.prepare("""
        INSERT INTO user_events (eventID, userID, timeStamp, wasClicked, campaignName)
        VALUES (?, ?, ?, ?, ?)
    """)

    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchmany(2000)
        
        if not rows:
            break

        params = [(
            row['eventID'],
            row['userID'],      
            row['timeStamp'],    
            True if row['clickTimestamp'] else False,
            row['campaignName']
        ) for row in rows]


        execute_concurrent_with_args(session, query, params, concurrency=100)       

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#user_events()
def get_last_events(userID):

    cluster = create_cluster()
    session = cluster.connect() 

    session.set_keyspace('ad_analytics')

    cql = session.prepare('''
                          select * from user_events 
                          where userid = ?
                          limit 10;''')
    
    response = session.execute(cql, (userID,))
    for row in response:
        print(f"date: {row[1]}, campaign_name:{row[3]}, was_clicked: {row[4]}")
    cluster.shutdown()

#get_last_events(142488)

def top_advertiser_spend_by_month():

    mysql_conn = create_conn()
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT 
            SP_BY_CAMP.day_bucket,
            a.advertiserName, 
            ROUND(SUM(SP_BY_CAMP.cost), 2) AS total_cost
        FROM (
            SELECT 
            DATE_FORMAT(timeStamp, '%Y-%m-%d') AS day_bucket,
            campaignID, 
            SUM(adCost) AS cost
            FROM event
            GROUP BY 1, 2
            ) AS SP_BY_CAMP
        JOIN campaign c ON SP_BY_CAMP.campaignID = c.campaignID
        JOIN advertiser a ON a.id = c.advertiserID
        GROUP BY 1, 2;
    """

    cluster = create_cluster()
    session = cluster.connect() 

    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS top_advertiser_spend_by_month (
            date text,
            total_cost float,
            advertiserName text,
            PRIMARY KEY (date, advertiserName)
        );
    """)

    query_top_advertisers_spend_by_month = session.prepare("""
        INSERT INTO top_advertiser_spend_by_month (date, total_cost, advertiserName)
        VALUES (?, ?, ?)
    """)

    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchmany(500)
        
        if not rows:
            break

        params = [(
                row['day_bucket'],
                float(row['total_cost']),
                row['advertiserName']) for row in rows]

        execute_concurrent_with_args(session, query_top_advertisers_spend_by_month, params, concurrency=100)
        

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#top_advertiser_spend_by_month()

def get_last_30_days(start_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    previous_days = []
    for i in range(1, 31):
        previous_days.append((start_date - timedelta(days=i)).strftime('%Y-%m-%d'))

    return previous_days

def get_top_advertiser_spend_by_month(date):
    
    cluster = create_cluster()
    session = cluster.connect()
    session.set_keyspace('ad_analytics')

    total_spend = defaultdict(float)
    last_30_days = get_last_30_days(date)

    cql_query = session.prepare('''
        SELECT * FROM top_advertiser_spend_by_month
        WHERE date = ?;
    ''')
    for i in range(30):
        rows = session.execute(cql_query, (last_30_days[i],))

        for row in rows:
            total_spend[row.advertisername] += round(row.total_cost, 2)

    total_spend_list = []

    for key, value in total_spend.items():
        total_spend_list.append((value, key))

    total_spend_list.sort(reverse=True)

    top_5 = total_spend_list[:5]
    for spend, advertiser in top_5:
        print(f"Advertiser: {advertiser}, total_spend: {round(spend, 2)}")
    cluster.shutdown()

#get_top_advertiser_spend_by_month("2024-12-01")

def top_advertiser_spend_by_month_and_region():

    mysql_conn = create_conn()
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT 
            SP_BY_CAMP.day_bucket,
            a.advertiserName, 
            ROUND(SUM(SP_BY_CAMP.cost), 2) AS total_cost,
            l.location AS region
        FROM (
            SELECT 
            DATE_FORMAT(timeStamp, '%Y-%m-%d') AS day_bucket,
            campaignID, 
            location_id,
            SUM(adCost) AS cost
            FROM event
            GROUP BY 1, 2, 3
            ) AS SP_BY_CAMP
        JOIN campaign c ON SP_BY_CAMP.campaignID = c.campaignID
        JOIN advertiser a ON a.id = c.advertiserID
        JOIN locations l ON l.id = SP_BY_CAMP.location_id
        GROUP BY 1, 2, 4;
    """

    cluster = create_cluster()
    session = cluster.connect() 

    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS top_advertiser_spend_by_month_and_region (
            date text,
            total_cost float,
            advertiserName text,
            region text,
            PRIMARY KEY ((date, region), advertiserName)
        );
    """)

    query_top_advertiser_spend_by_month_and_region = session.prepare("""
        INSERT INTO top_advertiser_spend_by_month_and_region (date, total_cost, advertiserName, region)
        VALUES (?, ?, ?, ?)
    """)

    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchmany(2000)
        
        if not rows:
            break

        params = [(
                row['day_bucket'],
                float(row['total_cost']),
                row['advertiserName'],
                row['region']) for row in rows]

        execute_concurrent_with_args(session, query_top_advertiser_spend_by_month_and_region, params, concurrency=100)
        

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#top_advertiser_spend_by_month_and_region()


def get_top_advertiser_spend_by_month_and_region(date, region):
    
    cluster = create_cluster()
    session = cluster.connect()
    session.set_keyspace('ad_analytics')

    total_spend = defaultdict(float)
    last_30_days = get_last_30_days(date)

    cql_query = session.prepare('''
        SELECT * FROM top_advertiser_spend_by_month_and_region
        WHERE date = ? AND region = ?;
    ''')
    for i in range(30):
        rows = session.execute(cql_query, (last_30_days[i], region))

        for row in rows:
            total_spend[row.advertisername] += round(row.total_cost, 2)

    total_spend_list = []

    for key, value in total_spend.items():
        total_spend_list.append((value, key))

    total_spend_list.sort(reverse=True)

    top_5 = total_spend_list[:5]
    for spend, advertiser in top_5:
        print(f"Advertiser: {advertiser}, total_spend: {round(spend, 2)}")
    cluster.shutdown()

#get_top_advertiser_spend_by_month_and_region("2024-12-01", "USA")

def top_user_click_by_month():

    mysql_conn = create_conn()
    
    cursor = mysql_conn.cursor(dictionary=True)

    sql_query = """
        SELECT userID,
        DATE_FORMAT(timeStamp, '%Y-%m-%d') as date,
        COUNT(*) as total_clicks
        FROM event
        WHERE clickTimestamp is not NULL
        GROUP BY 2, 1;;
    """

    cluster = create_cluster()
    session = cluster.connect()

    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ad_analytics 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('ad_analytics')

    session.execute("""
        CREATE TABLE IF NOT EXISTS top_user_click_by_month (
            date text,
            total_clicks int,
            userID text,
            PRIMARY KEY (date, userID)
        );
    """)

    query_top_user_click_by_month = session.prepare("""
        INSERT INTO top_user_click_by_month (date, total_clicks, userID)
        VALUES (?, ?, ?)
    """)

    cursor.execute(sql_query)

    while True:
        rows = cursor.fetchmany(2000)
        
        if not rows:
            break

        params = [(
                row['date'],
                row['total_clicks'],
                str(row['userID'])) for row in rows]
        
        execute_concurrent_with_args(session, query_top_user_click_by_month, params, concurrency=100)
        

    cursor.close()
    mysql_conn.close()
    cluster.shutdown()

#top_user_click_by_month()


def get_top_user_click_by_month(date):
    
    cluster = create_cluster()
    session = cluster.connect()
    session.set_keyspace('ad_analytics')

    total_clicks = defaultdict(float)
    last_30_days = get_last_30_days(date)

    cql_query = session.prepare('''
        SELECT * FROM top_user_click_by_month
        WHERE date = ?;
    ''')
    for i in range(30):
        rows = session.execute(cql_query, (last_30_days[i],))

        for row in rows:
            total_clicks[row.userid] += row.total_clicks

    total_clicks_list = []

    for key, value in total_clicks.items():
        total_clicks_list.append((value, key))

    total_clicks_list.sort(reverse=True)

    top_10 = total_clicks_list[:10]
    for clicks, userID in top_10:
        print(f"UserID: {userID}, total_clicks: {int(clicks)}")

    cluster.shutdown()

#get_top_user_click_by_month("2024-12-01")
