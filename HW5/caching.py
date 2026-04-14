from flask import Flask
import mysql.connector
import redis
import json
import os

r = redis.Redis(host=os.getenv('REDIS_HOST', 'redis'), port=6379, decode_responses=True)

app = Flask(__name__)

def create_conn():
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    
    return conn

@app.route("/campaign/<campaign_id>/performance")
def campaign_perfomance(campaign_id):

    redis_key = f"campaign_id:{campaign_id}"

    redis_data = r.get(redis_key)

    if redis_data:
        return json.loads(redis_data)


    query = '''
       SELECT
       c.campaignName, 
       CAST(ROUND((COUNT(clickTimestamp) / COUNT(*)) * 100, 2) AS FLOAT) AS CTR,
       COUNT(clickTimestamp) AS Total_impressions,
       COUNT(*) AS Total_clicks,
       ROUND(SUM(e.adCost), 2) AS Total_ad_cost
       FROM event e
       JOIN campaign c ON e.campaignID = c.campaignID
       JOIN advertiser a ON c.advertiserID = a.id
       WHERE c.campaignID = %s;
    '''

    conn = create_conn()
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, (campaign_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()
    
    if result["campaignName"] == None:
        return {"error": "Campaign not found"}, 404

    r.setex(redis_key, 30, json.dumps(result))

    return result

@app.route("/advertiser/<advertiser_id>/spending")
def advertiser_spending(advertiser_id):

    redis_key = f"advertiser_id:{advertiser_id}"

    redis_data = r.get(redis_key)

    if redis_data:
        return json.loads(redis_data)

    query = '''
       SELECT a.advertiserName, ROUND(SUM(SP_BY_CAMP.cost), 2) AS total_spend
       FROM (SELECT campaignID, SUM(adCost) AS cost
       FROM event
       GROUP BY campaignID
       ) AS SP_BY_CAMP
       JOIN campaign c ON SP_BY_CAMP.campaignID = c.campaignID
       JOIN advertiser a ON a.id = c.advertiserID
       WHERE a.id = %s;
    '''

    conn = create_conn()
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, (advertiser_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result["advertiserName"] == None:
        return {"error": "Advertiser not found"}, 404
    
    r.setex(redis_key, 300, json.dumps(result))

    return result

@app.route("/user/<user_id>/engagements")
def user_engagements(user_id):

    redis_key = f"user_id:{user_id}"

    redis_data = r.get(redis_key)

    if redis_data:
        return json.loads(redis_data)

    pre_query = '''
        SELECT id FROM users WHERE id = %s;
    '''

    query = '''
        SELECT events.eventID, CAST(events.clickTimestamp AS CHAR) AS clickTimestamp, c.campaignName, a.advertiserName
        FROM(SELECT eventID, campaignID, clickTimestamp
        FROM event
        WHERE userID = %s and clickTimestamp is not NULL) AS events
        JOIN campaign c ON c.campaignID = events.campaignID
        JOIN advertiser a ON a.id = c.advertiserID;
    '''

    conn = create_conn()

    
    cursor = conn.cursor(dictionary=True)
    cursor.execute(pre_query,(user_id,))
    pre_result = cursor.fetchall()
    

    if not pre_result:

        cursor.close()
        conn.close()

        return {"error": "User not found"}, 404

    cursor.execute(query, (user_id,))
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    if result == []:
        r.setex(redis_key, 300, json.dumps(["User don't have cliks"]))
        return ["User don't have cliks"]
    
    r.setex(redis_key, 300, json.dumps(result))

    return result

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)