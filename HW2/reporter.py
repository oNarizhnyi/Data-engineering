import os
import mysql.connector
import csv

config = {
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'password'),
    'host': os.getenv('DB_HOST', 'db'), 
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'my_db')
}

def get_top_user(): 
    '''
    формує csv файл з даними користувачів
    які клікнули на найбільшу кількість оголошень 
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
        
    query = """
        SELECT u.id, u.age, u.gender, l.location, 
        GROUP_CONCAT(i.interest SEPARATOR ', '), 
        DATE_FORMAT(u.signup_date, '%Y-%m-%d'),
        top.total_click
        FROM (SELECT userID, COUNT(*) AS total_click
            FROM event
            GROUP BY userID
            ORDER BY total_click DESC
            LIMIT 10) AS top
        JOIN users u ON top.userID = u.id
        JOIN user_interests ui ON u.id = ui.user_id
        JOIN locations l ON l.id = u.location_id
        JOIN interests i ON i.id = ui.interest_id
        GROUP BY u.id, u.age, u.gender, l.location, u.signup_date
        ORDER BY top.total_click DESC;
    """
    cursor.execute(query)

    with open("top_users.csv", "w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow(["UserID", "Age", "Gender", "Location", "Interests", 
                        "Signup_date", "Total_click"])
        
        for i in cursor.fetchall():
            writer.writerow(i)

    cursor.close()
    conn.close()

get_top_user()


def most_effective_campaign_by_advertiser(advertiserName, startDate, endDate):
    '''
    формує csv файл з даними по кампаніям конкретного
    advertiser в розрізі кількості кліків
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    query = """
        SELECT c.campaignName, 
        CAST(ROUND((COUNT(clickTimestamp) / COUNT(*)) * 100, 2) AS FLOAT) AS CTR
        FROM event e
        JOIN campaign c ON e.campaignID = c.campaignID
        JOIN advertiser a ON c.advertiserID = a.id
        WHERE a.advertiserName = %s
        AND timeStamp >= %s
        AND timeStamp < %s
        GROUP BY e.campaignID
        ORDER BY CTR DESC
        LIMIT 5;
    """

    cursor.execute(query, (advertiserName, startDate, endDate))

    with open(f"{advertiserName} CTR.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["CampaignName", "CTR"])

        for i in cursor.fetchall():
            writer.writerow(i)


    cursor.close()
    conn.close()

most_effective_campaign_by_advertiser(
     "Advertiser_1", '2024-10-01 00:00:00', '2024-11-01 00:00:00')

def money_spent(startDate, endDate):
    '''
    формує csv файл з сумою, яку advertiser
    витратив за місяць по своїх кампаніях
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    query = """
        SELECT a.advertiserName, ROUND(SUM(SP_BY_CAMP.cost), 2) AS total
        FROM (SELECT campaignID, SUM(adCost) AS cost 
        FROM event
        WHERE timeStamp >= %s
        AND timeStamp < %s
        GROUP BY campaignID
        ) AS SP_BY_CAMP
        JOIN campaign c ON SP_BY_CAMP.campaignID = c.campaignID
        JOIN advertiser a ON a.id = c.advertiserID
        GROUP BY a.advertiserName
        ORDER BY total DESC;
    """

    cursor.execute(query, (startDate, endDate))

    with open("money_spent.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["AdvertiserName", "MoneySpent"])

        for i in cursor.fetchall():
            writer.writerow(i)


    cursor.close()
    conn.close()

money_spent('2024-10-01 00:00:00', '2024-11-01 00:00:00')

def get_CPC_and_CPM():
    '''
    формує csv файл з CPC та CPM по кожній кампанії
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    query = """
        SELECT c.campaignName, data.CPC, data.CPM
        FROM(SELECT campaignID, 
        ROUND(SUM(adCost) / NULLIF(COUNT(clickTimestamp), 0), 2) AS CPC,
        ROUND((SUM(adCost) / (COUNT(*)) * 1000), 2) AS CPM
        FROM event
        GROUP BY campaignID) AS data
        JOIN campaign c ON data.campaignID = c.campaignID
        ORDER BY data.CPC DESC;
    """

    cursor.execute(query)

    with open("CPC_and_CPM.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["CampaignName", "CPC", "CPM"])

        for i in cursor.fetchall():
            writer.writerow(i)


    cursor.close()
    conn.close()

get_CPC_and_CPM()

def top_locations():
    '''
    формує csv файл з топом країн
    в яких отриманий найбільший дохід
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    query = """
        SELECT l.location, top.revenue
        FROM(SELECT location_id, ROUND(SUM(adRevenue), 2) AS revenue
        FROM event
        GROUP BY location_id) AS top
        JOIN locations l ON l.id = top.location_id
        ORDER BY top.revenue DESC;
    """

    cursor.execute(query)

    with open("top_locations.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Location", "Revenue"])

        for i in cursor.fetchall():
            writer.writerow(i)


    cursor.close()
    conn.close()

top_locations()

def exhausted_money():
    '''
    формує csv файл з відсотком вичерпаного бюджету
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    query = """
        SELECT c.campaignName,
        ROUND(((cost.total / c.Budget) * 100), 2) AS percent
        FROM (SELECT campaignID, 
        ROUND(SUM(adCost), 2) AS total
        FROM event
        GROUP BY campaignID) AS cost
        JOIN campaign c ON c.campaignID = cost.campaignID
        HAVING percent > 80
        ORDER BY percent DESC;
    """

    cursor.execute(query)

    with open("exhausted_money.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["CampaignName", "UsedBudgetPercent"])

        for i in cursor.fetchall():
            writer.writerow(i)


    cursor.close()
    conn.close()

exhausted_money()

def better_device():
    '''
    формує csv файл з відсотком кліків 
    по різним видам девайсів
    '''
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    query = """
        SELECT d.device_type, data.pr
        FROM(SELECT device_id,
        COUNT(clickTimestamp) * 100.0 / COUNT(*) AS pr
        FROM event
        GROUP BY device_id) AS data
        JOIN device d ON data.device_id = d.id;
    """

    cursor.execute(query)

    with open("better_device.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["DeviceType", "ClickPercent"])

        for i in cursor.fetchall():
            writer.writerow(i)


    cursor.close()
    conn.close()

better_device()