import os
import csv
import mysql.connector


DB_CONFIG = {
    "host": os.getenv("DB_HOST"), 
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

LOCATIONS = {
    'UK': 1, 
    'USA': 2, 
    'Germany': 3, 
    'Australia': 4, 
    'India': 5
}

INTERESTS = {
    'Fashion': 1, 
    'Gaming': 2, 
    'Travel': 3, 
    'Education': 4, 
    'Technology': 5, 
    'Health': 6, 
    'Finance': 7, 
    'Sports': 8
}

DEVICES = {
    'Mobile': 1,
    'Desktop': 2,
    'Tablet': 3
}
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS locations (
        id INT PRIMARY KEY,
        location VARCHAR(50)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS interests (
        id INT PRIMARY KEY,
        interest VARCHAR(50)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS device (
        id INT PRIMARY KEY,
        device_type VARCHAR(50)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        age INT,
        gender ENUM('Male', 'Female', 'Other'),
        location_id INT,
        signup_date DATETIME,
        FOREIGN KEY (location_id) REFERENCES locations(id)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_interests (
        user_id INT,
        interest_id INT,
        PRIMARY KEY (user_id, interest_id),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (interest_id) REFERENCES interests(id) ON DELETE CASCADE
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS advertiser (
        id INT PRIMARY KEY,
        advertiserName VARCHAR(255)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign (
        campaignID INT PRIMARY KEY,
        advertiserID INT,
        campaignName VARCHAR(255),	
        campaignStartDate DATETIME,
        campaignEndDate	DATETIME,
        AdSlotSize	VARCHAR(100),
        Budget DECIMAL(15, 2),	
        RemainingBudget DECIMAL(15, 2),
        FOREIGN KEY (advertiserID) REFERENCES advertiser(id)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS targetingCriteria (
        campaignID INT PRIMARY KEY,
        minAge INT,
        maxAge INT,
        interest_id INT,
        location_id INT,
        FOREIGN KEY (campaignID) REFERENCES campaign(campaignID) ON DELETE CASCADE,
        FOREIGN KEY (interest_id) REFERENCES interests(id),
        FOREIGN KEY (location_id) REFERENCES locations(id)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS event (
        eventID VARCHAR(50) PRIMARY KEY,
        campaignID INT,
        userID INT,
        device_id INT,
        location_id INT,
        timeStamp DATETIME,
        bidAmount FLOAT,
        adCost FLOAT,
        wasClicked BOOLEAN,
        clickTimestamp DATETIME,
        adRevenue FLOAT,
        FOREIGN KEY (campaignID) REFERENCES campaign(campaignID) ON DELETE CASCADE,
        FOREIGN KEY (userID) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (location_id) REFERENCES locations(id),
        FOREIGN KEY (device_id) REFERENCES device(id)
    )
""")


sql = """
INSERT IGNORE INTO device (id, device_type) VALUES (%s, %s)
"""

for key, value in DEVICES.items():
    cursor.execute(sql, (value, key))

sql = """
INSERT IGNORE INTO locations (id, location) VALUES (%s, %s)
"""

for key, value in LOCATIONS.items():
    cursor.execute(sql, (value, key))

sql = """
INSERT IGNORE INTO interests (id, interest) VALUES (%s, %s)
"""

for key, value in INTERESTS.items():
    cursor.execute(sql, (value, key))

conn.commit() 

BATCH_SIZE = 1000
users_batch = []
user_interests_batch = []
i = 0

sql_users = """
INSERT IGNORE INTO users (id, age, gender, location_id, signup_date) 
VALUES (%s, %s, %s, %s, %s)
"""

sql_users_interest = """
INSERT IGNORE INTO user_interests (user_id, interest_id) 
VALUES (%s, %s)
"""

with open("users.csv", mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)

    next(reader)

    for row in reader:
        
        users_batch.append(
            [row[0], row[1], row[2], LOCATIONS.get(row[3]), row [5]]
        )

        for interest in row[4].split(","):
            user_interests_batch.append([row[0], INTERESTS.get(interest)])

        if len(users_batch) == BATCH_SIZE:
            cursor.executemany(sql_users, users_batch)
            cursor.executemany(sql_users_interest, user_interests_batch)
            conn.commit()
            print(i)
            i += 1
            users_batch.clear()
            user_interests_batch.clear()

    if users_batch or user_interests_batch:
        cursor.executemany(sql_users, users_batch)
        cursor.executemany(sql_users_interest, user_interests_batch)
        conn.commit()
            
advertiser_dict = {}
campaigns_dict = {}
campaigns_list = []
targetingCriteria_list = []

with open("campaigns.csv", mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)

    next(reader)
    i = 1

    for row in reader:
        if row[1] not in advertiser_dict:
            advertiser_dict[row[1]] = i
            i += 1

        campaigns_dict[row[2]] = row[0]
        
        campaigns_list.append(
            [row[0], advertiser_dict.get(row[1]), row[2], 
             row[3], row[4], row[6], row[7], row[8]]
        )

        temporaty_dict = row[5].split(",")
        temporaty_dict.append(row[0])
        
        targetingCriteria_list.append(temporaty_dict)

denom_targetingCriteria_list = []

for item in targetingCriteria_list:
    denom_targetingCriteria_list.append(
        [
        item[3], 
        item[0][-5:-3], 
        item[0][-2:], 
        INTERESTS.get(item[1].strip()),
        LOCATIONS.get(item[2].strip())
        ])
    


sql_advertiser = """
INSERT IGNORE INTO advertiser (id, advertiserName) 
VALUES (%s, %s)
"""

sql_campaign = """
INSERT IGNORE INTO campaign (
campaignID,
advertiserID,
campaignName,
campaignStartDate,
campaignEndDate,
AdSlotSize,
Budget,
RemainingBudget
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

sql_targetingCriteria = """
INSERT IGNORE INTO targetingCriteria (
campaignID,
minAge,
maxAge,
interest_id,
location_id
) 
VALUES (%s, %s, %s, %s, %s)
"""

for key, value in advertiser_dict.items():
    cursor.execute(sql_advertiser, (value, key))
    conn.commit()

cursor.executemany(sql_campaign, campaigns_list)
conn.commit()

cursor.executemany(sql_targetingCriteria, denom_targetingCriteria_list)
conn.commit()

event_list = []

sql_event = """
INSERT IGNORE INTO event (
eventID,
campaignID,
userID,
device_id,
location_id,
timeStamp,
bidAmount,
adCost,
wasClicked,
clickTimestamp,
adRevenue
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


with open("events.csv", mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    i = 1

    next(reader)

    for row in reader:
        
        event_list.append(
            [row[0], 
             campaigns_dict.get(row[2]),
             row[9],
             DEVICES.get(row[10]),
             LOCATIONS.get(row[11]),
             row[12],
             row[13],
             row[14],
             row[15],
             row[16].strip() if row[16].strip() else None,
             row[17],
             ]
        )


        if len(event_list) == BATCH_SIZE:
            cursor.executemany(sql_event, event_list)
            conn.commit()
            print(f"{i} тис оброблено", flush=True)
            i += 1
            event_list.clear()
            if i == 1000:
                break

    if event_list:
        cursor.executemany(sql_event, event_list)
        conn.commit()

    


print("\n✅ Скрипт успішно завершив роботу! Перевір таблиці у Workbench.", flush=True)