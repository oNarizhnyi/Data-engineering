from cassandra.cluster import Cluster
from flask import Flask, request, jsonify
import json
from cassandra.query import dict_factory
import redis
from datetime import date, timedelta
from collections import defaultdict
import atexit
import os

r = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=6379, decode_responses=True)

def get_date_range(start_str, end_str):
    """
    Генерує масив дат між двома вхідними рядками (включно).
    """
    start_date = date.fromisoformat(start_str)
    end_date = date.fromisoformat(end_str)
    
    delta = (end_date - start_date).days    
    
    date_list = [start_date + timedelta(days=i) for i in range(delta + 1)]
    return date_list


cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
cluster = Cluster([cassandra_host], port=9042)
session = cluster.connect() 
session.set_keyspace('amazon')
session.row_factory = dict_factory

def close_cassandra():
    print("Closing Cassandra connection...")
    cluster.shutdown()

atexit.register(close_cassandra)


app = Flask(__name__)

@app.route("/products/<product_id>/reviews")
def product_reviews(product_id):

    grade = request.args.get('grade')

    if grade:
        redis_key = f"product_id_with_grade{grade}:{product_id}"

        redis_data = r.get(redis_key)

        if redis_data:
            return jsonify(json.loads(redis_data))

        cql = session.prepare('''
        SELECT * FROM product_reviews
        WHERE product_id = ? AND star_rating = ?
        ''')

        response = session.execute(cql, (product_id, int(grade)))

        data_list = list(response)

        if data_list == []:
            r.setex(redis_key, 300, json.dumps({"error": "Reviews not found"}))
            return jsonify({"error": "Reviews not found"}), 404
        
        r.setex(redis_key, 300, json.dumps(data_list))

        return jsonify(data_list)

    else:
        redis_key = f"product_id:{product_id}"

        redis_data = r.get(redis_key)

        if redis_data:
            return jsonify(json.loads(redis_data))

        cql = session.prepare('''
        SELECT * FROM product_reviews
        WHERE product_id = ?
        ''')

        response = session.execute(cql, (product_id,))

        data_list = list(response)

        if data_list == []:
            r.setex(redis_key, 300, json.dumps({"error": "Reviews not found"}))
            return jsonify({"error": "Reviews not found"}), 404

        r.setex(redis_key, 300, json.dumps(data_list))

        return jsonify(data_list)

@app.route("/customers/<customer_id>/reviews")
def customer_reviews(customer_id):

    redis_key = f"customer_id:{customer_id}"

    redis_data = r.get(redis_key)

    if redis_data:
        return jsonify(json.loads(redis_data))

    cql = session.prepare('''
    SELECT * FROM customer_reviews
    WHERE customer_id = ?
    ''')

    response = session.execute(cql, (customer_id,))

    
    data_list = [{**row, "review_date": str(row["review_date"])} for row in response]

    
    if data_list == []:
        r.setex(redis_key, 300, json.dumps({"error": "Customer not found"}))
        return jsonify({"error": "Customer not found"}), 404
    
    r.setex(redis_key, 300, json.dumps(data_list))

    return jsonify(data_list)

@app.route("/amount_of_reviews")
def amount_of_reviews():

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    amount_of_products = request.args.get('amount_of_products', type=int)

    if not all([start_date, end_date, amount_of_products]):
        return jsonify({
            "error": "Missing parameters", 
            "required": ["start_date", "end_date", "amount_of_products"]
        }), 400

    list_of_date = get_date_range(start_date, end_date)
    period = len(list_of_date)
    
    redis_key = f"data:[{start_date}, {end_date}, {amount_of_products}]"

    redis_data = r.get(redis_key)

    if redis_data:
        return jsonify(json.loads(redis_data))

    amount_of_reviews = defaultdict(int)

    cql = session.prepare('''
        SELECT product_id, review_numbers 
        FROM most_reviewed_items_for_period
        WHERE review_date = ?
    ''')
    for i in range(period):

        rows = session.execute(cql, (list_of_date[i],))
        
        for row in rows:

            amount_of_reviews[row["product_id"]] += row["review_numbers"]

   
    amount_of_reviews_list = []
    for key, value in amount_of_reviews.items():
        amount_of_reviews_list.append((value, key))

    
    amount_of_reviews_list.sort(reverse=True)
    top = amount_of_reviews_list[:amount_of_products]

    final_data = [{ "product_id": i[1], "number_of_reviews": i[0] } for i in top]
    if final_data == []:
        r.setex(redis_key, 300, json.dumps({"error": "Data not found"}))
        return jsonify({"error": "Data not found"}), 404
    
    r.setex(redis_key, 300, json.dumps(final_data))

    return jsonify(final_data)

@app.route("/customer_productivity")
def customer_productivity():

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    amount_of_customers = request.args.get('amount_of_customers', type=int)
    ers = request.args.get('ers')

    if not all([start_date, end_date, amount_of_customers]):
        return jsonify({
            "error": "Missing parameters", 
            "required": ["start_date", "end_date", "amount_of_customers"]
        }), 400
    

    list_of_date = get_date_range(start_date, end_date)
    period = len(list_of_date)
    print(ers)
    
    if ers:
        redis_key = f"data:[{start_date}, {end_date}, {amount_of_customers}, {ers}]"
    else:
        redis_key = f"data:[{start_date}, {end_date}, {amount_of_customers}]"

    redis_data = r.get(redis_key)

    if redis_data:
        return jsonify(json.loads(redis_data))

    amount_of_reviews = defaultdict(int)

    cql = session.prepare('''
        SELECT customer_id, reviews_count 
        FROM most_productive_customers_for_period
        WHERE review_date = ?
    ''')

    cql_haters = session.prepare('''
        SELECT customer_id, hater_reviews_count 
        FROM most_productive_customers_for_period
        WHERE review_date = ?
    ''')

    cql_backers = session.prepare('''
        SELECT customer_id, backer_reviews_count 
        FROM most_productive_customers_for_period
        WHERE review_date = ?
    ''')
    
    if ers == "haters":
        for i in range(period):

            rows = session.execute(cql_haters, (list_of_date[i],))
            
            for row in rows:

                amount_of_reviews[row["customer_id"]] += row["hater_reviews_count"]

    elif ers == "backers":
        for i in range(period):

            rows = session.execute(cql_backers, (list_of_date[i],))
            
            for row in rows:

                amount_of_reviews[row["customer_id"]] += row["backer_reviews_count"]

    elif ers == None:
        for i in range(period):

            rows = session.execute(cql, (list_of_date[i],))
            
            for row in rows:

                amount_of_reviews[row["customer_id"]] += row["reviews_count"]

    else:
        return jsonify({
            "error": "Incorrect ers", 
        }), 400

   
    amount_of_reviews_list = []
    for key, value in amount_of_reviews.items():
        amount_of_reviews_list.append((value, key))

    
    amount_of_reviews_list.sort(reverse=True)
    top = amount_of_reviews_list[:amount_of_customers]

    final_data = [{ "customer_id": i[1], "number_of_reviews": i[0] } for i in top]
    if final_data == []:
        r.setex(redis_key, 300, json.dumps({"error": "Data not found"}))
        return jsonify({"error": "Data not found"}), 404
    
    r.setex(redis_key, 300, json.dumps(final_data))

    return jsonify(final_data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

