import os
import psycopg2
from flask import Flask, request
from flask_api import status
from requests.models import Response
import json
import re

app = Flask(__name__)

def get_db_connection():
    # u = os.environ['DB_USERNAME']
    # pw = os.environ['DB_PASSWORD']

    conn = psycopg2.connect(host='localhost',
                database='tbd_medsos',
                user='medsos',
                password='123456')
    return conn


@app.route('/api/socmed', methods=['POST'])
def fetch_socmed_data():
    response = Response()
    
    if request.method == 'POST':
        req_json = request.get_json(force=True)
        keys = list(req_json)
        print(req_json)
        if 'start' not in keys:
            return "Missing field <start>", status.HTTP_400_BAD_REQUEST
        elif 'end' not in keys:
            return "Missing field <end>", status.HTTP_400_BAD_REQUEST
        elif 'social_media' not in keys:
            return "Missing field <social_media>", status.HTTP_400_BAD_REQUEST

        start = req_json['start']
        end = req_json['end']

        if re.search('^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', start):
            if re.search('^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', end):
                pass
            else:
                return "Incorrect datetime format", status.HTTP_400_BAD_REQUEST
        else:
            return "Incorrect datetime format", status.HTTP_400_BAD_REQUEST

        start += ":00"
        end += ":00"

        social_media = req_json['social_media']

        if social_media not in ["twitter", "facebook", "youtube", "instagram"]:
            return "Invalid social_media value", status.HTTP_400_BAD_REQUEST
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT timestamp, count, unique_count FROM socmed WHERE social_media = '{social_media}' and timestamp >= '{start}' and timestamp <= '{end}';")
    data = cur.fetchall()
    cur.close()
    conn.close()

    data = [(t.strftime("%Y-%m-%d %H:%M:%S"), c, uc) for t, c, uc in data]

    response = app.response_class(
            response=json.dumps(data),
            status=200,
            mimetype='application/json'
    )
    
    return response

if __name__ == '__main__':
    app.run(debug = True)