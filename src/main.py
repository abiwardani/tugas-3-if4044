import os
import psycopg2
from flask import Flask, request
from flask_api import status
from requests.models import Response
import json

app = Flask(__name__)

def get_db_connection():
	# u = os.environ['DB_USERNAME']
	# pw = os.environ['DB_PASSWORD']

	conn = psycopg2.connect(host='localhost',
				database='socmed',
				user='bigdata',
				password='IF4044')
	return conn


@app.route('/api/socmed', methods=['POST'])
def fetch_socmed_data():
	response = Response()
	
	if request.method == 'POST':
		if 'start' not in request.form:
			return "Missing field <start>", status.HTTP_400_BAD_REQUEST
		elif 'end' not in request.form:
			return "Missing field <end>", status.HTTP_400_BAD_REQUEST
		elif 'social_media' not in request.form:
			return "Missing field <social_media>", status.HTTP_400_BAD_REQUEST

		start = request.form['start']
		end = request.form['end']
		social_media = request.form['social_media']
	
	conn = get_db_connection()
	cur = conn.cursor()
	cur.execute(f'SELECT timestamp, count, unique_count FROM socmed WHERE social_media = "{social_media}" AND timestamp >= {start} AND timestamp <= {end};')
	data = cur.fetchall()
	cur.close()
	conn.close()

	response = app.response_class(
	        response=json.dumps(data),
	        status=200,
	        mimetype='application/json'
	)
	
	return response