import os, sqlite3, sys
from flask import Flask, render_template, request, make_response
import acs_db

app = Flask(__name__)

@app.route('/home')
def home():
	print 'hi'
	return render_template('home.html')

@app.route('/acs_lookup', methods=['GET','POST'])
def acs_lookup():
	if request.method == 'GET':
		return render_template('acs_lookup.html')
	else:
		input_csv = request.files['zipfile']
		input_fields = request.form['fields']
		print input_fields
		success = acs_db.join_by_zip(input_csv,input_fields.split(','))
		if success:
			print 'ok'
		response = make_response(file( 'tmp/tmp.csv', 'rb' ).read())
		response.headers["Content-Disposition"] = "attachment; filename=result1.csv"
		return response

if __name__ == '__main__':
	app.run()