import os
from flask import Flask, jsonify, request
from flask.ext.sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from datetime import datetime
from niftygateway_volume.py import get_all_time_vol, get_week_vol

sched = BackgroundScheduler(daemon=True)
sched.add_job(upload_week_nifty, 'interval', minutes=1)
sched.start()

app = Flask(__name__)
app.config["DATABASE_URI"] = os.environ.get('DATABASE_URL', None)
db = SQLAlchemy(app)

class WeeklyData(db.Model):
	__tablename__ = "weeklydata"
	id = db.Column(db.Integer, primary_key=True)
	date = db.Column(db.DateTime)
	volume = db.Column(db.Float)

	def __init__(self, date, volume):
		self.date = date
		self.volume = volume

	def __repr__(self):
		return ('<Date %r>' % self.date + 'Volume %r>' % self.volume)

@app.route("/get_all_nifty")
def get_all_nifty():
	return jsonify(get_all_time_vol())

@app.route("/get_all_nifty", methods=["POST"])
def upload_all_nifty():
	weekly_volume = get_all_time_vol()
	weekly_volume.to_sql(name="weeklydata", con=db, if_exists="replace", index=False)
	return "sucess"

@app.route("/get_week_nifty", methods=["POST"])
def upload_week_nifty():
	today = datetime.today().strftime('%Y-%m-%d')
	last_week = datetime.today().timedelta(days=7).strftime('%Y-%m-%d')

	weekly_volume = get_week_vol(last_week, today)
	weekly_volume.to_sql(name="weeklydata", con=db, if_exists="append", index=False)
	return "success"

if __name__ == "__main__":
	app.run(debug=True)