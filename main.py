from flask import Flask, jsonify, request
from flask.ext.sqlalchemy import SQLAlchemy
import pandas as pd
from niftygateway_volume.py import get_all_time_vol

app = Flask(__name__)
app.config["DATABASE_URI"] = "postgres://iqypwtvcfnkjos:dfb71980cff8ba514efaaab0d6e8782529276497afd2fb66e404fc59b250fb83@ec2-54-157-113-118.compute-1.amazonaws.com:5432/d5plm2a3lfhtcl"
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

@app.route("/get_day_nifty", methods=["POST"])
def upload_day_nifty():
	

if __name__ == "__main__":
	app.run(debug=True)