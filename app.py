# Import the dependencies.
from flask import Flask, jsonify
from sql_helper import SQLHelper
import pandas as pd
#################################################
# Flask Setup
#################################################

app = Flask(__name__)
sqlHelper = SQLHelper()

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/precipitation2<br/>"
        f"<a href='/api/v1.0/temperature' target='_blank'>/api/v1.0/temperature</a><br/>"
        f"<a href='/api/v1.0/2017-01-01' target='_blank'>/api/v1.0/2017-01-01</a><br/>"
        f"<a href='/api/v1.0/2017-01-01/2017-01-31' target='_blank'>/api/v1.0/2017-01-01/2017-01-31</a><br/>"
    )
@app.route("/api/v1.0/tobs")
def tobs():
    df = sqlHelper.queryTOBS()  
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/precipitation")
def precipitation():
    
    df = sqlHelper.queryPrecipitationORM()
    
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/precipitation2")
def precipitation2():
    
    df = sqlHelper.queryPrecipitationSQL()

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/stations")
def stations():

    df = sqlHelper.queryStationsSQL()
    
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/stations2")
def stations2():
    
    df = sqlHelper.queryStationsORM()

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/temperature")
def temperature():
    
    df = sqlHelper.queryTemperatureSQL()

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/temperature2")
def temperature2():
    df = sqlHelper.queryTemperatureORM()

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/<start>")
def tstats1(start):
    df = sqlHelper.queryTStatsSQL(start)

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/orm/<start>")
def tstats2(start):
    df = sqlHelper.queryTStatsORM(start)

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/<start>/<end>")
def tstats_startend1(start, end):
    df = sqlHelper.queryTStats_StartEndSQL(start, end)

    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/orm/<start>/<end>")
def tstats_startend2(start, end):
     
    df = sqlHelper.queryTStats_StartEndORM(start, end)

    data = df.to_dict(orient="records")
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)

            