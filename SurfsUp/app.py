from flask import Flask, jsonify
from sqlHelper import SQLHelper

#################################################
# Flask Setup
#################################################
app = Flask(__name__)
sql = SQLHelper()

#################################################
# Flask Routes
#################################################


@app.route('/')
def welcome():
    return ('''
            Welcome to the Climate Analysis API!                                                                                                  <br/>
            Available Routes:                                                                                                                     <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/precipitation_orm">/api/v1.0/precipitation_orm</a>                                         <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/precipitation_sql">/api/v1.0/precipitation_sql</a>                                         <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/stations_orm">/api/v1.0/stations_orm</a>                                                   <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/stations_sql">/api/v1.0/stations_sql</a>                                                   <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/tobs_orm">/api/v1.0/tobs_orm</a>                                                           <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/tobs_sql">/api/v1.0/tobs_sql</a>                                                           <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/temp_orm/start_date">/api/v1.0/temp_orm/&lt;start_date&gt;</a>                             <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/temp_sql/start_date">/api/v1.0/temp_sql/&lt;start_date&gt;</a>                             <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/temp_orm/start_date/end_date">/api/v1.0/temp_orm/&lt;start_date&gt;/&lt;end_date&gt;</a>   <br/>
            &nbsp;&nbsp;&nbsp;&nbsp;<a href="/api/v1.0/temp_sql/start_date/end_date">/api/v1.0/temp_sql/&lt;start_date&gt;/&lt;end_date&gt;</a>   <br/>
            ''')


@app.route("/api/v1.0/precipitation_orm")
def precipitation_orm():
    data = sql.query_precipitation_orm()
    return (jsonify(data))


@app.route("/api/v1.0/precipitation_sql")
def precipitation_sql():
    data = sql.query_precipitation_sql()
    return (jsonify(data))


@app.route("/api/v1.0/stations_orm")
def stations_orm():
    data = sql.query_stations_orm()
    return (jsonify(data))


@app.route("/api/v1.0/stations_sql")
def stations_sql():
    data = sql.query_stations_sql()
    return (jsonify(data))


@app.route("/api/v1.0/tobs_orm")
def tobs_orm():
    data = sql.query_tobs_orm()
    return (jsonify(data))


@app.route("/api/v1.0/tobs_sql")
def tobs_sql():
    data = sql.query_tobs_sql()
    return (jsonify(data))


@app.route("/api/v1.0/temp_orm/<start>")
def temp_orm(start):
    data = sql.query_temp_orm(start)
    return (jsonify(data))


@app.route("/api/v1.0/temp_sql/<start>")
def temp_sql(start):
    data = sql.query_temp_sql(start)
    return (jsonify(data))


@app.route("/api/v1.0/temp_orm/<start>/<end>")
def temp_range_orm(start, end):
    data = sql.query_temp_range_orm(start, end)
    return (jsonify(data))


@app.route("/api/v1.0/temp_sql/<start>/<end>")
def temp_range_sql(start, end):
    data = sql.query_temp_range_sql(start, end)
    return (jsonify(data))


# Run the App
if __name__ == '__main__':
    app.run(debug=True)