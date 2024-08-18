from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func
import pandas as pd
import datetime as dt



# The Purpose of this Class is to separate out any Database logic
class SQLHelper():
    #################################################
    # Database Setup
    #################################################

    # define properties
    def __init__(self):
        self.engine = create_engine("sqlite:///Resources/hawaii.sqlite")
        self.Base = None

        # automap Base classes
        self.init_base()

    def init_base(self):
        # reflect an existing database into a new model
        self.Base = automap_base()
        # reflect the tables
        self.Base.prepare(autoload_with=self.engine)

    #################################################
    # Database Queries
    #################################################

    def query_precipitation_orm(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Find the most recent date in the data set
        recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
        most_recent_date = dt.datetime.strptime(recent_date[0], '%Y-%m-%d')

        # Calculate the date one year from the last date in data set
        # When using the date from the database, we need to subtract 366 days to make sure it includes 2016-08-23
        year_ago = most_recent_date - dt.timedelta(days=366) 

        # Perform a query to retrieve the data and precipitation scores
        precipitation = session.query(Measurement.date, Measurement.prcp).\
            filter(Measurement.date >= year_ago).\
            order_by(Measurement.date).all()

        # Save the query results as a Pandas DataFrame. Explicitly set the column names
        precipitation_df = pd.DataFrame(precipitation, columns=['Date', 'Precipitation'])

        # close session
        session.close()

        # Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
        data = precipitation_df.to_dict(orient="records")
        return (data)

    def query_precipitation_sql(self):
        # Find the most recent date in the data set.
        query = """
                SELECT date as Date, prcp as Precipitation
                FROM measurement
                WHERE date >= (SELECT DATE(date, '-365 days')
                                FROM measurement
                                ORDER BY date DESC
                                LIMIT 1)
                ORDER BY date;
                """

        # Save the query results as a Pandas DataFrame
        precipitation_df = pd.read_sql(text(query), con=self.engine)
        data = precipitation_df.to_dict(orient="records")
        return (data)

    def query_stations_orm(self):
        # Save reference to the table
        Station = self.Base.classes.station

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Query all stations
        results = session.query(Station.station, Station.name).all()

        # Save the query results as a Pandas DataFrame. Explicitly set the column names
        stations_df = pd.DataFrame(results, columns=['Station', 'Name'])

        # close session
        session.close()

        # Convert the query results to a Dictionary using `station` as the key and `name` as the value.
        data = stations_df.to_dict(orient="records")
        return (data)

    def query_stations_sql(self):
        # Query all stations
        query = "SELECT station as Station, name as Name FROM station;"

        df = pd.read_sql(text(query), con=self.engine)
        data = df.to_dict(orient="records")
        return (data)

    def query_tobs_orm(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Find the most recent date in the data set
        recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

        # Starting from the most recent data point in the database
        most_recent_date = dt.datetime.strptime(recent_date[0], '%Y-%m-%d')

        # Calculate the date one year from the last date in data set
        # When using the date from the database, we need to subtract 366 days to make sure it includes 2016-08-23
        year_ago = most_recent_date - dt.timedelta(days=366)  

        # Query the most active stations
        active_stations = session.query(Measurement.station, func.count(Measurement.station)).\
            group_by(Measurement.station).\
            order_by(func.count(Measurement.station).desc()).all()
        
        # Get the most active station
        most_active_station = active_stations[0][0]

        # Query the dates and temperature observations of the most active station for the last year of data.
        results = session.query(Measurement.date, Measurement.tobs).\
            filter(Measurement.station == most_active_station).\
            filter(Measurement.date >= year_ago).all()
        
        # Save the query results as a Pandas DataFrame. Explicitly set the column names
        tobs_df = pd.DataFrame(results, columns=['Date', 'Temperature'])

        # close session
        session.close()

        # Convert the query results to a Dictionary using `date` as the key and `tobs` as the value.
        data = tobs_df.to_dict(orient="records")
        return (data)

    def query_tobs_sql(self):
        # Find the Date and Temperature of the most active station for the last year of data
        query = """
                SELECT date as Date, tobs as Temperature
                FROM measurement
                WHERE station = (SELECT station
                                FROM measurement
                                GROUP BY station
                                ORDER BY COUNT(station) DESC
                                LIMIT 1)
                AND date >= (SELECT DATE(date, '-365 days')
                            FROM measurement
                            ORDER BY date DESC
                            LIMIT 1);
                """

        # Save the query results as a Pandas DataFrame
        tobs_df = pd.read_sql(text(query), con=self.engine)
        data = tobs_df.to_dict(orient="records")
        return (data)

    def query_temp_orm(self, start_date):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Perform a query to retrieve the data and precipitation scores
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).all()

        # close session
        session.close()

        # Save the query results as a Pandas DataFrame. Explicitly set the column names
        temp_df = pd.DataFrame(results, columns=['TMIN', 'TAVG', 'TMAX'])

        # Convert the query results to a Dictionary
        data = temp_df.to_dict(orient="records")
        return (data)

    def query_temp_sql(self, start_date):
        # Validate start_date is in the correct format
        # Code from the site discuss.python.org/t/best-way-to-validate-an-entered-date/49406/4
        try:
            dt.datetime.strptime(start_date, '%Y-%m-%d')
        except ValueError:
            return("Invalid date format. Please use the format 'YYYY-MM-DD'")
                   
        # Perform a query to retrieve the data and precipitation scores
        query = f""" 
            SELECT MIN(tobs) AS TMIN, AVG(tobs) AS TAVG, MAX(tobs) AS TMAX
            FROM measurement
            WHERE date >= '{start_date}';
            """
        # Save the query results as a Pandas DataFrame
        temp_df = pd.read_sql(text(query), con=self.engine)
        data = temp_df.to_dict(orient="records")
        return (data)

    def query_temp_range_orm(self, start_date, end_date):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Perform a query to retrieve the data and precipitation scores
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).\
            filter(Measurement.date <= end_date).all()
       
        # close session
        session.close()

        # Save the query results as a Pandas DataFrame. Explicitly set the column names
        temp_range_df = pd.DataFrame(results, columns=['TMIN', 'TAVG', 'TMAX'])

        # Convert the query results to a Dictionary
        data = temp_range_df.to_dict(orient="records")
        return (data)

    def query_temp_range_sql(self, start_date, end_date):
        # Validate start_date and end_date are in the correct format
        # Code from https://discuss.python.org/t/best-way-to-validate-an-entered-date/49406/4
        # Check if the date is in the correct format for start_date
        try:
            dt.datetime.strptime(start_date, '%Y-%m-%d')
        except ValueError:
            return ("Invalid start_date format. Please use the format 'YYYY-MM-DD'")
        # Check if the date is in the correct format for end_date
        try:
            dt.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return ("Invalid end_date format. Please use the format 'YYYY-MM-DD'")
                   
        # Perform a query to retrieve the data and precipitation scores
        query = f"""
            SELECT MIN(tobs) AS TMIN, AVG(tobs) AS TAVG, MAX(tobs) AS TMAX
            FROM measurement
            WHERE date >= '{start_date}'
            AND date <= '{end_date}';
            """

        # Save the query results as a Pandas DataFrame
        temp_df = pd.read_sql(text(query), con=self.engine)
        data = temp_df.to_dict(orient="records")
        return (data)
    