# Import the dependencies.
import numpy as np
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect, desc

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
def OpenSession():
    return Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Helper Functions
#################################################

# Returns helpful dates from the datebase, as a tuple:
# year_start: start date for the last year of available data
# max_date  : latest date with any data
# Both dates are strings in %Y-%m-%d format
def dates(session):
    # Get the latest date with any measurements:
    max_date_result = session.query(func.max(Measurement.date)).all()
    max_date = max_date_result[0][0]

    # Calculate the date one year from the last date in data set.
    # I didn't find a direct way to subtract years, and since leap years
    # are a thing I don't want to use days or weeks. So just set the year.
    max_date_dt = dt.datetime.strptime(max_date, '%Y-%m-%d')
    year_start = dt.datetime(max_date_dt.year-1, max_date_dt.month, max_date_dt.day)
    
    print(f"year_start: {year_start}, max_date: {max_date}")
    return (year_start, max_date)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f'Available Routes:<br/>'
        f'<a href="/api/v1.0/precipitation">/api/v1.0/precipitation</a><br/>'
        f'<a href="/api/v1.0/stations">/api/v1.0/stations</a><br/>'
        f'<a href="/api/v1.0/tobs">/api/v1.0/tobs</a><br/>'
        f'/api/v1.0/&ltstart&gt<br/>'
        f'/api/v1.0/&ltstart&gt/&ltend&gt<br/>'
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Convert the query results from your precipitation analysis (i.e. retrieve only the last 12 months of data) to a dictionary using date as the key and prcp as the value."""
    result = "ERROR" # will be replaced if query succeeds
    with OpenSession() as session:
        start_date, _ = dates(session)

        # Perform a query to retrieve the data and precipitation scores
        last_year = session.query(Measurement.date, Measurement.prcp). \
            filter(Measurement.date >= func.strftime("%Y-%m-%d", start_date)). \
            order_by(Measurement.date).all()
        result_d = { date: prcp for date, prcp in last_year }
        result = jsonify(result_d)
    return result    
        
@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset."""
    result = "ERROR" # will be replaced if query succeeds
    with OpenSession() as session:
        stations = session.query(Station.station).all()
        result = jsonify(list(np.ravel(stations)))
    return result    

@app.route("/api/v1.0/tobs")
def tobs():
    """Return a JSON list of temperature observations for the previous year."""
    result = "ERROR" # will be replaced if query succeeds
    with OpenSession() as session:
        # Design a query to find the most active stations (i.e. which stations have the most rows?)
        # List the stations and their counts in descending order.
        station_counts = session.query(Measurement.station, func.count(Measurement.station)). \
            group_by(Measurement.station).order_by(desc(func.count(Measurement.station))).all()        
        active_id = station_counts[0][0]   
        
        # Perform a query to retrieve the data and precipitation scores
        start_date, _ = dates(session)
        last_year = session.query(Measurement.date, Measurement.prcp). \
            filter(Measurement.date >= func.strftime("%Y-%m-%d", start_date)). \
            filter(Measurement.station == active_id). \
            order_by(Measurement.date).all()   
        result = jsonify(list(np.ravel(last_year))) 
    return result    

@app.route("/api/v1.0/<start>")
def temps_start(start):
    """For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date."""
    result = "ERROR" # will be replaced if query succeeds
    with OpenSession() as session:
        _, max_date = dates(session)
        result = temps_start_end(start, max_date)
    return result

@app.route("/api/v1.0/<start>/<end>")
def temps_start_end(start, end):
    """For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive."""
    result = "ERROR" # will be replaced if query succeeds
    with OpenSession() as session:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)). \
            filter(Measurement.date >= func.strftime("%Y-%m-%d", start)). \
            filter(Measurement.date <= func.strftime("%Y-%m-%d", end)). \
            order_by(Measurement.date).all()
        result = jsonify(list(np.ravel(results)))
    return result    

# Enable debug logging when running directly
if __name__ == '__main__':
    app.run(debug=True)