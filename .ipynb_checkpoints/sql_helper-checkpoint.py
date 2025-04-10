from sqlalchemy import create_engine, func, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd
#################################################
# Database Setup
#################################################
    # Create engine using the `hawaii.sqlite` database file
class SQLHelper():
    def __init__(self):
        self.engine = create_engine("sqlite:////Users/aryamaredia/Desktop/sqlalchemy-challenge/hawaii.sqlite")        
        self.Base = automap_base()
        self.Base.prepare(self.engine, reflect=False)
        self.Measurement = self.Base.classes.measurement
        self.Station = self.Base.classes.station

        
# Declare a Base using `automap_base()`
        def get_most_active_stations(self):
            results = self.session.query(
                self.Measurement.station,
                func.count(self.Measurement.id).label('num_observations')
            ).group_by(self.Measurement.station).order_by(func.count(self.Measurement.id).desc()).all()

            return pd.DataFrame(results, columns=["Station", "Observation Count"])
        def check_measurement_rows(self):
            result = self.session.query(func.count(self.Measurement.id)).scalar()
            print(f"Total measurement rows: {result}")



# Use the Base class to reflect the database tables
# Assign the measurement class to a variable called `Measurement` and
# the station class to a variable called `Station'
        Station = self.Base.classes.station
        return(Station)


    def createMeasurement(self):
      
        Base = automap_base()
        Base.prepare(autoload_with=self.engine)
        Measurement = Base.classes.measurement
        return(Measurement)

    def queryPrecipitationORM(self):
    
        session = Session(self.engine)
        rows = session.query(self.Measurement.id, self.Measurement.station, self.Measurement.date, self.Measurement.prcp).filter(self.Measurement.date >= '2016-08-23').order_by(self.Measurement.date).all()
        df = pd.DataFrame(rows)
        session.close()
        return(df)

    def queryPrecipitationSQL(self):
    
        conn = self.engine.connect() 

        query = text("""SELECT
                    id,
                    station,
                    date,
                    prcp
                FROM
                    measurement
                WHERE
                    date >= '2016-08-23'
                ORDER BY
                    date;""")
        df = pd.read_sql(query, con=conn)

        conn.close()
        return(df)

    def queryStationsSQL(self):
        conn = self.engine.connect() 
        query = text("""SELECT
                        station,
                        name,
                        latitude,
                        longitude,
                        elevation
                FROM
                    station
                ORDER BY
                    station;""")
        df = pd.read_sql(query, con=conn)


        conn.close()
        return(df)

    def queryStationsORM(self):
            
        session = Session(self.engine)

        rows = session.query(self.Station.station, self.Station.name, self.Station.latitude, self.Station.longitude, self.Station.elevation).order_by(self.Station.station).all()

        df = pd.DataFrame(rows)

        session.close()
        return(df)

    def queryTemperatureSQL(self):
    
        conn = self.engine.connect()

        query = text("""SELECT
                        id,
                        station,
                        date,
                        tobs
                FROM
                    measurement
                WHERE
                    station = 'USC00519281'
                    and date >= '2016-08-23'
                ORDER BY
                        date;""")
        df = pd.read_sql(query, con=conn)

            
        conn.close()
        return(df)

    def queryTemperatureORM(self):
    
        session = Session(self.engine)

        rows = session.query(self.Measurement.id, self.Measurement.station, self.Measurement.date, self.Measurement.tobs).filter(self.Measurement.station == 'USC00519281').filter(self.Measurement.date >= '2016-08-23').order_by(self.Measurement.date).all()

        df = pd.DataFrame(rows)


        session.close()
        return(df)

    def queryTStatsSQL(self, start):
        conn = self.engine.connect() 

        query = text(f"""SELECT
                    min(tobs) as min_tobs,
                    max(tobs) as max_tobs,
                    avg(tobs) as avg_tobs
                FROM
                    measurement
                WHERE
                    date >= '{start}';""")
        print(query)
        df = pd.read_sql(query, con=conn)

    
        conn.close()
        return(df)

    def queryTStatsORM(self, start):
            
        session = Session(self.engine)

        rows = session.query(func.min(self.Measurement.tobs).label('min_tobs'), func.max(self.Measurement.tobs).label('max_tobs'), func.avg(self.Measurement.tobs).label('avg_tobs')).filter(self.Measurement.date >= start).all()
        
        df = pd.DataFrame(rows)

        session.close()
        return(df)

    def queryTStats_StartEndSQL(self, start, end):
        conn = self.engine.connect() # Raw SQL/Pandas
        query = text(f"""SELECT
                    min(tobs) as min_tobs,
                    max(tobs) as max_tobs,
                    avg(tobs) as avg_tobs
                FROM
                        measurement
                WHERE
                        date >= '{start}'
                        AND date <= '{end}';""")
        print(query)
        df = pd.read_sql(query, con=conn)

        # Close the connection
        conn.close()
        return(df)

    def queryTStats_StartEndORM(self, start, end):
        session = Session(self.engine)
        rows =  session.query(func.min(self.Measurement.tobs).label('min_tobs'), func.max(self.Measurement.tobs).label('max_tobs'), func.avg(self.Measurement.tobs).label('avg_tobs')).filter(self.Measurement.date >= start).filter(self.Measurement.date <= end).all()
        df = pd.DataFrame(rows)

        # Close the Session
        session.close()
        return(df)
    
if __name__ == "__main__":
            helper = SQLHelper()
            df = helper.get_most_active_stations()
            print(df.head())