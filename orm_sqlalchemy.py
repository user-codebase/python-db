from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, Date, inspect, text, select, update, delete
import csv
from datetime import datetime

engine = create_engine("sqlite:///database.db", echo=True)
meta = MetaData()

stations = Table(
    "stations", meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)

measurements = Table(
    "measurements", meta,
    Column("station", String),
    Column("date", Date),
    Column("precip", Float),
    Column("tobs", Integer),
)

def save_data(conn, list_with_data, table):    
    transaction = conn.begin()
    conn.execute(table.insert(), list_with_data)
    transaction.commit()

def extract_stations():
    with open("clean_stations.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        list_with_data = list(reader)
        return list_with_data

def extract_measurements():
    with open("clean_measure.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        list_with_data = []
        for item in reader:
            list_with_data.append({
                "station": item["station"],
                "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                "precip": float(item["precip"]),
                "tobs": int(item["tobs"]),
            })
        return list_with_data
    
def execute_sql(conn, sql):
    return conn.execute(text(sql)).fetchall()

def print_two_columns_from_stations(conn):
    print("Selected columns from stations:")
    sql = select(stations.c.station, stations.c.name)
    result = conn.execute(sql)
    for row in result:
        print(row.station, row.name)

def delete_station_with_code(conn, station):
    sql = delete(stations).where(stations.c.station == station)
    conn.execute(sql)
    conn.commit()

def update_station_name(conn, station_old_name, station_new_name):
    sql = update(stations).where(stations.c.name == station_old_name).values(name=station_new_name)
    conn.execute(sql)
    conn.commit()
    
def select_measurements_with_tobs_above_85(conn):
    print("Measurements with tobs above 85:")
    sql = select(measurements.c.station, measurements.c.tobs).where(measurements.c.tobs > 85)
    result = conn.execute(sql)
    for row in result:
        print(row.station, row.tobs)

if __name__ == '__main__':
    meta.create_all(engine)
    inspector = inspect(engine)
    print(inspector.get_table_names())
    conn = engine.connect()
    if conn:
        list_with_stations = extract_stations()
        list_with_measurements = extract_measurements()
        save_data(conn, list_with_stations, stations)
        save_data(conn, list_with_measurements, measurements)
        sql = "SELECT * FROM stations LIMIT 5"
        for row in execute_sql(conn, sql):
            print(row)
        print_two_columns_from_stations(conn)
        delete_station_with_code(conn, "USC00519397")
        update_station_name(conn, "PEARL CITY", "NEW NAME")
        select_measurements_with_tobs_above_85(conn)
        conn.close()