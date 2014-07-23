import time
from influxdb import InfluxDBClient

HOST = 'localhost'
PORT = 8086
USER = 'root'
PASSWORD = 'root'
DBNAME = 'mydb'

client = InfluxDBClient(HOST, PORT, USER, PASSWORD, DBNAME)
t = int(time.time())
insert_ts = [{
	     "name": "location:ixl pool:ixltrade type:nlost_total",
	     "columns": ["time", "value"],
	     "points": [[t, 0.5]]}]

print insert_ts
client.write_points_with_precision(insert_ts, 's')
