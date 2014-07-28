#!/usr/bin/python
import re
from collections import defaultdict
from influxdb import InfluxDBClient

FILE_NAME = 'ts_data.txt'

HOST = 'localhost'
PORT = 8086
USER = 'root'
PASSWORD = 'root'
DBNAME = 'mydb'

client = InfluxDBClient(HOST, PORT, USER, PASSWORD, DBNAME)

# Uncomment when needed to make a new database with 'DBNAME'
client.create_database(DBNAME)

# Will want to change the file name in some cases
datafile = open(FILE_NAME).readlines()


def nonblank_lines(f):
    for l in f:
        line = l.rstrip()
        if line:
            yield line


ts_regex = re.compile(' tm=(\S+)\ id=(\S+)\ keywords=(\S+) value=([0-9.]+)')
timeseries = defaultdict(list)

for line in nonblank_lines(datafile):
    timeseries.clear()
    data = ts_regex.findall(line.strip())
    for ts in data:
	timeseries[ts[2]].append((int(float(ts[0]) * 1e6), float(ts[3])))
    insert_ts = [{"name": ts_key.replace("%20", " "),
                  "columns": ["time", "value"],
                  "points": timeseries[ts_key]} for ts_key in timeseries]
    print insert_ts
    client.write_points_with_precision(insert_ts, "u")
