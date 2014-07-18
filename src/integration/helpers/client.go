package helpers

import (
	influxdb "../../demo/influxdb-go" //"github.com/influxdb/influxdb-go"
	. "gopkg.in/check.v1"
)


type Client interface {
	RunQuery(query string, c *C, timePrecision ...influxdb.TimePrecision) []*influxdb.Series
	RunQueryWithNumbers(query string, c *C, timePrecision ...influxdb.TimePrecision) []*influxdb.Series
	RunInvalidQuery(query string, c *C, timePrecision ...influxdb.TimePrecision) []*influxdb.Series
	WriteData(series []*influxdb.Series, c *C, timePrecision ...influxdb.TimePrecision)
	WriteJsonData(series string, c *C, timePrecision ...influxdb.TimePrecision)
}
