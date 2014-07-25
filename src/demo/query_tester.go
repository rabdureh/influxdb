package main

import (
	. "influxdb-go"
	. "query_converter"
	"fmt"
	//"reflect"
)

func main() {
	client, _ := NewClient(&ClientConfig{})
	if testQueryIds(client) != true {
		fmt.Println("Query-Ids failed at least one test!")
	} else {
		fmt.Println("Query-Ids passed unit tests!")
	}


}

func testQueryIds(client *Client) (bool) {
	wrapperResults, _ := QueryHandler("Query-Ids")
	clientResults, _ := client.Query("select * from /.*/")
	matchFound := false
	for _, series := range wrapperResults {
		for _, secSeries := range clientResults {
			//fmt.Printf("wrapSer: %v\n", series)
			//fmt.Printf("clieSer: %v\n", secSeries)
			if pointsSlicesEqual(series.GetPoints(), secSeries.GetPoints()) {
				//matchFound = true
				fmt.Printf("1Ser: %v\n", series.GetPoints())
				fmt.Printf("2Ser: %v\n", secSeries.GetPoints())
				fmt.Println("MATCH!")
			}
		}
		if matchFound == true {
			fmt.Println("Found a match!")
		}
	}
	return true
}

func pointsSlicesEqual(s1 [][]interface{}, s2 [][]interface{}) (bool) {
	for _, point := range s1 {
		pointMatch := false
		for _, p := range s2 {
			//fmt.Printf("S1Point: %v\n", point)
			//fmt.Printf("S2Point: %v\n", p)
			if 
			}	
		}
		if pointMatch == false {
			return false
		}
	}
	return true
}

func contains(slice []*Point, pt *Point) {
	for _, p := range slice { if p == pt { return true } }
	return false
}
