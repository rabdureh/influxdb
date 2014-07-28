package main

import (
	. "influxdb-go"
	. "query_converter"
	"fmt"
	"strconv"
	//"strings"
	//"reflect"
)
const (
	qiq1 = "Query-Ids" 
	qiq2 = "Query-Ids pool:ixltrade" 
	qiq3 = "Query-Ids pool:ixltrade location:ixl *" 
	qiq4 = "Query-Ids pool:ixltrade * 2010-01-01 12:00:00"
	qiq5 = "QI location:ixl * 2014-05-14 12:00:00 2014-05-20 12:00:00" 
	qiq6 = "QI location:ixl * 2015-01-01 12:00:00"
	qir1 = "select * from /.*/"
	qir2 = "select * from \"pool:ixltrade\""
	qir3 = "select * from /pool:ixltrade location:ixl/"
	qir4 = "select * from /pool:ixltrade/ where time > '2010-01-01 12:00:00'" 
	qir5 = "select * from /location:ixl/ where time > '2014-05-14 12:00:00' and time < '2014-05-20 12:00:00'"
	qir6 = "select * from /location:ixl/ where time > '2015-01-01 12:00:00'"

	qtq1 = "Query-Timeseries type:size_traded_total 2014-01-01 12:00:00"
	qtq2 = "QT type:size_traded_total * 2014-01-01 12:00:00"
	qtq3 = "Query-Timeseries location:ixl pool:ixltrade type:size_traded_total 2014-05-01 12:00:00"
	qtq4 = "QT * 2000-01-01 00:00:00"
	qtq5 = "Query-Timeseries * 2015-01-01 12:00:00"
	qtr1 = "select * from \"type:size_traded_total\" where time > '2014-01-01 12:00:00'" 
	qtr2 = "select * from /type:size_traded_total/ where time > '2014-01-01 12:00:00'"
	qtr3 = "select * from \"location:ixl pool:ixltrade type:size_traded_total\" where time > '2014-01-01 12:00:00'"
	qtr4 = "select * from /.*/ where time > '2000-01-01 00:00:00'"
	qtr5 = "select * from /.*/ where time > '2015-01-01 12:00:00'"

	qcq1 = "Query-Current pool:ixltrade *"
	qcq2 = "QC location:ixl pool:ixltrade type:norders_total"
	qcq3 = "Query-Current * 2015-01-01 00:00:00"
	qcq4 = "QC location:ixl"
	qcq5 = "Query-Current * 2014-01-01 12:00:00"
	qcr1 = "select * from /pool:ixltrade/ limit 1"
	qcr2 = "select * from \"location:ixl pool:ixltrade type:norders_total\" limit 1"
	qcr3 = "select * from /.*/ where time < '2015-01-01 00:00:00' limit 1"
	qcr4 = "select * from \"location:ixl\" limit 1"
	qcr5 = "select * from /.*/ where time < '2014-01-01 12:00:00' limit 1"

)

func main() {
	client, _ := NewClient(&ClientConfig{})
	if testQueryIds(client) != true {
		fmt.Println("Query-Ids FAILED at least one test!")
	} else {
		fmt.Println("Query-Ids passed unit tests!")
	}
	if testQueryTS(client) != true {
		fmt.Println("Query-TS FAILED at least one test!")
	} else {
		fmt.Println("Query-TS passed unit tests!")
	}
	if testQueryCur(client) != true {
		fmt.Println("Query-Current FAILED at least one test!")
	} else {
		fmt.Println("Query-Current passed unit tests!")
	}

}

func testQueryIds(client *Client) (bool) {
	wrapperQueries := []string{qiq1, qiq2, qiq3, qiq4, qiq5, qiq6}
	clientQueries := []string{qir1, qir2, qir3, qir4, qir5, qir6}
	for i := range wrapperQueries {
		fmt.Printf("Wrapper query: %v\n", wrapperQueries[i])
		fmt.Println("Wrapper results: ")
		wrapperResults, _ := QueryHandler(wrapperQueries[i])
		clientResults, _ := client.Query(clientQueries[i])
		fmt.Printf("Client query: %v\n", clientQueries[i])
		fmt.Println("Client results: ")
		numResults := len(clientResults)
		if numResults >= 1 {
			fmt.Printf("202, %v match found.\n", numResults)
		} else if numResults == 0 {
			fmt.Printf("203, %v matches found.\n", numResults)
		} else {
			fmt.Printf("Possible error/warning!\n")
		}
		for _, series := range clientResults {
			for _, maxPoint := range series.GetPoints() {
				fmt.Printf("%v\t %v\t %v\t\n", series.GetName(), maxPoint[0], maxPoint[2])
			}
		}
		if compareResults(wrapperResults, clientResults) != true {
			fmt.Printf("\nFailed test %v in query-ids!\n", i)
			return false
		} else {
			fmt.Printf("\nPassed test %v in query-ids!\n\n", i)
		}
	}
	return true
}

func testQueryTS(client *Client) (bool) {
	wrapperQueries := []string{qtq1, qtq2, qtq3, qtq4, qtq5}
	clientQueries := []string{qtr1, qtr2, qtr3, qtr4, qtr5}
	for i := range wrapperQueries {
		fmt.Println("Wrapper results: ")
		fmt.Printf("Wrapper query: %v\n", wrapperQueries[i])
		wrapperResults, _ := QueryHandler(wrapperQueries[i])
		clientResults, _ := client.Query(clientQueries[i])
		fmt.Printf("Client query: %v\n", clientQueries[i])
		fmt.Println("Client results: ")
		numResults := len(clientResults)
		if numResults >= 1 {
			fmt.Printf("202, %v match found.\n", numResults)
		} else if numResults == 0 {
			fmt.Printf("203, %v matches found.\n", numResults)
		} else {
			fmt.Printf("Possible error/warning!\n")
		}
		for _, series := range clientResults {
			for _, maxPoint := range series.GetPoints() {
				fmt.Printf("%v\t %v\t %v\t\n", series.GetName(), maxPoint[0], maxPoint[2])
			}
		}
		if compareResults(wrapperResults, clientResults) != true {
			fmt.Printf("\nFailed test %v in query-ts!\n", i)
			return false
		} else {
			fmt.Printf("\nPassed test %v in query-ts!\n\n", i)
		}
	}
	return true
}

func testQueryCur(client *Client) (bool) {
	wrapperQueries := []string{qcq1, qcq2, qcq3, qcq4, qcq5}
	clientQueries := []string{qcr1, qcr2, qcr3, qcr4, qcr5}
	for i := range wrapperQueries {
		fmt.Printf("Wrapper query: %v\n", wrapperQueries[i])
		fmt.Println("Wrapper results: ")
		wrapperResults, _ := QueryHandler(wrapperQueries[i])
		clientResults, _ := client.Query(clientQueries[i])
		fmt.Printf("Client query: %v\n", clientQueries[i])
		fmt.Println("Client results: ")
		numResults := len(clientResults)
		if numResults >= 1 {
			fmt.Printf("202, %v match found.\n", numResults)
		} else if numResults == 0 {
			fmt.Printf("203, %v matches found.\n", numResults)
		} else {
			fmt.Printf("Possible error/warning!\n")
		}
		for _, series := range clientResults {
			for _, maxPoint := range series.GetPoints() {
				fmt.Printf("%v\t %v\t %v\t\n", series.GetName(), maxPoint[0], maxPoint[2])
			}
		}
		if compareResults(wrapperResults, clientResults) != true {
			fmt.Printf("\nFailed test %v in query-current!\n", i)
			return false
		} else {
			fmt.Printf("\nPassed test %v in query-current!\n\n", i)
		}
	}
	return true
}

func compareResults(wrapperResults []*Series, clientResults []*Series) (bool) {
	for _, series := range wrapperResults {
		matchFound := false
		for _, secSeries := range clientResults {
			if ptsToString(series.GetPoints()) == ptsToString(secSeries.GetPoints()) && series.GetName() == secSeries.GetName() && compareSlices(series.GetColumns(), secSeries.GetColumns()) {
				matchFound = true
			}
			
		}
		if matchFound != true {
			return false
		}
	}
	return true
}

/*
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
*/

func ptsToString(pts [][]interface{}) (string) {
	s := "["
	for _, pt := range pts {
		for i, elem := range pt {
			s += strconv.FormatFloat(elem.(float64), 'e', -1, 64)
			if i < len(pt) - 1 {
				s += " "
			}
		}
	}
	s += "]"
	//fmt.Printf("S: %v\n", s)
	return s
}

func compareSlices(slice1 []string, slice2 []string) bool {
	for i := range slice1 { if slice1[i] != slice2[i] { return false } }
	return true
}
