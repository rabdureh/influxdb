package main
import (
	"os"
	"log"
	"bufio"
	"fmt"
	"time"
	"strings"
	"strconv"
	//"encoding/json"
	. "influxdb-go"
	//"regexp"
	"reflect"
)

const (
	idQ = "QI"
	idQuery = "Query-Ids"
	keyQ = "QK"
	keyQuery = "Query-Keywords"
	tsQ = "QT"
	tsQuery = "Query-Timeseries"
	curQ = "QC"
	curQuery = "Query-Current"
	// folQ = "QF"   // Not sure if this works or not
	folQuery = "Query-Follow"
	scQ = "SC"
	scQuery = "Sub-Current"
	stQ = "ST"
	stQuery = "Sub-Timeseries"
	unsubQ = "UN"
	unsubQuery = "Unsubscribe"
	qsubQ = "QS"
	qsubQuery = "Query-Sub"
	timeSeries = "ts_data.txt"
)

/*
Need a way of determining if the user is subscribed in the first place
The subscription stuff should really go in the actual influx code
*/
type RGMQuery struct {
	Type		string
	Id 			int
	Keywords 	string
	Start_Tm	time.Time
	End_Tm		time.Time
}

/*
Figure out how to handle a request to allow for regex exploration
*/
type RGMCommand struct {
	Command		string
	TimeSeries 	string
	// Necessary for some, but not all of the possible commands
	source		string
	target		string
	// Other possible input parameters
	update		string
	dryrun		string
	verbose		string
	merge		string
	end 		string
	reindex		string
	match		string
	not_match	string
	normalize	string
	replace		string
}

func QueryHandler(rgmQuery string) (string) {
	//fmt.Println(rgmQ)
	tokenizedQuery := strings.Fields(rgmQuery)	
	tokenizedQuery[0] = strings.Replace(tokenizedQuery[0], "\"", "", -1)
	switch tokenizedQuery[0] {
	case idQuery, idQ:
		rgmQ := "select * from " + timeSeries 
		/*if !(strings.EqualFold(tokenizedQuery[1], "")) {
			rgmQ = rgmQ + " where "
			for index := 1; index < len(tokenizedQuery) - 2; index++ {
				if len(keyValuePair) == 2 {
					keyValuePair[0] = strings.Replace(keyValuePair[0], "\"", "", -1)
					keyValuePair[1] = strings.Replace(keyValuePair[1], "\"", "", -1)
					rgmQ = rgmQ + keyValuePair[0] + " =~ /" + keyValuePair[1] + "/"
					if (len(tokenizedQuery) - index > 3) {
						rgmQ = rgmQ + " and "
					}
				} else {
					log.Fatal("Query has an incomplete key-value pair.")
					return ""
				}
				
				
			}

			rgmQ = rgmQ + " and "
		} else {
			rgmQ = rgmQ + " where "
		}*/
		
		rgmQ = rgmQ + " where num_vals_tm > " + tokenizedQuery[len(tokenizedQuery) - 2] + " and num_vals_tm < " + tokenizedQuery[len(tokenizedQuery) - 1]
		
		client, err := NewClient(&ClientConfig{})
		if err != nil {
			fmt.Println("error occured!")
		}
		results, err := client.Query(rgmQ)
		if err != nil {
			fmt.Println("ANOTHER ERROR!")
		}
		// fmt.Print("RESULTS: ")
		for index := range results {
			points := results[index].GetPoints()
			if len(points) == 0 {
				fmt.Println("203")
			} else if len(points) == 1 {
				fmt.Println("201")
			} else if len(points) > 1 {
				fmt.Println("202")
			}
			
			for _,point := range points {
				for index, elem := range point {
					if e, ok := elem.(float64); ok {
						fmt.Println(index)
						point[index] := strconv.FormatFloat(e, 'f', -1, 64)
					}
					fmt.Print("TYPE: ")
					fmt.Println(reflect.TypeOf(elem))
					//match, _ := regexp.MatchString(tokenizedQuery[1], elem) 
					/*if match == true {
						fmt.Println(point)
					}*/	
				}
			}
		}
		return rgmQ
	
	case keyQuery, keyQ:
		rgmQ := "select num_vals_keywords from " + timeSeries + " where num_vals_id = "
		return rgmQ
	case tsQuery, tsQ:
		rgmQ := ""
		return rgmQ
	case curQuery, curQ:
		rgmQ := ""
		return rgmQ
	case folQuery:
		rgmQ := ""
		return rgmQ
	case scQuery, scQ:
		rgmQ := ""
		return rgmQ
	case stQuery, stQ:
		rgmQ := ""
		return rgmQ
	case unsubQuery, unsubQ:
		rgmQ := ""
		return rgmQ
	case qsubQuery, qsubQ:
		rgmQ := ""
		return rgmQ
	default:
		log.Fatal("%s is an unrecognized query type - see documentation for allowed query types", rgmQuery)
	} 
	
	return rgmQuery
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("ENTER QUERY: ")
	for scanner.Scan() {
		QueryHandler(scanner.Text())
		/*fmt.Println(query)
		client, err := NewClient(&ClientConfig{})
		if err != nil {
			fmt.Println("error occured!")
		}
		results, err := client.Query(query)
		if err != nil {
			fmt.Println("ANOTHER ERROR!")
		}
		// fmt.Print("RESULTS: ")
		for index := range results {
			points := results[index].GetPoints()
			if len(points) == 0 {
				fmt.Println("203")
			} else if len(points) == 1 {
				fmt.Println("201")
			} else if len(points) > 1 {
				fmt.Println("202")
			}
			
			for _,point := range points {
				fmt.Println(point)
			}
		}
		*/
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		
		fmt.Print("ENTER QUERY: ")
	}
}
