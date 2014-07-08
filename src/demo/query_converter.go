package main
import (
	"os"
	"log"
	"bufio"
	"fmt"
	"time"
	"strings"
	. "influxdb-go"
	"regexp"
	"strconv"
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
	tokenizedQuery := strings.Fields(rgmQuery)	
	//tokenizedQuery[0] = strings.Replace(tokenizedQuery[0], "\"", "", -1)
	client, err := NewClient(&ClientConfig{})
	if err != nil {
		fmt.Println("error occured!")
	}
	switch tokenizedQuery[0] {
	case idQuery, idQ:
		rgmQ := "select * from " + timeSeries //+ " where num_vals_tm > " + tokenizedQuery[len(tokenizedQuery) - 2] + " and num_vals_tm < " + tokenizedQuery[len(tokenizedQuery) - 1]
		results, err := client.Query(rgmQ)
		if err != nil {
			fmt.Println("ANOTHER ERROR!")
		}
		
		keywords := make(map[string]int)
		for i := 1; i < len(tokenizedQuery); i++ {
			keywords[tokenizedQuery[i]] = 1
		}	
	
		pointIndices := []int{}	
		for index := range results {
			fmt.Println(reflect.TypeOf(results))
			points := results[index].GetPoints()
			for i, point := range points {
				pointKeywords := make(map[string]int)
				for key := range keywords {
					pointKeywords[key] = 1
				}	
				for _, elem := range point {
					for keyword := range keywords {
						if str, ok := elem.(string); ok {
							match, _ := regexp.MatchString(keyword, str)
							if match == true {
								delete(pointKeywords, keyword)
							}
						}
					}
				}
				if len(pointKeywords) == 0 {
					pointIndices = append(pointIndices, i)
				}
			}
			if len(pointIndices) == 0 {
				fmt.Print("203")
			} else if len(pointIndices) == 1 {
				fmt.Print("201")
			} else if len(pointIndices) > 1 {
				fmt.Print("202")
			}
			fmt.Println(", " + strconv.Itoa(len(pointIndices)) + " matches found.")	
			for count := 0; count < len(pointIndices); count++ {
				fmt.Println(points[pointIndices[count]])
			}
		}
		return rgmQ
	
	case keyQuery, keyQ:
		var results [][]*Series
		rgmQ := ""
		if strings.EqualFold(tokenizedQuery[1], "*") {
			rgmQ := "select * from " + timeSeries
			results, err := client.Query(rgmQ)
			if err != nil {
				fmt.Println("Invalid query!")
				return rgmQ
			}
			fmt.Println(results)
		} else {
			for counter := 1; counter < len(tokenizedQuery); counter++ {
				rgmQ := "select * from " + timeSeries + " where num_vals_id = " + tokenizedQuery[counter]
				result, err := client.Query(rgmQ)
				if err != nil {
					fmt.Println("Invalid Query!")
					return rgmQ
				}
				//results[counter] = result
				results = append(results, result)
			}
		}
		
		//results, err := client.Query(rgmQ)
		if err != nil {
			fmt.Println("Another err!")
		}
		
		fmt.Println(reflect.TypeOf(results))
		/*
		for elem := results.Front(); elem != nil; elem = elem.Next() {
			points := results[index].GetPoints()
			fmt.Println(reflect.TypeOf(elem))
			points = elem.GetPoints()	
			if len(points) == 0 {
				fmt.Print("203")
			} else if len(points) == 1 {
				fmt.Print("201")
			} else if len(points) > 1 {
				fmt.Print("202")
			}
			fmt.Println(", " + strconv.Itoa(len(points)) + " matches found.")
			for _,point := range points {
				fmt.Println(point[2])
				for _, elem := range point {
					if str, ok := elem.(string); ok {
						fmt.Print(str + " ")
					}
				}
				fmt.Println()
			}
			
		}
		*/
		
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
