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
	//"reflect"
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
	year = "[0-9]{4,4}"
	month = "[0-9]{2,2}"
	day = "[0-9]{2,2}"
	hour = "[0-9]{2,2}"
	min = "[0-9]{2,2}"
	sec = "[0-9]{2,2}"
	ymdhmsz = year + "-" + month + "-" + day + " " + hour + ":" + min + ":" + sec
	mdyhmsz = month + "-" + day + "-" + year + " " + hour + ":" + min + ":" + sec
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
	client, err := NewClient(&ClientConfig{})
	if err != nil {
		fmt.Println("error occured!")
	}
	//starttime := ""
	//endtime := ""
	starttimefound := false
	var starttimeunix int64
	var endtimeunix int64
	starttime, err := strconv.ParseInt(tokenizedQuery[len(tokenizedQuery) - 1], 10, 64)
	//starttimeunix = starttime
	if err != nil && starttimeunix > 1000 {
		starttimeunix = starttime
		starttimefound = true
		//fmt.Println("First Case!")
		//starttimeunix = tokenizedQuery[len(tokenizedQuery) - 1]
		time, err := strconv.ParseInt(tokenizedQuery[len(tokenizedQuery) - 2], 10, 64)
		if err != nil && time > 1000 {
			endtimeunix = starttimeunix
			starttimeunix = time
			//starttimeunix = tokenizedQuery[len(tokenizedQuery) - 2]
		}
	} else if (isDateTime(tokenizedQuery[len(tokenizedQuery) - 2] + " " + tokenizedQuery[len(tokenizedQuery) - 1])) { 
		fmt.Println("Second Case!")
		starttime, _ := getDateFromString(tokenizedQuery[len(tokenizedQuery) - 2] + " " + tokenizedQuery[len(tokenizedQuery) - 1])
		starttimeunix = starttime.Unix()
		starttimefound = true
		if (isDateTime(tokenizedQuery[len(tokenizedQuery) - 4] + " " + tokenizedQuery[len(tokenizedQuery) - 3])) == true {
			endtimeunix = starttimeunix
			starttime, _ := getDateFromString(tokenizedQuery[len(tokenizedQuery) - 4] + " " + tokenizedQuery[len(tokenizedQuery) - 3]) 
			starttimeunix = starttime.Unix()
		}
	}
	switch tokenizedQuery[0] {
	case idQuery, idQ:
		rgmQ := "select * from " 
		keywordBuffer := 0	
		rgmQEnd := ""
		if starttimeunix >= 0 && starttimefound == true {
			fmt.Println("Found start time!")
			rgmQEnd = " where num_vals_tm > " + strconv.FormatInt(starttimeunix, 10)
			keywordBuffer += 2
		}
		if endtimeunix > 0 {
			fmt.Println("Found end time!")
			rgmQEnd = rgmQEnd + " and num_vals_tm < " + strconv.FormatInt(endtimeunix, 10)
			keywordBuffer += 2
		}
		if err != nil {
			fmt.Println("ANOTHER ERROR!")
		}
	
		rgmQ = rgmQ + "\""
		for i := 1; i < len(tokenizedQuery) - keywordBuffer; i++ {
			rgmQ = rgmQ + tokenizedQuery[i]
			if i < len(tokenizedQuery) - keywordBuffer - 1 {
				rgmQ = rgmQ + " "
			}
		
		}	
		rgmQ = rgmQ + "\""	
		rgmQ = rgmQ + rgmQEnd
		fmt.Printf("Influx Query: %v\n", rgmQ)
		results, err := client.Query(rgmQ)
		if err != nil {
			fmt.Printf("Results: %v\n", results)
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		/*	
		for index := range results {
			pointIndices := []int{}
			points := results[index].GetPoints()
			for i, point := range points {
				pointKeywords := make(map[string]int)
				for key := range keywords {
					pointKeywords[key] = 1
				}
				pointMatches := []string{}
				for keyword := range keywords {
					for _, elem := range point {
						if str, ok := elem.(string); ok {
							match, _ := regexp.MatchString(keyword, str)
							//fmt.Println("Expression: " + keyword)
							//fmt.Println("String: " + str)
							if match == true {
								alreadyUsed := false
								for _, word := range pointMatches {
									if strings.EqualFold(word, str) {
										alreadyUsed = true
									}
								}
								if !alreadyUsed {
									delete(pointKeywords, keyword)
									//fmt.Println("MATCH!")
								}
							}
						}
					}
				}
				if len(pointKeywords) == 0 || (len(pointKeywords) == 1 && pointKeywords["*"] == 1) {
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
				fmt.Printf("%v\t %v\n", points[pointIndices[count]][2], points[pointIndices[count]])
			}
		}
		*/
		return rgmQ
	
	case keyQuery, keyQ:
		var results [][]*Series
		rgmQ := ""
		if strings.EqualFold(tokenizedQuery[1], "*") {
			rgmQ := "select * from " + timeSeries
			//fmt.Println(results)
			results, err := client.Query(rgmQ)
			if err != nil {
				fmt.Println("Invalid query!")
				return rgmQ
			}
			//fmt.Println("Found a placeholder!")
			fmt.Println(results)
		} else {
			for counter := 1; counter < len(tokenizedQuery); counter++ {
				rgmQ := "select * from " + timeSeries + " where num_vals_id = " + tokenizedQuery[counter]
				result, err := client.Query(rgmQ)
				if err != nil {
					fmt.Println("Invalid Query!")
					return rgmQ
				}
				results = append(results, result)
			}
		}
		if err != nil {
			fmt.Println("Another err!")
		}
		
		for _, result := range results {
			//points := results[index].GetPoints()
			//fmt.Println("Looping")	
			//fmt.Println(reflect.TypeOf(results[index]))
			for _, elem := range result {
				points := elem.GetPoints()	
				if len(points) == 0 {
					fmt.Print("203")
				} else if len(points) == 1 {
					fmt.Print("201")
				} else if len(points) > 1 {
					fmt.Print("202")
				}
				fmt.Println(", " + strconv.Itoa(len(points)) + " matches found.")
				for _,point := range points {
					fmt.Printf("%v\t %v\n", point[2], point)
				}
			}
		}
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

func isDateTime(datetime string) (bool) { 
	//fmt.Printf("Datetime: %v\n", datetime)
	//re := regexp.MustCompile(month)
	//fmt.Printf("Month: %v\n", re.FindString(datetime))
	//strings.Split(datetime, "-")
	//fmt.Println(len(strings.Split(datetime, " ")))
	date := strings.Split(datetime, " ")[0]
	time := strings.Split(datetime, " ")[1]
	ymd := strings.Split(date, "-")
	hms := strings.Split(time, ":")
	match, _ := regexp.MatchString(ymdhmsz, datetime) 
	if match == true {
		//ymd := strings.Split(datetime, "-")
		//hms := strings.Split(datetime, ":")
		if isValidDate(ymd, true) && isValidTime(hms) {
			//fmt.Println("FOUND YMDHMSZ!!")
			return true
		}
	} else {
		match, _ := regexp.MatchString(mdyhmsz, datetime)
		if match == true {
			if isValidDate(ymd, false) && isValidTime(hms) {
				//fmt.Println("FOUND MDYHMSZ!!")
				return true
        		}
		}
	}       
	return false 
}

// If YMD format is passed the isymd is true, otherwise false.

func isValidDate(date []string, isymd bool) (bool) {
	//fmt.Printf("Date: %v\n", date)
	intDates := []int64{}
	for _, val := range date {
		intDate, err := strconv.ParseInt(val, 10, 32)
		intDates = append(intDates, intDate)
		if err != nil {
			return false
		}
		//fmt.Println(reflect.TypeOf(val))
	}
	if (isymd) {
		if (intDates[1] >= 1 && intDates[1] <= 12 && intDates[2] >= 1 && intDates[2] <= 31) {
			return true
		}
	} else {
		if (intDates[0] >= 1 && intDates[0] <= 12 && intDates[1] >= 1 && intDates[1] <= 31) {
			return true
		}
	}
	return false
}

func isValidTime(time []string) (bool) {
	intTimes := []int64{}
	for _, val := range time {
		intTime, err := strconv.ParseInt(val, 10, 32)
		intTimes = append(intTimes, intTime)
		if err != nil {
			return false
		}
	} 
	if intTimes[0] >= 0 && intTimes[0] <= 23 && intTimes[1] >= 0 && intTimes[1] <= 59 && intTimes[2] >= 0 && intTimes[2] <= 59 {
		return true
	}
	return false
}

func getDateFromString (datestring string) (time.Time, error) {
	//m, _ := strconv.ParseInt(datestring[5:7], 10, 0)
	date := strings.Split(datestring, " ")[0]
	timestring := strings.Split(datestring, " ")[1]
	ymd := strings.Split(date, "-")
	hms := strings.Split(timestring, ":")
	intDates := []int{}
	for _, val := range ymd {
		var intDate int
		int64Date, err := strconv.ParseInt(val, 0, 0)
		intDate = int(int64Date)
		//fmt.Println(reflect.TypeOf(strconv.ParseInt(val, 0, 0)))
		intDates = append(intDates, intDate)
		if err != nil {
			return time.Now(), err
		}
	}
	intTimes := []int{}
	for _, val := range hms {
		var intTime int
		int64Time, err := strconv.ParseInt(val, 0, 0)
		intTime = int(int64Time)
		//fmt.Println(reflect.TypeOf(strconv.ParseInt(val, 0, 0)))
		intTimes = append(intTimes, intTime)
		if err != nil {
			return time.Now(), err 
		}
	}
	//fmt.Printf("Date time: %v\n", intDates)
	//fmt.Printf("Int time: %v\n", intTimes)
	//return time.Now(), nil 
	return time.Date(intDates[0], time.Month(intDates[1]), intDates[2], intTimes[0], intTimes[1], intTimes[2], 0, time.UTC), nil
	//return time.Now() 
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("ENTER QUERY: ")
	for scanner.Scan() {
		QueryHandler(scanner.Text())
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		
		fmt.Print("ENTER QUERY: ")
	}
}
