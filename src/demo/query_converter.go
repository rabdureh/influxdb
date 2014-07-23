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
	//"../integration/helpers"
	//"../protocol"
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

func QueryHandler(rgmQuery string) ([]*Series, error) {
	retResults := []*Series{}
	tokenizedQuery := strings.Fields(rgmQuery)	
	client, err := NewClient(&ClientConfig{})
	if err != nil {
		fmt.Println("error occured!")
		return retResults, err
	}
	starttimeunix, endtimeunix, starttimefound := ParseTime(tokenizedQuery, false)
	switch tokenizedQuery[0] {
	case idQuery, idQ:
		if len(tokenizedQuery) == 1 {
			result, err := client.Query("select * from /.*/")
			if err != nil {
				fmt.Println("Could not complete query!")
				return []*Series{}, err
			}
			for _, series := range result {
				retResults = append(retResults, series)
			}
		} else {
			rgmQ := "select * from " 
			keywordBuffer := 0	
			rgmQEnd := ""
			starttime := ""
			if starttimeunix >= 0 && starttimefound == true {
				//fmt.Println("Found start time!")
				starttime = tokenizedQuery[len(tokenizedQuery) - 2] + " " + tokenizedQuery[len(tokenizedQuery) - 1]
				rgmQEnd = " where time > '" + starttime + "'" 
				
				keywordBuffer += 2
			}
			if endtimeunix > 0 {
				//fmt.Println("Found end time!")
				endtime := starttime
				starttime = tokenizedQuery[len(tokenizedQuery) - 4] + " " + tokenizedQuery[len(tokenizedQuery) - 3]
				rgmQEnd = " where time > '" + starttime + "' and time < '" + endtime + "'"
				keywordBuffer += 2
			}
	
			rgmQ = rgmQ + "\""
			regexfound := false
			for i := 1; i < len(tokenizedQuery) - keywordBuffer; i++ {
				if strings.EqualFold(tokenizedQuery[i], "*") {
					regexfound = true
					//rgmQ = rgmQ + "."
				} else {
					rgmQ = rgmQ + tokenizedQuery[i]
				}
				if i < len(tokenizedQuery) - keywordBuffer - 1 {
					rgmQ = rgmQ + " "
				}
			
			}
			rgmQ = rgmQ + "\""
			if regexfound == true {
				rgmQ = strings.Replace(rgmQ, "\"", "/", 2)
				rgmQ = strings.Replace(rgmQ, "/ ", "/", 1)	
				rgmQ = strings.Replace(rgmQ, " /", "/", -1)
				rgmQ = strings.Replace(rgmQ, "/", " /", 1)
			}
			rgmQ = rgmQ + rgmQEnd
			fmt.Printf("Influx Query: %v\n", rgmQ)
			result, err := client.Query(rgmQ)
			for _, series := range result {
				retResults = append(retResults, series)
			}
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return retResults, err
			}
			//fmt.Println(retResults)	
		}
		if len(retResults) == 1 {
			fmt.Printf("201, %v match found.\n", len(retResults))
		} else if len(retResults) > 1 {
			fmt.Printf("202, %v matches found.\n", len(retResults))
		} else if len(retResults) == 0 {
			fmt.Printf("200, %v matches found.\n", len(retResults))
		} else {
			fmt.Printf("Possible error/warning!\n")
		}
		for _, series := range retResults {
			//fmt.Println(series.GetName())
			//fmt.Println(series.GetColumns())
			for _, pts := range series.GetPoints() {
				fmt.Printf("%v\t %v\n", series.GetName(), pts[2])
			}
		}
		return retResults, nil
	case tsQuery, tsQ:
		rgmQEnd := ""
		buffer := 0
		if starttimeunix >= 0 && starttimefound == true {
			rgmQEnd = " where time > " + strconv.FormatInt(starttimeunix, 10) + "s"
			buffer += 2
		} else {
			fmt.Println("No start-time provided. Query-Timeseries requires at least a startime.")
			return []*Series{}, nil 
		}
		if endtimeunix > 0 {
			rgmQEnd = rgmQEnd + " and time < " + strconv.FormatInt(endtimeunix, 10) + "s"
			buffer += 2
		} 
		for counter := 1; counter < len(tokenizedQuery) - buffer; counter++ {
			rgmQ := "select * from "
			rgmQ = rgmQ + "\""
			regexfound := false
			for i := 1; i < len(tokenizedQuery) - buffer; i++ {
				if strings.EqualFold(tokenizedQuery[i], "*") {
					regexfound = true
				} else {
					rgmQ = rgmQ + tokenizedQuery[i]
				}
				if i < len(tokenizedQuery) - buffer - 1 {
					rgmQ = rgmQ + " "
				}
			
			}
			rgmQ = rgmQ + "\""
			if regexfound == true {
				rgmQ = strings.Replace(rgmQ, "\"", "/", 2)
				rgmQ = strings.Replace(rgmQ, "/ ", "/", 1)	
				rgmQ = strings.Replace(rgmQ, " /", "/", -1)
				rgmQ = strings.Replace(rgmQ, "/", " /", 1)
			}
			rgmQ = rgmQ + rgmQEnd
			result, err := client.Query(rgmQ)
			if err != nil {
				fmt.Println("Invalid Query!")
				return retResults, err
			}
			for _, series := range result {
				retResults = append(retResults, series)
			}
		}
		numResults := len(retResults)
		if numResults == 1 {
			fmt.Printf("201, %v match found.\n", numResults)
		} else if numResults > 1 {
			fmt.Printf("202, %v matches found.\n", numResults)
		} else if numResults == 0 {
			fmt.Printf("200, %v matches found.\n", numResults)
		} else {
			fmt.Printf("Possible error/warning!\n")
		}
		for _, series := range retResults {
			fmt.Printf("%v\t %v\t %v\n", series.GetPoints()[0][2], series.GetPoints()[0][0], series.GetPoints()[0][3])
		}
		return retResults, nil
	case curQuery, curQ:
		rgmQEnd := ""
		buffer := 0
		if endtimeunix > 0 {
			rgmQEnd = rgmQEnd + " and time < " + strconv.FormatInt(endtimeunix, 10) + "s"
			buffer += 2
		} 
		for counter := 1; counter < len(tokenizedQuery) - buffer; counter++ {
			rgmQ := "select * from "
			rgmQ = rgmQ + "\""
			regexfound := false
			for i := 1; i < len(tokenizedQuery) - buffer; i++ {
				if strings.EqualFold(tokenizedQuery[i], "*") {
					regexfound = true
				} else {
					rgmQ = rgmQ + tokenizedQuery[i]
				}
				if i < len(tokenizedQuery) - buffer - 1 {
					rgmQ = rgmQ + " "
				}
			}
			rgmQ = rgmQ + "\""
			if regexfound == true {
				rgmQ = strings.Replace(rgmQ, "\"", "/", 2)
				rgmQ = strings.Replace(rgmQ, "/ ", "/", 1)	
				rgmQ = strings.Replace(rgmQ, " /", "/", -1)
				rgmQ = strings.Replace(rgmQ, "/", " /", 1)
			}
			rgmQ = rgmQ + rgmQEnd
			result, err := client.Query(rgmQ)
			if err != nil {
				fmt.Println("Invalid Query!")
				return retResults, err
			}
			for _, series := range result {
				retResults = append(retResults, series)
			}
		}
		numResults := len(retResults)
		var maxtime float64
		maxtime = 0
		//maxpoint := protocol.Point{} 
		var maxPointIndex int
		var maxSeriesIndex int
		for serIndex, series := range retResults {
			for i := range series.GetPoints() {
				pointTime := series.GetPoints()[i][0]
				if pointTime.(float64) > maxtime {
					maxPointIndex = i
					maxSeriesIndex = serIndex
					maxtime = pointTime.(float64)
				}
			}
		}
		if numResults >= 1 {
			fmt.Printf("201, 1 match found.\n")
		} else if numResults == 0 {
			fmt.Printf("200, %v matches found.\n", numResults)
			return retResults, nil
		} else {
			fmt.Printf("Possible error/warning!\n")
			return []*Series{}, nil
		}
		maxPoint := retResults[maxSeriesIndex].GetPoints()[maxPointIndex]
		fmt.Printf("%v\t %v\t %v\n", maxPoint[2], maxPoint[0], maxPoint[3])
		return retResults, nil
	case folQuery:
		rgmQend := ""
		rgmQstart := ""	
		buffer := 0
		if starttimeunix >= 0 && starttimefound == true {
			rgmQstart = " where time > " + strconv.Itoa(int(starttimeunix)) + "s"
			buffer += 2
		} 
		if endtimeunix > 0 {
			rgmQend = " and time < " + strconv.Itoa(int(endtimeunix)) + "s"
			buffer += 2
		} 
		if starttimefound != true {
			fmt.Println("No start-time provided. Query-Timeseries requires at least a startime.")
			return []*Series{}, nil 
		}
		for counter := 1; counter < len(tokenizedQuery) - buffer; counter++ {
			rgmQ := "select * from "
			rgmQ = rgmQ + "\""
			regexfound := false
			for i := 1; i < len(tokenizedQuery) - buffer; i++ {
				if strings.EqualFold(tokenizedQuery[i], "*") {
					regexfound = true
				} else {
					rgmQ = rgmQ + tokenizedQuery[i]
				}
				if i < len(tokenizedQuery) - buffer - 1 {
					rgmQ = rgmQ + " "
				}
			}
			rgmQ = rgmQ + "\""
			if regexfound == true {
				rgmQ = strings.Replace(rgmQ, "\"", "/", 2)
				rgmQ = strings.Replace(rgmQ, "/ ", "/", 1)	
				rgmQ = strings.Replace(rgmQ, " /", "/", -1)
				rgmQ = strings.Replace(rgmQ, "/", " /", 1)
			}
			rgmQ = rgmQ + rgmQstart + rgmQend
			result, err := client.Query(rgmQ)
			//fmt.Printf("RGMQ: %v\n", rgmQ)
			if err != nil {
				fmt.Println("Invalid Query!")
				return retResults, err
			}
			for _, series := range result {
				retResults = append(retResults, series)
			}
		}
		numResults := len(retResults)
		if numResults == 1 {
			fmt.Printf("201, %v match found.\n", numResults)
		} else if numResults > 1 {
			fmt.Printf("202, %v matches found.\n", numResults)
		} else if numResults == 0 {
			fmt.Printf("200, %v matches found.\n", numResults)
		} else {
			fmt.Printf("Possible error/warning!\n")
		}
		for _, series := range retResults {
			fmt.Printf("Point: %v\n", series.GetPoints())
			//fmt.Printf("%v\t %v\t %v\n", series.GetPoints()[0][2], series.GetPoints()[0][0], series.GetPoints()[0][3])
		}
		
		lastQueryTime := time.Now()
		fmt.Printf("NowUnix: %v\n", lastQueryTime.UTC().Unix())
		fmt.Printf("Endtime: %v\n", endtimeunix)
		oldResults := retResults
		fmt.Printf("OldResults: %v\n", oldResults)
		for (lastQueryTime.Unix() < endtimeunix || (endtimeunix == 0)) {
			rgmQstart = " where time > " + strconv.FormatInt(lastQueryTime.Unix(), 10) + "s"
			//fmt.Printf("QStart: %v\n", rgmQstart)
			newResults := []*Series{}
			//fmt.Printf("New Before: %v\n", newResults)
			for counter := 1; counter < len(tokenizedQuery) - buffer; counter++ {
				rgmQ := "select * from "
				rgmQ = rgmQ + "\""
				regexfound := false
				for i := 1; i < len(tokenizedQuery) - buffer; i++ {
					if strings.EqualFold(tokenizedQuery[i], "*") {
						regexfound = true
					} else {
						rgmQ = rgmQ + tokenizedQuery[i]
					}
					if i < len(tokenizedQuery) - buffer - 1 {
						rgmQ = rgmQ + " "
					}
				}
				rgmQ = rgmQ + "\""
				if regexfound == true {
					rgmQ = strings.Replace(rgmQ, "\"", "/", 2)
					rgmQ = strings.Replace(rgmQ, "/ ", "/", 1)	
					rgmQ = strings.Replace(rgmQ, " /", "/", -1)
					rgmQ = strings.Replace(rgmQ, "/", " /", 1)
				}
				rgmQ += rgmQstart
				if (endtimeunix != 0) {
					rgmQ = rgmQ + rgmQend
				}
				fmt.Printf("rgmQ: %v\n", rgmQ)
				result, err := client.Query(rgmQ)
				if err != nil {
					fmt.Println("Invalid Query!")
					return retResults, err
				}
				for _, series := range result {
					fmt.Printf("Pts: %v\n", series.GetPoints())
					for _, point := range series.GetPoints() {
						tm := int(point[0].(float64))
						fmt.Printf("T: %v\n", tm)
						//if (tm < starttimeunix) {
						//	series.GetPoints()[i] = nil
						//}
					}
					newResults = append(newResults, series)
				}
			}
			//fmt.Printf("NewBefore: %v\n", newResults)
			//if len(newResults) > 0 {
			//	fmt.Printf("NewResults: %v\n", newResults)
			//}	
			for _, series := range newResults {
				firstSeen := false
				for _, point := range series.GetPoints() {
					for _, oldSeries := range oldResults {
						for _, point2 := range oldSeries.GetPoints() {
							for i := 0; i < len(point2); i++ {
								if point[i] != point2[i] {
									firstSeen = true
								}
							}
						}
					}
					//fmt.Printf("%v\t %v\t %v\n", series.GetPoints()[0][2], series.GetPoints()[0][0], series.GetPoints()[0][3])
				}
				if firstSeen == true {
					oldResults = append(oldResults, series)
					//printPoint := series.GetPoints()[len(series.GetPoints()) - 1]
					//fmt.Printf("%v\t %v\t %v\n", series.GetName(), printPoint[0], printPoint[1])
				}
			}
			//retResults = []*Series{}
			//fmt.Printf("New After: %v\n", newResults)
			lastQueryTime = time.Now()
		}
		return retResults, nil
	case scQuery, scQ:
		return retResults, nil
	case stQuery, stQ:
		return retResults, nil
	case unsubQuery, unsubQ:
		return retResults, nil
	case qsubQuery, qsubQ:
		return retResults, nil
	default:
		log.Fatal("%s is an unrecognized query type - see documentation for allowed query types", rgmQuery)
	} 
	
	return retResults, nil
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
	date := strings.Split(datestring, " ")[0]
	timestring := strings.Split(datestring, " ")[1]
	ymd := strings.Split(date, "-")
	hms := strings.Split(timestring, ":")
	intDates := []int{}
	for _, val := range ymd {
		var intDate int
		int64Date, err := strconv.ParseInt(val, 0, 0)
		intDate = int(int64Date)
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
		intTimes = append(intTimes, intTime)
		if err != nil {
			return time.Now(), err 
		}
	}
	retTime := time.Date(intDates[0], time.Month(intDates[1]), intDates[2], intTimes[0], intTimes[1], intTimes[2], 0, time.UTC)
	return retTime, nil
}

func ParseTime(tokenizedQuery []string, starttimefound bool) (int64, int64, bool) {
	var starttimeunix int64
	var endtimeunix int64
	starttime, err := strconv.ParseInt(tokenizedQuery[len(tokenizedQuery) - 1], 10, 64)
	//starttimeunix = starttime
	if err != nil && starttimeunix > 1000 && len(tokenizedQuery) >= 2 {
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
	} else if (len(tokenizedQuery) > 2 && isDateTime(tokenizedQuery[len(tokenizedQuery) - 2] + " " + tokenizedQuery[len(tokenizedQuery) - 1])) { 
		starttime, _ := getDateFromString(tokenizedQuery[len(tokenizedQuery) - 2] + " " + tokenizedQuery[len(tokenizedQuery) - 1])
		starttimeunix = starttime.Unix()
		starttimefound = true
		if len(tokenizedQuery) > 4 && (isDateTime(tokenizedQuery[len(tokenizedQuery) - 4] + " " + tokenizedQuery[len(tokenizedQuery) - 3]) == true) {
			endtimeunix = starttimeunix
			starttime, _ := getDateFromString(tokenizedQuery[len(tokenizedQuery) - 4] + " " + tokenizedQuery[len(tokenizedQuery) - 3]) 
			starttimeunix = starttime.Unix()
		}
	}
	fmt.Printf("EndtimeUnix: %v\n", endtimeunix)
	fmt.Printf("StatimeUnix: %v\n", starttimeunix)
	return starttimeunix, endtimeunix, starttimefound
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("ENTER QUERY: ")
	for scanner.Scan() {
		_, err := QueryHandler(scanner.Text())
		if err != nil {
			log.Fatal(err)
		}
		/*
		for _, seriesArr := range result {
			for _, series := range seriesArr {
				fmt.Println(series.GetName())
				fmt.Println(series.GetColumns())
				//fmt.Println(series.GetPoints())
			}
		}
		*/
		fmt.Print("ENTER QUERY: ")
	}
}
