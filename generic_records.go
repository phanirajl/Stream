package main

import (
	"errors"
	"fmt"
	"github.com/dminGod/Stream/models"
	"github.com/antigloss/go/logger"
	"github.com/linkedin/goavro"
	"github.com/gocql/gocql"
	"strings"
	"reflect"
	"time"
	"regexp"
	"github.com/golang/snappy"
	"strconv"
)

// TODO: Only certain types are currently handled -- Other types are not considered
// TODO: []byte 107 there is no loop


func convertUnixToTimeString(secsSince int64) (timeString string){

	timeString = time.Unix(secsSince,0).Format("20060102T150405 +07")
	return
}


// Send a single row to this value and it will give you back the gr that you want
func MakeGr(res map[string]interface{}, as *models.APIDetails) (gr []interface{}, errs []error) {

	tmp := make(map[string]interface{})

	// lookFor := []string{"int_created_date", "int_updated_date"}

	// 1 -- Check for all 4 (bool)
	// Should exist in the target avro (int_created_date, int_updated_date, int_created_date_cass, int_updated_date_cass)
	icdb,_ := as.AvroFields.CheckFieldExists("int_created_date")
	iudb,_ := as.AvroFields.CheckFieldExists("int_updated_date")
	icdbc,_ := as.AvroFields.CheckFieldExists("int_created_date_str")
	iudbc,_ := as.AvroFields.CheckFieldExists("int_updated_date_str")
	icdc,_ := as.AvroFields.CheckFieldExists("int_created_date_cass")
	bool_cond1 := false

	if icdb && iudb && icdbc && iudbc && icdc{
		bool_cond1 = true
	}
	// 2 -- Check for both fields in msg (bool)
	// Should exist in the msg data (int_created_date, int_updated_date)
	icdc2 := false
	iudc2 := false
	iudexists := false
	bool_cond2 := false
	if icd2,ok := res["int_created_date"]; ok {
		if _, ok2 := icd2.(int64); ok2 {
			icdc2 = true
		}
	}

	if iud2,ok := res["int_updated_date"]; ok{
		if v2, ok2 := iud2.(int64); ok2{
			if v2 > 1000000{
				iudexists = true
			}
		}
		iudc2 = true
	}
	if iudexists{
		logger.Info("Data exists for Updated Date")
	}
	if icdc2 && iudc2 {
		bool_cond2 = true
	}

	// if 1
	// Set the values for int_cass_created_date and int_cass_updated_date
	// "int_created_date" --> "int_cass_created_date"
	// "int_updated_date" --> "int_cass_updated_date"
	// Set the present time as the value for int_created_date, int_updated_date
	// "int_cass_stream_diff -- This is a calculated field
	var timeCreated int64
	if bool_cond1{
		timeCreated = res["int_created_date"].(int64)/int64(time.Microsecond)
		res["int_created_date_str"] = convertUnixToTimeString(timeCreated)
		res["int_created_date_cass"] = res["int_created_date"].(int64)
		if iudexists {
			res["int_updated_date_str"] = convertUnixToTimeString(res["int_updated_date"].(int64)/int64(time.Microsecond))
		} else {
			res["int_updated_date_str"] = convertUnixToTimeString(timeCreated)
		}
		res["int_created_date"] = time.Now().Unix()
		res["int_updated_date"] = time.Now().Unix()
	}

	// if 1 && 2
	// Should exist in the target avro (int_cass_stream_diff)
	// Do the calculation for int_cass_stream_diff
	if bool_cond1 && bool_cond2{
		res["int_cass_stream_diff"] = time.Now().Sub(time.Unix(timeCreated,0)).Nanoseconds()/int64(time.Millisecond)
	}





	// Loop over all the values sent :
	for k, v := range res {

		// Findout if k is there in the Schema
		scTy, isArray, err := as.AvroFields.GetTypeByName( k )
		if err != nil {
			e := errors.New(fmt.Sprintf("Error when searching for field in the Avro schema : '%v' ", err))
			logger.Error(e.Error())
			errs = append(errs, e)
			continue
		}


		var compField bool

		for _, vv := range as.CassCompressedFields {
			if vv == k {
				compField = true
			}
		}

		// Get the type of field
		tmp[k], err = returnGeneric(scTy, isArray, compField, k, v)
		if err != nil {
			e := errors.New(fmt.Sprintf("Error when converting to Avro format : '%v' ", err))
			logger.Error(e.Error())
			errs = append(errs, e)
			continue
		}
	}

	gr = []interface{}{ tmp }
	return
}

/*
Avro Types List
"boolean":
"bytes":
"double":
"float":
"int":
"long":
"null":
"string":

case "array":

case "enum":
case "fixed":
case "map":

case "record":


 */


func returnGeneric(typ string, isArr bool, isComp bool, key string, s interface{}) (a interface{}, err error) {

	var found bool
	isBlank := true

	switch s.(type) {
	case string:
		if typ == "string" {
			if len(s.(string)) > 0 {
				a = goavro.Union("string", s)
				isBlank = false
			}

			found = true
		}

	case []string:
		if typ == "string" && isArr == true {
			var tmpArr []interface{}

			for _, v := range s.([]string) {
				tmpArr = append(tmpArr, goavro.Union("string", v))
			}

			if len(tmpArr) > 0 {
				a = goavro.Union("array", tmpArr)
				isBlank = false
			}

			found = true
		}

	case int64:
		if typ == "long" {
			a = goavro.Union("long", s)
			found = true
		}

	case int:
		if typ == "long" {
			a = goavro.Union("long", s)
			isBlank = false
			found = true
		}

	case gocql.UUID:
		if typ == "string" {

			tmp := s.(gocql.UUID).String()
			if len(tmp) > 0 {
				a = goavro.Union("string", s.(gocql.UUID).String())
				isBlank = false
			}

			found = true
		}

	case []byte:
		if typ == "string" && isArr == true {

			var tmpArr []interface{}
			var lsrs string

			if len(s.([]byte)) > 0 {

				// Is a compressed field
				if isComp {
					var m []byte

					m, er := snappy.Decode(nil, s.([]byte))
					if er != nil {
						logger.Error("There was an error decoding the compressed message, setting it as blank -- Error : %v ", er)
						err = er
						return
					}

					lsrs = strings.Replace(string(m), `"`, `\"`, -1)
				} else {

					lsrs = strings.Replace(string(s.([]byte)), `"`, `\"`, -1)
				}

				tmpArr = append(tmpArr, goavro.Union("string", lsrs))
				a = goavro.Union("array", tmpArr)

				isBlank = false
			}

			found = true
		}
	}

	_ = isBlank

	if found == false {

		err = errors.New(fmt.Sprintf("Expecting '%v' - Got '%v' -- Key : %v -  Value %v or could be blank field -- skipping field", typ, reflect.TypeOf(s).String(), key, s))
	}

	return
}

func returnGenericTimeDiff(tm time.Time, tms int64) interface{} {

	tmss := strconv.Itoa(int(tms))

	if len(tmss) > 10 {

		re := regexp.MustCompile(`(\d{10})(\d{3})`)
		sm := re.FindAllStringSubmatch(tmss, -1)

		var err error

		if len(sm) > 0 {
			if len(sm[0]) == 3 {

				var tsTmp, ts2Tmp int

				tsTmp, err = strconv.Atoi(sm[0][1])
				ts2Tmp, err = strconv.Atoi(sm[0][2])

				if err == nil {

					ts := int64(tsTmp)
					ts2 := int64(ts2Tmp) * int64(time.Millisecond)

					td := tm.Sub(time.Unix(ts, ts2)).Nanoseconds() / int64(time.Millisecond)
					return goavro.Union("long", td)
				}
			} else {
				err = errors.New(fmt.Sprint(`Size of sm array from regex sm[0] is not 3`, sm))
			}
		} else {
			err = errors.New(`Size of sm array from regex is 0`)
		}

		if err != nil {
			logger.Error("Got error in getting time diff, Error : %v ", err)
		}
	} else {

		logger.Error("Length of string is less than 10 -- ", tmss)
	}

	return nil
}

