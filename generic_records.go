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
// TODO: Internal fields are not handled
// TODO: []byte 107 there is no loop
// TODO: Integrate influx code with this

// Send a single row to this value and it will give you back the gr that you want
func MakeGr(res map[string]interface{}, as models.APIDetails) (gr []interface{}, errs []error) {

	var tmp map[string]interface{}

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
		tmp[k], err = returnGeneric(scTy, isArray, compField, v)
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


func returnGeneric(typ string, isArr bool, isComp bool, s interface{}) (a interface{}, err error) {

	var found bool

	switch s.(type) {
	case string:
		if typ == "string" {
			if len(s.(string)) > 0 {
				a = goavro.Union("string", s)
				found = true
			}
		}

	case []string:
		if typ == "string" && isArr == true {
			var tmpArr []interface{}

			for _, v := range s.([]string) {
				tmpArr = append(tmpArr, goavro.Union("string", v))
			}

			if len(tmpArr) > 0 {
				a = goavro.Union("array", tmpArr)
				found = true
			}
		}

	case int64:
		if s.(int64) > 0 && typ == "long" {
			a = goavro.Union("long", s)
			found = true
		}

	case int:
		if s.(int) > 0 && typ == "long" {
			a = goavro.Union("long", s)
			found = true
		}

	case gocql.UUID:
		if typ == "string" {

			a = goavro.Union("string", s.(gocql.UUID).String())
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
				found = true
			}
		}
	}

	if found == false {

		err = errors.New(fmt.Sprintf("Expecting '%v' - Got '%v' -- Value %v or could be blank field -- skipping field", typ, reflect.TypeOf(s).String()))
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

