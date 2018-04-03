package main

import (
	"errors"
	"reflect"
	"fmt"
)

// . This guy will return an object, that when given a []map[string]interface will return back a gr

// . Types of fields that it handles and the type of data that it expects those fields to have
// . string:string, []string:[]string, int64:int64, timestamp:int64

// . If your []map[string]interface does not have the correct type that it is expecting then
// . 	it will not process that field and give you a warning.

// . Take the table name as string
// . loop over each of the fields from

type Grf struct {
	// . Field type hash : type map[string]string
	// .
	FieldHash map[string]string

	// . fieldname[type]
	// . getTypeByField(string)(string)
	// . populate(map[type string]fn string
	// .

}

// This function will take the type that is created by the module that parses the configuration object
// 	for the API level and returns stuff
func (g *Grf) PopulateNew(in map[string][]string) (err []error) {

	// Check for duplicates
	val := g.checkForDuplicates(in)

	if val == false {

		err = append(err, errors.New("Field repeated 2 times in configuration"))
		return
	}

	for k, v := range in {

		for _, vv := range v {

			switch k {
			case "string" :
				g.FieldHash[vv] = "string"

			case "arr_str":
				g.FieldHash[vv] = "arr_str"

			case "int":
				g.FieldHash[vv] = "int"

			case "timestamp":
				g.FieldHash[vv] = "timestamp"

			default:
				err = append(err, errors.New("Field type passed is invalid"))
				return
			}
		}
	}

	return
}

// TODO : Check for duplicates has to be written
func (g *Grf) checkForDuplicates(in map[string][]string) (ret bool) {

	var chkArr []string
	ret = true

	// First make a flat array
	for _, v := range in {

		for _, vv := range v {

			chkArr = append(chkArr, vv)
		}
	}

	// Check if this flat array has duplicates, if yes, throw error

	return
}

func (g *Grf) getTypeByName(s string) (ret string, err error) {

	if _, ok := g.FieldHash[s]; ok {

		ret = g.FieldHash[s]
	} else {

		err = errors.New("Field type searched not found")
	}

	return
}

// Send a single row to this value and it will give you back the gr that you want
func (g *Grf) MakeGr(res map[string]interface{}) (gr []interface{}, errs []error) {

	var tmp map[string]interface{}

	// Loop over all the values sent :
	for k, v := range res {

		// Get the standard type based on name
		typ, err := g.getTypeByName(k)
		if err != nil {
			errs = append(errs, errors.New(`Could not get a type for the field : %v to make a generic record.
			Please make sure all the fields returned by your SQL query are part of some type array reference`))
		}

		valType := reflect.TypeOf(v).String()

		// Here for the standard types mentioned by the user, we will check to see if the field returned from the
		// db is of the type that we are expecting the field type to be
		// if it is not -- add to the error array
		switch typ {
		case "string":
			if valType == "string" {

				if v != nil {

					tmp[k] = returnGenericString(v.(string))
				}
			} else {
				errs = append(errs, errors.New(fmt.Sprintf(`Type of the field : %v returned from the
					database is %v and not string as expected.`,  k, valType)))
			}

		case "[]string":
			if valType == "[]string" {

				if v != nil {
					tmp[k] = returnGenericStringArray(v.([]string))
				}
			} else {
				errs = append(errs, errors.New(fmt.Sprintf(`Type of the field : %v returned from the
					database is %v and not []string as expected.`,  k, valType)))
			}

		case "int":

			if valType == "int64" {

				if v != nil {
					tmp[k] = returnGenericStringInt(v.(int64))
				}
			} else {
				errs = append(errs, errors.New(fmt.Sprintf(`Type of the field : %v returned from the
					database is %v and not int64 as expected.`,  k, valType)))
			}

		case "timestamp":

			if valType == "int64" {
				if v != nil {
					tmp[k] = returnGenericTimeString(v.(int64))
				}
			} else {
				errs = append(errs, errors.New(fmt.Sprintf(`Type of the field : %v returned from the
					database is %v and not timestamp as expected.`,  k, valType)))
			}

		default:
			errs = append(errs, errors.New(fmt.Sprint("Unexpected type was returned for field : %v -- Got type : %v", k, valType)))
		}
	}

	// If there are problems, send this user back
	if errs != nil {
		// Make the result blank -- errors are already filled, lets send this back
		gr = []interface{}{}
		return
	}

	gr = []interface{}{ tmp }
	return
}