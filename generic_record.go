package main

import "github.com/linkedin/goavro"

func returnGenericString(s string) interface{} {

	if len(s) > 0 {
		return goavro.Union("string", s)
	}

	return nil
}

func returnGenericStringArray(s []string) (r interface{}) {

	var tmpArr []interface{}

	for _, v := range s {

		tmpArr = append(tmpArr, goavro.Union("string", v))
	}

	if len(tmpArr) > 0 {

		return goavro.Union("array", tmpArr)
	}

	return nil
}

func returnGenericStringInt(s int64) (r interface{}) {

	if s > 0 {

		return goavro.Union("long", s)
	}

	return nil
}
