package main

import (
	"errors"
	"fmt"
	"github.com/antigloss/go/logger"
	"github.com/linkedin/goavro"
	"regexp"
	"strconv"
	"time"
)

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

func returnGenericTimeString(s int64) interface{} {

	if s > 100000 {

		return goavro.Union("long", s)
	} else {

		logger.Error("Time string too small to be timestamp, got : %v", s)
	}

	return nil
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
