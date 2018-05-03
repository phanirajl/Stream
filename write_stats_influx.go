package main

import (
	"github.com/dminGod/Stream/influx_writer"
	"reflect"
	"github.com/antigloss/go/logger"
	"fmt"
)

func writeStatsToInflux(cassT int64,streamT int64, keyVal interface{},writer influx_writer.InfluxWriter){

	var timeTakenCassStreamDiff int64
	errorOccured := false
	if _,ok := keyVal.([]interface{}); ok{
		returnVals := getValuesforStream(keyVal.([]interface{}))
		if len(returnVals) > 0{
			timeTakenCassStreamDiff = returnVals[0]
		} else {
			errorOccured = true
			logger.Error("Cassandra to Stream Difference not available for the record")
		}
	} else {
		errorOccured = true
		logger.Error("generic record is not in the right format")
	}

	if ! errorOccured{
		messageToBeSent := fmt.Sprintf(`%v,StreamID=%v TimeTaken=%v,TimeTakenCass=%v,StreamTimeTaken=%v`,writer.InfluxCfg.Measurement,getHostname(),timeTakenCassStreamDiff,cassT,streamT)
		err := writer.WriteRow(messageToBeSent)
		if err != nil{
			logger.Error("error is %v",err)
		}
	}


}


func getValuesforStream(arrInt []interface{}) ([]int64){
	var returnInt64 []int64
	for _,element := range arrInt{


		if _,ok := element.(map[string]interface{}); ok{
			elementVal := element.(map[string]interface{})
			if _, ok := elementVal["int_cass_stream_diff"].(map[string]interface{}); ok{
				cassStreamDiffmsi := elementVal["int_cass_stream_diff"].(map[string]interface{})
				if _,ok := cassStreamDiffmsi["long"].(int64);ok{
					cassStreamDiffi64 := cassStreamDiffmsi["long"].(int64)
					returnInt64 = append(returnInt64, cassStreamDiffi64)
				} else {
					logger.Error("Error in format of cass stream diff")
				}
			} else {
				logger.Error("Error occured in format of int_cass_Stream_diff container")
			}
		}else{
			logger.Error("Error occured in format of element : expected map[string] interface{}",reflect.TypeOf(element))
		}
	}
	return returnInt64
}