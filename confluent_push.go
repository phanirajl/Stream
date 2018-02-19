package main
//
//import (
//	kafavro "github.com/elodina/go-kafka-avro"
//	"github.com/elodina/go-avro"
//)
//
//
//
//func SendConfluent(){
//
//	schemaUrl := "http://172.17.0.2:8081"
//	sc := `{"type" : "record", "name" : "hive_sink_lsr_7", "namespace" : "app_db", "fields" : [ { "name" : "local_service_requests_new_con5_pk", "type" : [ "null", "string" ], "default" : null }, { "name" : "actor", "type" : [ "null", "string" ], "default" : null }, { "name" : "application_detail", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "ba_category", "type" : [ "null", "string" ], "default" : null }, { "name" : "ba_segment", "type" : [ "null", "string" ], "default" : null }, { "name" : "bano", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "billing_system", "type" : [ "null", "string" ], "default" : null }, { "name" : "bso_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bwo_id", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "cano", "type" : [ "null", "string" ], "default" : null }, { "name" : "channel", "type" : [ "null", "string" ], "default" : null }, { "name" : "charge_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "complete_dt", "type" : [ "null", "string" ], "default" : null }, { "name" : "destination", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "error_message", "type" : [ "null", "string" ], "default" : null }, { "name" : "imsi_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "last_upd", "type" : [ "null", "string" ], "default" : null }, { "name" : "local_service_requests_s", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "location_cd", "type" : [ "null", "string" ], "default" : null }, { "name" : "msisdn_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "ne_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_ref", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_status", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_sub_type", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_item_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_item_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_item_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "pgz_record_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "pgz_task_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "product_name_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "promotion_action_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "promotion_code_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "req_status", "type" : [ "null", "string" ], "default" : null }, { "name" : "request_id", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "resp_status", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "sano", "type" : [ "null", "string" ], "default" : null }, { "name" : "service_action_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "service_code_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "sim_serial_no", "type" : [ "null", "string" ], "default" : null }, { "name" : "submitted_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "submitted_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "subscriberid1_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "subscriberid2_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "subscriberid3_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "user_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "user_sys", "type" : [ "null", "string" ], "default" : null }, { "name" : "wo_header_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "wo_header_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "wo_header_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "int_created_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "int_created_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "int_updated_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "int_updated_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "int_is_deleted", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date_new", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_new_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date_time", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_time_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "request_id_int_field", "type" : [ "null", "string" ], "default" : null }, { "name" : "bwo_id_int_field", "type" : [ "null", "string" ], "default" : null }, { "name" : "retry_count", "type" : [ "null", "long" ], "default" : null }, { "name" : "val1_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val2_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val3_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val4_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val5_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val6_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val7_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val8_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val9_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val10_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val1", "type" : [ "null", "string" ], "default" : null }, { "name" : "val2", "type" : [ "null", "string" ], "default" : null }, { "name" : "val3", "type" : [ "null", "string" ], "default" : null }, { "name" : "val4", "type" : [ "null", "string" ], "default" : null }, { "name" : "val5", "type" : [ "null", "string" ], "default" : null }, { "name" : "val6", "type" : [ "null", "string" ], "default" : null }, { "name" : "val7", "type" : [ "null", "string" ], "default" : null }, { "name" : "val8", "type" : [ "null", "string" ], "default" : null }, { "name" : "val9", "type" : [ "null", "string" ], "default" : null }, { "name" : "val10", "type" : [ "null", "string" ], "default" : null }, { "name" : "resend_flag", "type" : [ "null", "string" ], "default" : null }, { "name" : "file_name", "type" : [ "null", "string" ], "default" : null }, { "name" : "ref_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "lot_no", "type" : [ "null", "string" ], "default" : null }, { "name" : "val11_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val12_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val13_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val14_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val15_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val16_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val17_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val18_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val19_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val20_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null } ]}`
//
//
//	encoder := kafavro.NewKafkaAvroEncoder(schemaUrl)
//
//	schema, _ := avro.ParseSchema(sc)
//
//	record := avro.NewGenericRecord(schema)
//	record.Set("id", int64(3))
//	record.Set("timings", []int64{123456, 654321})
//
//	bytes, err := encoder.Encode(record)
//
//
//
//
//
//
//}
