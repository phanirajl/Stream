package main

import (
	"fmt"
	"strings"
	"time"
)

type lsr_record struct {

	Local_service_requests_new_con5_pk string
	Actor                              string
	Application_detail                 []string
	Ba_category                        string
	Ba_segment                         string
	Bano                               []string
	Billing_system                     string
	Bso_key                            []string
	Bwo_id                             []string
	Bso_status_key                     []string
	Bso_error_message_key              []string
	Cano                               string
	Channel                            string
	Charge_type                        string
	Complete_dt                        string
	Destination                        []string
	Error_message                      string
	Imsi_key                           []string
	Last_upd                           string
	Local_service_requests_s           []string
	Location_cd                        string
	Msisdn_key                         []string
	Ne_id                              string
	Order_date                         int64
	Order_date_str                     string
	Order_ref                          string
	Order_status                       string
	Order_sub_type                     []string
	Order_type                         string
	Order_item_status_key              []string
	Order_item_error_message_key       []string
	Order_item_error_date_key          []string
	Pgz_record_type                    string
	Pgz_task_id                        string
	Product_name_key                   []string
	Promotion_action_key               []string
	Promotion_code_key                 []string
	Req_status                         string
	Request_id                         []string
	Resp_status                        []string
	Sano                               string
	Service_action_key                 []string
	Service_code_key                   []string
	Sim_serial_no                      string
	Submitted_date                     int64
	Submitted_date_str                 string
	Subscriberid1_key                  []string
	Subscriberid2_key                  []string
	Subscriberid3_key                  []string
	User_id                            string
	User_sys                           string
	Wo_header_error_message_key        []string
	Wo_header_status_key               []string
	Wo_header_error_date_key           []string
	Bso_error_date_key                 []string
	Int_created_date                   int64
	Int_created_date_str               string
	Int_updated_date                   int64
	Int_updated_date_str               string
	Int_is_deleted                     string
	Order_info_error_date_new          int64
	Order_info_error_date_new_str      string
	Order_info_error_date_time         int64
	Order_info_error_date_time_str     string
	Order_info_error_date              int64
	Order_info_error_date_str          string
	Request_id_int_field               string
	Bwo_id_int_field                   string
	Retry_count                        int64
	Val1_key                           []string
	Val2_key                           []string
	Val3_key                           []string
	Val4_key                           []string
	Val5_key                           []string
	Val6_key                           []string
	Val7_key                           []string
	Val8_key                           []string
	Val9_key                           []string
	Val10_key                          []string
	Val1                               string
	Val2                               string
	Val3                               string
	Val4                               string
	Val5                               string
	Val6                               string
	Val7                               string
	Val8                               string
	Val9                               string
	Val10                              string
	Resend_flag                        string
	File_name                          string
	Ref_id                             string
	Lot_no                             string
	Val11_key                          []string
	Val12_key                          []string
	Val13_key                          []string
	Val14_key                          []string
	Val15_key                          []string
	Val16_key                          []string
	Val17_key                          []string
	Val18_key                          []string
	Val19_key                          []string
	Val20_key                          []string
}

func (l *lsr_record) getGenericRecord() (ret []interface{}) {

	t := time.Now()
	nn := t.UnixNano()
	tm := nn / 1000000
	ts := t.Format("2006-01-02 15:04:05-0700")
	gr := make(map[string]interface{})
	grRet := make(map[string]interface{})

	gr["local_service_requests_new_con5_pk"] = returnGenericString(l.Local_service_requests_new_con5_pk)
	gr["actor"] = returnGenericString(l.Actor)
	gr["application_detail"] = returnGenericStringArray(l.Application_detail)
	gr["ba_category"] = returnGenericString(l.Ba_category)
	gr["ba_segment"] = returnGenericString(l.Ba_segment)
	gr["bano"] = returnGenericStringArray(l.Bano)
	gr["billing_system"] = returnGenericString(l.Billing_system)
	gr["bso_key"] = returnGenericStringArray(l.Bso_key)
	gr["bwo_id"] = returnGenericStringArray(l.Bwo_id)
	gr["bso_status_key"] = returnGenericStringArray(l.Bso_status_key)
	gr["bso_error_message_key"] = returnGenericStringArray(l.Bso_error_message_key)
	gr["cano"] = returnGenericString(l.Cano)
	gr["channel"] = returnGenericString(l.Channel)
	gr["charge_type"] = returnGenericString(l.Charge_type)
	gr["complete_dt"] = returnGenericString(l.Complete_dt)
	gr["destination"] = returnGenericStringArray(l.Destination)
	gr["error_message"] = returnGenericString(l.Error_message)
	gr["imsi_key"] = returnGenericStringArray(l.Imsi_key)
	gr["last_upd"] = returnGenericString(l.Last_upd)
	gr["local_service_requests_s"] = returnGenericStringArray(l.Local_service_requests_s)
	gr["location_cd"] = returnGenericString(l.Location_cd)
	gr["msisdn_key"] = returnGenericStringArray(l.Msisdn_key)
	gr["ne_id"] = returnGenericString(l.Ne_id)
	gr["order_date"] = returnGenericStringInt(l.Order_date)
	gr["order_date_str"] = returnGenericString(l.Order_date_str)
	gr["order_ref"] = returnGenericString(l.Order_ref)
	gr["order_status"] = returnGenericString(l.Order_status)
	gr["order_sub_type"] = returnGenericStringArray(l.Order_sub_type)
	gr["order_type"] = returnGenericString(l.Order_type)
	gr["order_item_status_key"] = returnGenericStringArray(l.Order_item_status_key)
	gr["order_item_error_message_key"] = returnGenericStringArray(l.Order_item_error_message_key)
	gr["order_item_error_date_key"] = returnGenericStringArray(l.Order_item_error_date_key)
	gr["pgz_record_type"] = returnGenericString(l.Pgz_record_type)
	gr["pgz_task_id"] = returnGenericString(l.Pgz_task_id)
	gr["product_name_key"] = returnGenericStringArray(l.Product_name_key)
	gr["promotion_action_key"] = returnGenericStringArray(l.Promotion_action_key)
	gr["promotion_code_key"] = returnGenericStringArray(l.Promotion_code_key)
	gr["req_status"] = returnGenericString(l.Req_status)
	gr["request_id"] = returnGenericStringArray(l.Request_id)
	gr["resp_status"] = returnGenericStringArray(l.Resp_status)
	gr["sano"] = returnGenericString(l.Sano)
	gr["service_action_key"] = returnGenericStringArray(l.Service_action_key)
	gr["service_code_key"] = returnGenericStringArray(l.Service_code_key)
	gr["sim_serial_no"] = returnGenericString(l.Sim_serial_no)
	gr["submitted_date"] = returnGenericStringInt(l.Submitted_date)
	gr["submitted_date_str"] = returnGenericString(l.Submitted_date_str)
	gr["subscriberid1_key"] = returnGenericStringArray(l.Subscriberid1_key)
	gr["subscriberid2_key"] = returnGenericStringArray(l.Subscriberid2_key)
	gr["subscriberid3_key"] = returnGenericStringArray(l.Subscriberid3_key)
	gr["user_id"] = returnGenericString(l.User_id)
	gr["user_sys"] = returnGenericString(l.User_sys)
	gr["wo_header_error_message_key"] = returnGenericStringArray(l.Wo_header_error_message_key)
	gr["wo_header_status_key"] = returnGenericStringArray(l.Wo_header_status_key)
	gr["wo_header_error_date_key"] = returnGenericStringArray(l.Wo_header_error_date_key)
	gr["bso_error_date_key"] = returnGenericStringArray(l.Bso_error_date_key)
	gr["int_created_date"] = returnGenericStringInt(tm) 	// Diff
	gr["int_created_date_str"] = returnGenericString(ts) 	// Diff
	gr["int_created_date_cass"] = returnGenericTimeString(l.Int_created_date)	// Diff
	gr["int_cass_stream_diff"] = returnGenericTimeDiff(t, l.Int_created_date)	// Diff
	gr["int_created_date_cass_str"] = returnGenericString(l.Int_created_date_str) // Diff
	gr["int_updated_date"] = returnGenericStringInt(l.Int_updated_date)
	gr["int_updated_date_str"] = returnGenericString(l.Int_updated_date_str)
	gr["int_is_deleted"] = returnGenericString(l.Int_is_deleted)
	gr["order_info_error_date_new"] = returnGenericStringInt(l.Order_info_error_date_new)
	gr["order_info_error_date_new_str"] = returnGenericString(l.Order_info_error_date_new_str)
	gr["order_info_error_date_time"] = returnGenericStringInt(l.Order_info_error_date_time)
	gr["order_info_error_date_time_str"] = returnGenericString(l.Order_info_error_date_time_str)
	gr["order_info_error_date"] = returnGenericStringInt(l.Order_info_error_date)
	gr["order_info_error_date_str"] = returnGenericString(l.Order_info_error_date_str)
	gr["request_id_int_field"] = returnGenericString(l.Request_id_int_field)
	gr["bwo_id_int_field"] = returnGenericString(l.Bwo_id_int_field)
	gr["retry_count"] = returnGenericStringInt(l.Retry_count)
	gr["val1_key"] = returnGenericStringArray(l.Val1_key)
	gr["val2_key"] = returnGenericStringArray(l.Val2_key)
	gr["val3_key"] = returnGenericStringArray(l.Val3_key)
	gr["val4_key"] = returnGenericStringArray(l.Val4_key)
	gr["val5_key"] = returnGenericStringArray(l.Val5_key)
	gr["val6_key"] = returnGenericStringArray(l.Val6_key)
	gr["val7_key"] = returnGenericStringArray(l.Val7_key)
	gr["val8_key"] = returnGenericStringArray(l.Val8_key)
	gr["val9_key"] = returnGenericStringArray(l.Val9_key)
	gr["val10_key"] = returnGenericStringArray(l.Val10_key)
	gr["val1"] = returnGenericString(l.Val1)
	gr["val2"] = returnGenericString(l.Val2)
	gr["val3"] = returnGenericString(l.Val3)
	gr["val4"] = returnGenericString(l.Val4)
	gr["val5"] = returnGenericString(l.Val5)
	gr["val6"] = returnGenericString(l.Val6)
	gr["val7"] = returnGenericString(l.Val7)
	gr["val8"] = returnGenericString(l.Val8)
	gr["val9"] = returnGenericString(l.Val9)
	gr["val10"] = returnGenericString(l.Val10)
	gr["resend_flag"] = returnGenericString(l.Resend_flag)
	gr["file_name"] = returnGenericString(l.File_name)
	gr["ref_id"] = returnGenericString(l.Ref_id)
	gr["lot_no"] = returnGenericString(l.Lot_no)
	gr["val11_key"] = returnGenericStringArray(l.Val11_key)
	gr["val12_key"] = returnGenericStringArray(l.Val12_key)
	gr["val13_key"] = returnGenericStringArray(l.Val13_key)
	gr["val14_key"] = returnGenericStringArray(l.Val14_key)
	gr["val15_key"] = returnGenericStringArray(l.Val15_key)
	gr["val16_key"] = returnGenericStringArray(l.Val16_key)
	gr["val17_key"] = returnGenericStringArray(l.Val17_key)
	gr["val18_key"] = returnGenericStringArray(l.Val18_key)
	gr["val19_key"] = returnGenericStringArray(l.Val19_key)
	gr["val20_key"] = returnGenericStringArray(l.Val20_key)

	for k, v := range gr {
		if v != nil {
			grRet[k] = v
		}
	}

	ret = []interface{}{ grRet }
	return
}

var arrFields = []string{"application_detail", "bano", "bso_key", "bwo_id", "bso_status_key", "bso_error_message_key", "destination", "imsi_key", "local_service_requests_s", "msisdn_key", "order_sub_type", "order_item_status_key", "order_item_error_message_key", "order_item_error_date_key", "product_name_key", "promotion_action_key", "promotion_code_key", "request_id", "resp_status", "service_action_key", "service_code_key", "subscriberid1_key", "subscriberid2_key", "subscriberid3_key", "wo_header_error_message_key", "wo_header_status_key", "wo_header_error_date_key", "bso_error_date_key", "val1_key", "val2_key", "val3_key", "val4_key", "val5_key", "val6_key", "val7_key", "val8_key", "val9_key", "val10_key", "val11_key", "val12_key", "val13_key", "val14_key", "val15_key", "val16_key", "val17_key", "val18_key", "val19_key", "val20_key"}
var intFields = []string{"retry_count"}
var timeFields = []string{"order_date", "submitted_date", "int_created_date", "int_updated_date", "order_info_error_date_new", "order_info_error_date_time", "order_info_error_date"}

func MakePgQuery(tab string, record map[string]interface{}) (ret string) {

	var keys, vals string

	for vv, kk := range record {

		switch inWhichArr(vv) {

		case "arrs":
			keys += vv + ","
			vals += fmt.Sprintf("ARRAY['%v']::text[],", kk)

		case "ints":
			keys += vv + ","
			vals += fmt.Sprintf("%v,", kk)

		case "timestamp":
			if vv == "int_created_date_cass" {
				continue
			}
			if vv == "int_created_date_cas" {
				continue
			}

			if vv == "int_created_date" {

				if _, ok := kk.(int); ok {
					dt := int64((kk.(int) / 1000) * 1000)

					keys += fmt.Sprintf("%v,", "int_created_date_cass")
					vals += fmt.Sprintf("to_timestamp(%v),", dt)

					continue
				} else {
					if _, ok := kk.(int64); ok {
						dt := int64((kk.(int64) / 1000) * 1000)

						keys += fmt.Sprintf("%v,", "int_created_date_cass")
						vals += fmt.Sprintf("to_timestamp(%v),", dt)

						continue
					}

				}
			}

			if vv == "int_updated_date" || vv == "order_date" || vv == "submitted_date" || vv == "order_info_error_date_new" || vv == "order_info_error_date_time" || vv == "order_info_error_date" {
				if _, ok := kk.(int); ok {
					dt := int64((kk.(int) / 1000) * 1000)

					keys += fmt.Sprintf("%v,", vv)
					vals += fmt.Sprintf("to_timestamp(%v),", dt)
					continue
				} else {
					if _, ok := kk.(int64); ok {
						dt := int64((kk.(int64) / 1000) * 1000)

						keys += fmt.Sprintf("%v,", vv)
						vals += fmt.Sprintf("to_timestamp(%v),", dt)
						continue
					}
				}
				//continue
			}

			if _, ok := kk.(int); ok {

				dt := int64((kk.(int) / 1000) * 1000)
				keys += fmt.Sprintf("%v,", vv)

				if dt > 1000000 {

					vals += fmt.Sprintf("to_timestamp(%v),", dt)
				} else {

					vals += fmt.Sprintf("%v,", dt)
				}
			} else {

				if _, ok := kk.(int64); ok {

					dt := int64((kk.(int64) / 1000) * 1000)
					keys += fmt.Sprintf("%v,", vv)

					if dt > 1000000 {
						vals += fmt.Sprintf("to_timestamp(%v),", dt)
					} else {
						vals += fmt.Sprintf("%v,", dt)
					}
				}
			}

		default:
			keys += vv + ","
			vals += fmt.Sprintf("'%v',", kk)
		}
	}

	keys = strings.TrimRight(keys, ",")
	vals = strings.TrimRight(vals, ",")

	ret += fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", tab, keys, vals)

	return
}

func inWhichArr(str string) (ret string) {

	for kk, vv := range map[string][]string{"arrs": arrFields, "ints": intFields, "timestamp": timeFields} {

		if ret == "" {
			for _, v := range vv {

				if v == str {
					ret = kk
				}
			}
		} else {
			break
		}
	}

	return
}
