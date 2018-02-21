package main

import (
	"github.com/linkedin/goavro"
)

type lsr_record struct {
	Local_service_requests_new_con5_pk string  `"json":"local_service_requests_new_con5_pk"`
	Actor string  `"json":"actor"`
	Application_detail []string `"json":"application_detail"`
	Ba_category string  `"json":"ba_category"`
	Ba_segment string  `"json":"ba_segment"`
	Bano []string `"json":"bano"`
	Billing_system string  `"json":"billing_system"`
	Bso_key []string `"json":"bso_key"`
	Bwo_id []string `"json":"bwo_id"`
	Bso_status_key []string `"json":"bso_status_key"`
	Bso_error_message_key []string `"json":"bso_error_message_key"`
	Cano string  `"json":"cano"`
	Channel string  `"json":"channel"`
	Charge_type string  `"json":"charge_type"`
	Complete_dt string  `"json":"complete_dt"`
	Destination []string `"json":"destination"`
	Error_message string  `"json":"error_message"`
	Imsi_key []string `"json":"imsi_key"`
	Last_upd string  `"json":"last_upd"`
	Local_service_requests_s []string `"json":"local_service_requests_s"`
	Location_cd string  `"json":"location_cd"`
	Msisdn_key []string `"json":"msisdn_key"`
	Ne_id string  `"json":"ne_id"`
	Order_date int64  `"json":"order_date"`
	Order_date_str string  `"json":"order_date_str"`
	Order_ref string  `"json":"order_ref"`
	Order_status string  `"json":"order_status"`
	Order_sub_type []string `"json":"order_sub_type"`
	Order_type string  `"json":"order_type"`
	Order_item_status_key []string `"json":"order_item_status_key"`
	Order_item_error_message_key []string `"json":"order_item_error_message_key"`
	Order_item_error_date_key []string `"json":"order_item_error_date_key"`
	Pgz_record_type string  `"json":"pgz_record_type"`
	Pgz_task_id string  `"json":"pgz_task_id"`
	Product_name_key []string `"json":"product_name_key"`
	Promotion_action_key []string `"json":"promotion_action_key"`
	Promotion_code_key []string `"json":"promotion_code_key"`
	Req_status string  `"json":"req_status"`
	Request_id []string `"json":"request_id"`
	Resp_status []string `"json":"resp_status"`
	Sano string  `"json":"sano"`
	Service_action_key []string `"json":"service_action_key"`
	Service_code_key []string `"json":"service_code_key"`
	Sim_serial_no string  `"json":"sim_serial_no"`
	Submitted_date int64  `"json":"submitted_date"`
	Submitted_date_str string  `"json":"submitted_date_str"`
	Subscriberid1_key []string `"json":"subscriberid1_key"`
	Subscriberid2_key []string `"json":"subscriberid2_key"`
	Subscriberid3_key []string `"json":"subscriberid3_key"`
	User_id string  `"json":"user_id"`
	User_sys string  `"json":"user_sys"`
	Wo_header_error_message_key []string `"json":"wo_header_error_message_key"`
	Wo_header_status_key []string `"json":"wo_header_status_key"`
	Wo_header_error_date_key []string `"json":"wo_header_error_date_key"`
	Bso_error_date_key []string `"json":"bso_error_date_key"`
	Int_created_date int64  `"json":"int_created_date"`
	Int_created_date_str string  `"json":"int_created_date_str"`
	Int_updated_date int64  `"json":"int_updated_date"`
	Int_updated_date_str string  `"json":"int_updated_date_str"`
	Int_is_deleted string  `"json":"int_is_deleted"`
	Order_info_error_date_new int64  `"json":"order_info_error_date_new"`
	Order_info_error_date_new_str string  `"json":"order_info_error_date_new_str"`
	Order_info_error_date_time int64  `"json":"order_info_error_date_time"`
	Order_info_error_date_time_str string  `"json":"order_info_error_date_time_str"`
	Order_info_error_date int64  `"json":"order_info_error_date"`
	Order_info_error_date_str string  `"json":"order_info_error_date_str"`
	Request_id_int_field string  `"json":"request_id_int_field"`
	Bwo_id_int_field string  `"json":"bwo_id_int_field"`
	Retry_count int64  `"json":"retry_count"`
	Val1_key []string `"json":"val1_key"`
	Val2_key []string `"json":"val2_key"`
	Val3_key []string `"json":"val3_key"`
	Val4_key []string `"json":"val4_key"`
	Val5_key []string `"json":"val5_key"`
	Val6_key []string `"json":"val6_key"`
	Val7_key []string `"json":"val7_key"`
	Val8_key []string `"json":"val8_key"`
	Val9_key []string `"json":"val9_key"`
	Val10_key []string `"json":"val10_key"`
	Val1 string  `"json":"val1"`
	Val2 string  `"json":"val2"`
	Val3 string  `"json":"val3"`
	Val4 string  `"json":"val4"`
	Val5 string  `"json":"val5"`
	Val6 string  `"json":"val6"`
	Val7 string  `"json":"val7"`
	Val8 string  `"json":"val8"`
	Val9 string  `"json":"val9"`
	Val10 string  `"json":"val10"`
	Resend_flag string  `"json":"resend_flag"`
	File_name string  `"json":"file_name"`
	Ref_id string  `"json":"ref_id"`
	Lot_no string  `"json":"lot_no"`
	Val11_key []string `"json":"val11_key"`
	Val12_key []string `"json":"val12_key"`
	Val13_key []string `"json":"val13_key"`
	Val14_key []string `"json":"val14_key"`
	Val15_key []string `"json":"val15_key"`
	Val16_key []string `"json":"val16_key"`
	Val17_key []string `"json":"val17_key"`
	Val18_key []string `"json":"val18_key"`
	Val19_key []string `"json":"val19_key"`
	Val20_key []string `"json":"val20_key"`
}


func (l *lsr_record)getGenericRecord() (grRet map[string]interface{}) {

	gr := make(map[string]interface{})
	grRet = make(map[string]interface{})

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
	gr["int_created_date"] = returnGenericStringInt(l.Int_created_date)
	gr["int_created_date_str"] = returnGenericString(l.Int_created_date_str)
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

	// ret = []interface{}{gr}

	for k, v := range gr {

		if v != nil {

			grRet[k] = v
		}
	}

	return
}

func returnGenericString(s string) (interface{}){

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




