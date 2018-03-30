package main

import (
	"os"
	"strings"
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	"github.com/golang/snappy"
	"github.com/antigloss/go/logger"
)

func FetchAndProcessCassandra(message []map[string]interface{}) (gr []interface{}) {

	for _, vv := range message {

		// Decompress the lsr_s field
		if _, ok := vv["local_service_requests_s"].([]byte); ok {

			if len(vv["local_service_requests_s"].([]byte)) > 0 {

				m, err := snappy.Decode(nil, vv["local_service_requests_s"].([]byte))
				if err != nil {
					logger.Error("There was an error decoding the compressed message, Error : %v ", err)
					os.Exit(1)
				}

				lsrs := strings.Replace(string(m), `"`, `\"`, -1)
				vv["local_service_requests_s"] = []string{lsrs}
			}
		} else {

			vv["local_service_requests_s"] = []string{""}
		}

		if _, ok := vv["local_service_requests_s"].([]string); !ok {

			vv["local_service_requests_s"] = []string{""}
		}

		// Take the lsr_pk field -- it will come as gocql.UUID -- change it to string
		if _, ok := vv["local_service_requests_new_con5_pk"].(gocql.UUID); ok {

			vv["local_service_requests_new_con5_pk"] = vv["local_service_requests_new_con5_pk"].(gocql.UUID).String()
			logger.Info(vv["local_service_requests_new_con5_pk"].(string))
		}

		var k lsr_record

		mapstructure.Decode(vv, &k)
		gr = k.getGenericRecord()
	}

	return
}