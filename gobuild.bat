del stream.exe
set GOOS=linux
go build stream.go cassandra_client.go generic_record.go hdfs_push.go kafka_listener.go lsr_model.go postgres.go