package main
const BuildNumber = "0.1"
const AppVersion = `
Build Version: "0.1" - Build Comment: "Tuere"
Build Details: "2018-04-27T21:21:25" - Dev: "Abhishek Kulkarni" - Machine: "unotech-HP-ProBook-450-G3"@"	Ubuntu 17.10"  
Git Details: Branch - " PushHDFS" :: Commit - "c0081ce837d458d6204a89c2fcf4b0ad75014814"@"Fixed a bug in Banner"
Go Details: "go version go1.8.3 linux/amd64"
`
const AppVersionSection = `
Git Status: "On branch PushHDFS
Your branch is up-to-date with 'origin/PushHDFS'.

Changes to be committed:
  (use "git reset HEAD <file>..." to unstage)

	new file:   lsr.avro
	new file:   lsr.cql
	new file:   utils.go

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git checkout -- <file>..." to discard changes in working directory)

	modified:   .idea/workspace.xml
	modified:   app_config/app_config.go
	modified:   lsr.avro
	modified:   lsr.cql
	modified:   stream.go
	modified:   utils.go
	modified:   version.go

Untracked files:
  (use "git add <file>..." to include in what will be committed)

	KafkaTrigger.yml
	alt_stream.bin
	cass_schema.cql
	cassandra-trigger-0.0.1-SNAPSHOT.jar
	cassandra_trigger_java_code/target/classes/
	cassandra_trigger_java_code/target/maven-archiver/
	cassandra_trigger_java_code/target/maven-status/
	cassandra_trigger_java_code/target/original-cassandra-trigger-0.0.1-SNAPSHOT.jar
	stream.avro
	stream.toml"
`

