#!/bin/bash

NOW=$(date +%Y-%m-%dT%H:%M:%S)
NOW_TRUNC=$(echo ${NOW} | sed 's/://g' | sed 's/-//g')

DEVNAME=$(git config --get user.name)

HOSTNAME=$(hostname)


#if [ -z "$DEVNAME" ]
#	then
#		echo "Please set the \$DEVNAME variable now and press [ENTER] key"
#		read TEMPDEV
#		echo "Temporarily set your name as $TEMPDEV"
#	else
#		echo "Read your \$DEVNAME is $DEVNAME"
#		TEMPDEV=$DEVNAME
#fi


GITHASH=$(git rev-parse HEAD)



GITBRANCH="$(git branch | grep '*' | sed 's/*//g')"

GITMESSAGE=$( git log -1 --pretty=%B | cat)


if [ -z "$GOPATH" ]
then
	echo " Please set your GOPATH variable "
	exit
else
	STREAMPATH=$GOPATH/src/github.com/dminGod/Stream
	cd $STREAMPATH
	GITSTATUS=$(git status) 
fi

if [ -f $STREAMPATH/version.go ]
then
	VERSIONNUMBER=$(sed '2q;d' $STREAMPATH/version.go | sed 's/const BuildNumber \?=//g' | sed 's/"//g' )
	echo "VERSION NUMBER IS $VERSIONNUMBER"
	echo "Please press [ENTER] to keep this build number or type your build number and press [Enter]"
	read TEMPVN
	if [ -z "$TEMPVN" ]
	then
		echo "Keeping Version Number as $VERSIONNUMBER"
	else
		VERSIONNUMBER="$TEMPVN"
		echo "Changed Version Number to $VERSIONNUMBER"
	fi
else
	echo "No version number detected. Please enter a version number"
	read VERSIONNUMBER
	if [ -z "$VERSIONNUMBER" ]
	then
		VERSIONNUMBER="0.0.1"
	fi
fi

GOVERSION=$(go version)


REDHATNAME=$(uname -r | grep el)
UBUNTUNAME=$(uname -v | grep Ubuntu)


if [ -z $REDHATNAME ]
then
	OSVERSIONNAME=$(lsb_release -d | sed 's/Description://g')
else
	OSVERSIONNAME=$(cat /etc/redhat-release)
fi

echo -e "Building on $GITBRANCH branch..\n Please Enter your build comment and then press [ENTER]"
read USERCOMMENT


if [ -z "$USERCOMMENT" ]
then
	USERCOMMENT="No user comment"
fi


cat << EOF > $STREAMPATH/version.go
package main
const BuildNumber = "${VERSIONNUMBER}"
const AppVersion = \`
Build Version: "${VERSIONNUMBER}" - Build Comment: "${USERCOMMENT}"
Build Details: "${NOW}" - Dev: "${DEVNAME}" - Machine: "${HOSTNAME}"@"${OSVERSIONNAME}"  
Git Details: Branch - "${GITBRANCH}" :: Commit - "${GITHASH}"@"${GITMESSAGE}"
Go Details: "${GOVERSION}"
\`
const AppVersionSection = \`
Git Status: "${GITSTATUS}"
\`

EOF

if [ $? != 0 ]; then
        echo "Failed to write version info. Exiting"
        exit 1
fi



echo -e "Building Stream. Output will be available in builds\\ directory"

mkdir builds 2>/dev/null 
go build -o builds/stream-${NOW_TRUNC} version.go generic_records.go kafka_listner.go cassandra_client.go write_stats_influx.go utils.go stream.go

if [ ! -f builds/stream-${NOW_TRUNC} ]
then
	echo "Build Failed"
else
	echo "Build Completed. Available in builds/stream-${NOW_TRUNC}"
fi
