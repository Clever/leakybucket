#!/usr/bin/env bash

set -x

jar=dynamo-local/DynamoDBLocal.jar

# download the dynamo jar if necessary
if [ ! -e "$jar" ]
then
    if [ `uname` = "Darwin" ] ; then
	# this will prompt for java to be installed if necessary
	java -version
    else
    	sudo apt-get update && sudo apt-get install -y default-jre
    fi
    mkdir -p dynamo-local
    echo "Downloading dynamo server..."
    curl -L -k --url https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz -o dynamo-local/dynamodb_local_latest.tar.gz
    tar -zxvf dynamo-local/dynamodb_local_latest.tar.gz -C dynamo-local/
fi

# start up DynamoDBLocal for integration tests
java -jar "$jar" -sharedDb -inMemory -port 8002 &
sleep 2
export AWS_DYNAMO_ENDPOINT=http://localhost:8002

# run our tests
go test -v github.com/Clever/leakybucket/dynamodb
err=$?

# kill all child processes to clean up
pkill -P $$
exit $err
