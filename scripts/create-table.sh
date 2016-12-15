#!/bin/bash

set -x
MUTEX_TABLE_NAME=${DD_MUTEX_TABLE_NAME:-Mutex}

aws dynamodb create-table \
    --table-name $MUTEX_TABLE_NAME \
    --attribute-definitions \
        AttributeName=lockname,AttributeType=S \
    --key-schema AttributeName=lockname,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=2,WriteCapacityUnits=2
