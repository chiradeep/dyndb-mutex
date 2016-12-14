#!/bin/bash
aws dynamodb create-table \
    --table-name Mutex \
    --attribute-definitions \
        AttributeName=lockname,AttributeType=S \
    --key-schema AttributeName=lockname,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=2,WriteCapacityUnits=2
