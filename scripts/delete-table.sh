#!/bin/bash

MUTEX_TABLE_NAME=${DD_MUTEX_TABLE_NAME:-Mutex}

aws dynamodb delete-table --table-name $MUTEX_TABLE_NAME
