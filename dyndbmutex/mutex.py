import logging
import boto3
import botocore
import datetime
import uuid
import os
from boto3.dynamodb.conditions import Attr


logger = logging.getLogger('mutex')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


DEFAULT_MUTEX_TABLE_NAME = 'Mutex'
NO_HOLDER = '__empty__'


class AcquireLockFailedError(Exception):
        pass


def timestamp_millis():
    return int((datetime.datetime.utcnow() -
                datetime.datetime(1970, 1, 1)).total_seconds() * 1000)


class MutexTable:

    def __init__(self, region_name='us-west-2'):
        self.dbresource = boto3.resource('dynamodb', region_name=region_name)
        self.dbclient = boto3.client('dynamodb', region_name=region_name)
        self.table_name = os.environ.get('DD_MUTEX_TABLE_NAME')
        if self.table_name is None:
            self.table_name = DEFAULT_MUTEX_TABLE_NAME
        logger.info("Mutex table name is " + self.table_name)
        self.get_table()

    def get_table(self):
        found = True
        try:
            self.dbclient.describe_table(TableName=self.table_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                found = False
        if found:
            return self.dbresource.Table(self.table_name)
        return self.create_table()

    def delete_table(self):
        self.dbclient.delete_table(TableName=self.table_name)
        logger.debug("Deleted table")

    def create_table(self):
        table = self.dbresource.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'lockname',
                    'KeyType': 'HASH'  # Partition key
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'lockname',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 2,
                'WriteCapacityUnits': 2
            }
        )
        logger.debug("Called create_table")
        table.wait_until_exists()
        logger.debug("Created table")
        return table

    def write_lock_item(self, lockname, caller, waitms):
        expire_ts = timestamp_millis() + waitms
        logger.debug("Write_item: lockname=" + lockname + ", caller=" +
                     caller + ", Expire time is " + str(expire_ts))
        try:
            self.get_table().put_item(
                Item={
                    'lockname': lockname,
                    'expire_ts': expire_ts,
                    'holder': caller
                },
                # TODO: adding Attr("holder").eq(caller) should make it re-entrant
                ConditionExpression=Attr("holder").eq(NO_HOLDER) | Attr('lockname').not_exists()
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.debug("Write_item: lockname=" + lockname +
                             ", caller=" + caller + ", lock is being held")
                return False
        logger.debug("Write_item: lockname=" + lockname +
                     ", caller=" + caller + ", lock is acquired")
        return True

    def clear_lock_item(self, lockname, caller):
        try:
            self.get_table().put_item(
                Item={
                    'lockname': lockname,
                    'expire_ts': 0,
                    'holder': NO_HOLDER
                },
                ConditionExpression=Attr("holder").eq(caller) | Attr('lockname').not_exists()
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.debug("clear_lock_item: lockname=" + lockname + ", caller=" + caller +
                             " release failed")
                return False
        logger.debug("clear_lock_item: lockname=" + lockname + ", caller=" + caller + " release succeeded")
        return True

    def prune_expired(self, lockname, caller):
        now = timestamp_millis()
        logger.debug("Prune: lockname=" + lockname + ", caller=" + caller +
                     ", Time now is " + str(now))
        try:
            self.get_table().put_item(
                Item={
                    'lockname': lockname,
                    'expire_ts': 0,
                    'holder': NO_HOLDER
                },
                ConditionExpression=Attr("expire_ts").lt(now) | Attr('lockname').not_exists()
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.debug("Prune: lockname=" + lockname + ", caller=" + caller +
                             " Prune failed")
                return False
        logger.debug("Prune: lockname=" + lockname + ", caller=" + caller + " Prune succeeded")
        return True


class DynamoDbMutex:

    def __init__(self, name, holder=str(uuid.uuid4()),
                 timeoutms=30 * 1000, region_name='us-west-2'):
        self.lockname = name
        self.holder = holder
        self.timeoutms = timeoutms
        self.table = MutexTable(region_name=region_name)
        self.locked = False

    def lock(self):
        self.table.prune_expired(self.lockname, self.holder)
        self.locked = self.table.write_lock_item(self.lockname, self.holder, self.timeoutms)
        logger.debug("mutex.lock(): lockname=" + self.lockname + ", locked = " + str(self.locked))
        return self.locked

    def release(self):
        self.table.clear_lock_item(self.lockname, self.holder)
        self.locked = False

    def __enter__(self):
        locked = self.lock()
        if not locked:
            raise AcquireLockFailedError()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def is_locked(self):
        return self.locked

    @staticmethod
    def delete_table(region_name='us-west-2'):
        table = MutexTable(region_name)
        table.delete_table()
