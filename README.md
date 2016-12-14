# lambda-mutex
A mutex implementation for AWS Lambda, leveraging DynamoDB

# Usage
Let's say you want to ensure that only 1 lambda function can access a resource (for example an instance i-8abd82c31) at a time

```
   from lambda-mutex import DynamoDbMutex
   # at the beginning of your lambda handler
   # generate a unique name for this instantiation of lambda
   my_name = str(uuid.uuid4()).split("-")[0]
   m = DynamoDbMutex(lockname='i-8abd832c32', holder=my_name, timeoutms=20 * 1000)
   locked = m.lock()
   if locked:
      # do your stuff

   m.release()

```

You can also use the `with` pattern:

```
   my_name = str(uuid.uuid4()).split("-")[0]
   m = DynamoDbMutex('i-8abd832c32', my_name, 20 * 1000)
   try:
       with m:
          # do your stuff
   except mutex.AcquireLockFailedError:
       #m will be released at this point

```

# Theory of operation
Uses [DynamoDb conditional write] (http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate):

  * Prune lock: if the acquirer fails to release it within the timeout, release it if it is expired
  * Acquire lock: prune the lock if required. If the lock is now released, acquire it
  * Release lock: release it if I am the holder, otherwise fail.

Since the conditional write is atomic (test and set), this works very well. In fact the code doesn't even read the table, only writes to it.
We could even make the lock re-entrant since we have the owner/holder information, but this is not a pattern in lambda.
