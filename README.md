# dyndb-mutex
A mutex implementation  leveraging [AWS DynamoDB](https://aws.amazon.com/dynamodb/)
Although this was built for use in  [AWS Lambda](https://aws.amazon.com/lambda), note that you can use this mutex implementation in any context - even outside AWS.

# Install
```
sudo pip install dyndbmutex
```
or checkout this repository and run `python setup.py`. Or download a release from the releases page and go from there.

# Usage
Let's say you want to ensure that only 1 python function can access a resource (for example an AWS instance `i-8abd82c31`) at a time

```
   from dyndbmutex.dyndbmutex import DynamoDbMutex
   # at the beginning of your function
   # generate a unique name for this process/thread
   my_name = str(uuid.uuid4()).split("-")[0]
   m = DynamoDbMutex('i-8abd832c32', holder=my_name, timeoutms=20 * 1000)
   locked = m.lock()
   if locked:
      # critical section begin
       ......
      # critical section end
      m.release()


```

You can also use the `with` pattern:

```
   from dyndbmutex.dyndbmutex import DynamoDbMutex, AcquireLockFailedError
   my_name = str(uuid.uuid4()).split("-")[0]
   m = DynamoDbMutex('i-8abd832c32', my_name, 20 * 1000)
   try:
       with m:
          # critical section
   except mutex.AcquireLockFailedError:
       #m will be released at this point

```

# Theory of operation
Uses [DynamoDb conditional write](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate) as an [atomic compare-and-swap](https://en.wikipedia.org/wiki/Compare-and-swap) operation to implement a mutex.

  * Prune lock: if the acquirer fails to release it within the timeout, release it if it is expired
  * Acquire lock: prune the lock if required. If the lock is now released, acquire it
  * Release lock: release it if I am the holder, otherwise fail.

Since the conditional write is atomic (test and set), this works very well. In fact the code doesn't even read the table, only writes to it.
(We could even make the lock re-entrant since we have the owner/holder information, but leave a note/issue if this is important for your usecase)

# Setup
The default name for the Mutex table in DynamoDB is 'Mutex'. You can change this by setting an environment variable:

```
export DD_MUTEX_TABLE_NAME=FancyPantsMutex
```

The code will auto-create the mutex DynamoDB table, but this could take at least 20 seconds. As an alternative, use the `create-table` script in the scripts directory before using this mutex library.


# Notes and Limitations
Although the code is general-purpose and can be used outside of AWS Lambda, note the following limitations:

* Not designed for fine-grained parallelism. Generally, it is expected that you acquire a lock and hold it for the duration of the lambda function
* Does not detect/prevent deadlocks. There is no spin lock, but the mutex user could create one by spinning until an acquire succeeds. Within a Lambda function, one should avoid taking more than 1 lock.
* Not re-entrant. If a thread (e.g.,a lambda function) tries to re-acquire a lock it already holds, it will block
* Not designed for speed. The DynamoDb table backing the locks is generally provisioned as low throughput (2 ops/sec)
* Cleanup - the mutex table is created with the [TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) feature. By default, the TTL is 2 days; after 2 days of disuse, the row created to hold the mutex should get automatically deleted. You can use set the `ttl_minutes` in the constructor to choose a different TTL


# TODO
* No limits on timeout. Perhaps there should be one (300 seconds?)
* Re-entrancy
