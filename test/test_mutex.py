import time
import uuid
import unittest
import base64
import sys
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from dyndbmutex.mutex import DynamoDbMutex, AcquireLockFailedError


def random_name():
    return base64.b32encode(str(uuid.uuid4()))[:7]


class TestDynamoDbMutex(unittest.TestCase):

    def test_create(self):
        m = DynamoDbMutex(random_name(), "myself", 3 * 1000)
        assert(m.lock())
        m.release()

    def test_create_delete_us_east_1(self):
        m = DynamoDbMutex(name=random_name(), holder=random_name(),
                          region_name='us-east-1')
        assert(m.lock())
        m.release()
        DynamoDbMutex.delete_table(region_name='us-east-1')

    def test_timeout(self):
        m = DynamoDbMutex(random_name(), "myself", 3 * 1000)
        m.lock()
        time.sleep(5)
        assert(m.lock())
        m.release()

    def test_mutual_exclusion(self):
        m = DynamoDbMutex(random_name(), holder=random_name())
        m.lock()
        assert(m.lock() == False)
        m.release()

    def test_with(self):
        m = DynamoDbMutex(name=random_name(), holder=random_name())
        try:
            with m:
                time.sleep(3)
                raise
        except:
            print("In exception handler")
            assert(m.is_locked() == False)

    def test_with_fail(self):
        name = random_name()
        m1 = DynamoDbMutex(name=name, holder=random_name())
        m1.lock()
        m2 = DynamoDbMutex(name=name, holder=random_name())
        exceptionHappened = False
        try:
            with m2:
                time.sleep(3)
        except AcquireLockFailedError:
            print("In exception handler")
            assert(m2.is_locked() == False)
            exceptionHappened = True
        assert(exceptionHappened)

    def test_release_expired(self):
        name = random_name()
        caller = "caller1"
        m1 = DynamoDbMutex(name=name, holder=caller, timeoutms=2 * 1000)
        m1.lock()
        time.sleep(3)
        caller = "caller2"
        m2 = DynamoDbMutex(name=name, holder=caller, timeoutms=2 * 1000)
        assert(m2.lock())
        m1.release()
        assert(m2.is_locked())
        m2.release()


if __name__ == "__main__":
    unittest.main()
