import time
import unittest
import base64
import sys
import warnings
import os
from os import path

from concurrent.futures import ThreadPoolExecutor

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from dyndbmutex.dyndbmutex import DynamoDbMutex, AcquireLockFailedError, setup_logging


def random_name():
    return base64.b32encode(os.urandom(5))[:7].decode('ascii')


class TestDynamoDbMutex(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_logging()
        super(TestDynamoDbMutex, cls).setUpClass()

    def setUp(self):
        #python 3.6 emits warnings due to requests which can be ignoredz    
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

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
        self.assertTrue(m.lock())
        m.release()

    def test_mutual_exclusion(self):
        m = DynamoDbMutex(random_name(), holder=random_name())
        m.lock()
        self.assertFalse(m.lock())
        m.release()

    def test_with(self):
        m = DynamoDbMutex(name=random_name(), holder=random_name())
        try:
            with m:
                time.sleep(3)
                raise
        except:
            print("In exception handler")
            self.assertFalse(m.is_locked())

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
            self.assertFalse(m2.is_locked())
            exceptionHappened = True
        self.assertTrue(exceptionHappened)

    def test_release_expired(self):
        name = random_name()
        caller = "caller1"
        m1 = DynamoDbMutex(name=name, holder=caller, timeoutms=2 * 1000)
        m1.lock()
        time.sleep(3)
        caller = "caller2"
        m2 = DynamoDbMutex(name=name, holder=caller, timeoutms=2 * 1000)
        self.assertTrue(m2.lock())
        m1.release()
        self.assertTrue(m2.is_locked())
        m2.release()

    def test_ttl(self):
        os.environ['DD_MUTEX_TABLE_NAME'] = random_name()
        name = random_name()
        caller = "myself"
        m1 = DynamoDbMutex(name=name, holder=caller, timeoutms=2 * 1000, ttl_minutes=2)
        m1.lock()
        item = m1.get_raw_lock()
        diff = item['Item']['ttl'] - item['Item']['expire_ts']
        print (diff)
        self.assertTrue(diff > 0 and diff <= 2*60)
        DynamoDbMutex.delete_table()
        del os.environ['DD_MUTEX_TABLE_NAME']

    def test_table_creation_race(self):
        old_table_name = os.environ.get('DD_MUTEX_TABLE_NAME')
        try:
            table_name = 'Mutex-' + random_name()
            os.environ['DD_MUTEX_TABLE_NAME'] = table_name
            cls = DynamoDbMutex

            def create_mutex(i):
                mutex_name = random_name()
                mutex = cls(name=mutex_name, holder='caller%i' % i)
                self.assertEqual(mutex.table.table_name, table_name)
                return mutex

            num_threads = 2
            with ThreadPoolExecutor(max_workers=num_threads) as pool:
                mutexes = pool.map(create_mutex, range(num_threads))
                for method in cls.lock, cls.release, cls.delete_table:
                    pool.map(method, mutexes)
        finally:
            if old_table_name is None:
                del os.environ['DD_MUTEX_TABLE_NAME']
            else:
                os.environ['DD_MUTEX_TABLE_NAME'] = old_table_name


if __name__ == "__main__":
    unittest.main()
