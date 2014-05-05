#!/usr/bin/env python2.7

import time
import unittest

from memcev import Client

class TestMemcev(unittest.TestCase):
    def setUp(self):
        try:
            self.client = Client('localhost', 11211)
        except:
            print "tests need memcached running on localhost:11211"
            raise

    def tearDown(self):
        self.client.close()


    def test_check(self):
        self.client.check()

    def test_check_fails(self):
        # make sure that check actually proves that the queueing machinery works
        self.client.stop()
        self.assertRaises(Exception, self.client.check())

    def test_get_missing(self):
        self.assertEqual(self.client.get('doesntexist'), None)

    def test_set(self):
        self.client.set('foo', 'bar')
        self.assertEqual(self.client.get('foo'), 'bar')

    def test_set_empty_string(self):
        self.client.set('empty', '')
        self.assertEqual(self.client.get('empty'), '')

    def test_invalid_key(self):
        self.assertRaises(ValueError, lambda: self.client.set('a'*500, ''))
        self.assertRaises(ValueError, lambda: self.client.set(1, ''))
        self.assertRaises(ValueError, lambda: self.client.set('', ''))

    def test_invalid_value(self):
        self.assertRaises(ValueError, lambda: self.client.set('foo', 'a'*1024*1024+'b'))
        self.assertRaises(ValueError, lambda: self.client.set('foo', 1))

    def test_connect_timeout(self):
        self.assertRaises(Exception, lambda: Client('missinghost',11211))

    def test_connect_refused(self):
        self.assertRaises(Exception, lambda: Client('localhost',11212))

    def test_double_close(self):
        c = Client('localhost', 11211)
        c.close()
        self.assert_(c._closed)
        c.close()
        self.assert_(c._closed)

    def test_expires(self):
        self.client.set('foo', 'bar', 1)
        time.sleep(2)
        self.assertEqual(self.client.get('foo'), None)

if __name__ == '__main__':
    unittest.main()
