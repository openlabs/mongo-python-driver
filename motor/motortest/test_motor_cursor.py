# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test Motor, an asynchronous driver for MongoDB and Tornado."""

import unittest

from tornado import ioloop

import motor
from motor.motortest import (
    MotorTest, async_test_engine, host, port, AssertEqual)
import pymongo


class MotorCursorTest(MotorTest):
    def test_cursor(self):
        cx = self.motor_connection(host, port)
        coll = cx.test.foo
        cursor = coll.find()
        self.assert_(isinstance(cursor, motor.MotorCursor))
        self.assertFalse(cursor.started, "Cursor shouldn't start immediately")

    @async_test_engine()
    def test_count(self):
        coll = self.motor_connection(host, port).test.test_collection
        yield AssertEqual(200, coll.find().count)
        yield AssertEqual(100, coll.find({'_id': {'$gt': 99}}).count)
        where = 'this._id % 2 == 0 && this._id >= 50'
        yield AssertEqual(75, coll.find({'$where': where}).count)
        yield AssertEqual(75, coll.find().where(where).count)
        yield AssertEqual(
            25,
            coll.find({'_id': {'$lt': 100}}).where(where).count)
        yield AssertEqual(
            25,
            coll.find({'_id': {'$lt': 100}, '$where': where}).count)

    @async_test_engine()
    def test_next(self):
        coll = self.motor_connection(host, port).test.test_collection
        cursor = coll.find({}, {'_id': 1}).sort([('_id', pymongo.ASCENDING)])
        yield AssertEqual({'_id': 0}, cursor.next)
        cursor.close()
        self.wait_for_cursors()

    def test_each(self):
        coll = self.motor_connection(host, port).test.test_collection
        cursor = coll.find({}, {'_id': 1}).sort([('_id', pymongo.ASCENDING)])

        results = []
        def callback(result, error):
            if error:
                raise error

            results.append(result)

        cursor.each(callback)
        expected = [{'_id': i} for i in range(200)] + [None]
        self.assertEventuallyEqual(expected, lambda: results)
        ioloop.IOLoop.instance().start()

    def test_cursor_close(self):
        # The flow here is complex; we're testing that a cursor can be explicitly
        # closed.
        # 1. Create a cursor on the server by running find()
        # 2. In the find() callback, start closing the cursor
        # 3. Wait a little to make sure the cursor closes
        # 4. Stop the IOLoop so we can exit test_cursor_close()
        # 5. In MotorTest.tearDown(), we'll assert all cursors have closed.
        cx = self.motor_connection(host, port)
        loop = ioloop.IOLoop.instance()

        def found(result, error):
            loop.stop()
            if error:
                raise error

            cursor.close()

            # Cancel iteration, so the cursor isn't exhausted
            return False

        cursor = cx.test.test_collection.find()
        cursor.each(callback=found)

        # Start the find(), the callback will close the cursor
        loop.start()
        self.wait_for_cursors()

    @async_test_engine()
    def test_cursor_slice(self):
        # This is an asynchronous copy of PyMongo's test_getitem_slice_index in
        # test_cursor.py

        cx = self.motor_connection(host, port)

        # test_collection was filled out in setUp()
        coll = cx.test.test_collection

        self.assertRaises(IndexError, lambda: coll.find()[-1])
        self.assertRaises(IndexError, lambda: coll.find()[1:2:2])
        self.assertRaises(IndexError, lambda: coll.find()[2:1])

        result = yield motor.Op(coll.find()[0:].to_list)
        self.assertEqual(200, len(result))

        result = yield motor.Op(coll.find()[20:].to_list)
        self.assertEqual(180, len(result))

        result = yield motor.Op(coll.find()[99:].to_list)
        self.assertEqual(101, len(result))

        result = yield motor.Op(coll.find()[1000:].to_list)
        self.assertEqual(0, len(result))

        result = yield motor.Op(coll.find()[20:25].to_list)
        self.assertEqual(5, len(result))

        # Any slice overrides all previous slices
        result = yield motor.Op(coll.find()[20:25][20:].to_list)
        self.assertEqual(180, len(result))

        result = yield motor.Op(coll.find()[20:25].limit(0).skip(20).to_list)
        self.assertEqual(180, len(result))

        result = yield motor.Op(coll.find().limit(0).skip(20)[20:25].to_list)
        self.assertEqual(5, len(result))

        result = yield motor.Op(coll.find()[:1].to_list)
        self.assertEqual(1, len(result))

        result = yield motor.Op(coll.find()[:5].to_list)
        self.assertEqual(5, len(result))

        result = yield motor.Op(coll.find()[:0].to_list)
        self.assertEqual(0, len(result))


if __name__ == '__main__':
    unittest.main()
