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

import threading
import time
import unittest

from tornado import ioloop

import motor
from motor.motortest import (
    MotorTest, async_test_engine, host, port, AssertEqual)


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

    def _test_tailable_cursor(self, await_data):
	# 1. Make a capped collection
	# 2. Spawn a thread that will use PyMongo to add data to the collection
	#   periodically
	# 3. Tail the capped collection with Motor
	# 4. Shut down the thread
	# 5. Assert tailable cursor got the right data
	# 6. Drop the capped collection
	# Seconds between inserts into capped collection -- simulate an
	# unpredictable process
	pauses = (2, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0.1, 0.1, 2, 0, 0)

	self.sync_db.capped.drop()
	self.sync_db.create_collection('capped', capped=True, size=10000)

	def add_docs():
	    i = 0
	    for pause in pauses:
		time.sleep(pause)
		self.sync_cx.test.capped.insert({'_id': i}, safe=True)
		i += 1

	t = threading.Thread(target=add_docs)
	t.start()

	# TODO: also test w/o await_data
	cx = self.motor_connection(host, port)
	capped = cx.test.capped
	cursor = [capped.find(tailable=True, await_data=await_data)]
	results = []

	def each(result, error):
	    if result:
		results.append(result)
	    if len(results) == len(pauses):
		# Cancel iteration
		cursor[0].close()
		return False

	    # On an empty capped collection the cursor will die immediately,
	    # despite await_data. Just keep trying until the thread inserts
	    # the first doc, at which point the cursor will remain alive for
	    # the rest of the test.
	    if not cursor[0].alive:
		cursor[0] = capped.find(tailable=True, await_data=await_data)
		cursor[0].each(each)

	# Start
	cursor[0].each(each)

	self.assertEventuallyEqual(
	    results,
	    lambda: [{'_id': i} for i in range(len(pauses))],
	    timeout_sec=sum(pauses) + 1
	)

	ioloop.IOLoop.instance().start()

	# We get here once assertEventuallyEqual has passed or timed out
	t.join()
	self.wait_for_cursors()

    def test_tailable_cursor_await(self):
	self._test_tailable_cursor(True)

    def test_tailable_cursor_no_await(self):
	self._test_tailable_cursor(False)

    def _test_tail(self, await_data):
	# Same as _test_tailable_cursor but uses Motor's own tail(callback) API
	# TODO: combine w/ _test_tailable_cursor
	pauses = (2, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0.1, 0.1, 2, 0, 0)

	self.sync_db.capped.drop()
	self.sync_db.create_collection('capped', capped=True, size=10000)

	def add_docs():
	    i = 0
	    for pause in pauses:
		time.sleep(pause)
		self.sync_cx.test.capped.insert({'_id': i}, safe=True)
		i += 1

	t = threading.Thread(target=add_docs)
	t.start()

	def each(result, error):
	    if result:
		results.append(result)
	    if len(results) == len(pauses):
		# Cancel iteration
		results.append('cancelled')
		return False

	# Start
	cx = self.motor_connection(host, port)
	capped = cx.test.capped

	# Note we do *not* pass tailable or await_data to find(), the
	# convenience method handles it for us.
	capped.find().tail(each, await_data=await_data)
	results = []

	self.assertEventuallyEqual(
	    results,
	    lambda: [{'_id': i} for i in range(len(pauses))] + ['cancelled'],
	    timeout_sec=sum(pauses) + 1
	)

	ioloop.IOLoop.instance().start()
	t.join()

	# Give the final each() a chance to execute before dropping collection
	ioloop.IOLoop.instance().add_timeout(
	    time.time() + 0.5,
	    self.sync_cx.test.capped.drop
	)

	self.wait_for_cursors()

    def test_tail_await(self):
	# TODO: this appears to fail solely because it's executed *AFTER*
	# test_cursor_slice(). Curious....
	self._test_tail(True)

    def test_tail_no_await(self):
	self._test_tail(False)

    @async_test_engine()
    def test_cursor_slice(self):
	# This seems to make the next test after it fail; right now that's
	# test_tail_await.

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
