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

import datetime
import time
import unittest

from tornado import ioloop, stack_context

import motor
import pymongo

from motor.motortest import (
    MotorTest, async_test_engine, host, port, AssertRaises, AssertEqual)
from pymongo.errors import (
    InvalidOperation, ConfigurationError, DuplicateKeyError)
from bson.objectid import ObjectId
from test.utils import server_is_master_with_slave, delay
from test import version


class MotorConnectionTest(MotorTest):
    @async_test_engine()
    def test_connection(self):
        cx = motor.MotorConnection(host, port)

        # Can't access databases before connecting
        self.assertRaises(
            pymongo.errors.InvalidOperation,
            lambda: cx.some_database_name
        )

        self.assertRaises(
            pymongo.errors.InvalidOperation,
            lambda: cx['some_database_name']
        )

        result = yield motor.Op(cx.open)
        self.assertEqual(result, cx)
        self.assertTrue(cx.connected)

    def test_connection_callback(self):
        cx = motor.MotorConnection(host, port)
        self.check_optional_callback(cx.open)

    @async_test_engine()
    def test_copy_db(self):
        cx = self.motor_connection(host, port)
        self.assertFalse(cx.in_request())

        with cx.start_request():
            self.assertTrue(cx.in_request())
            yield AssertRaises(TypeError, cx.copy_database, 4, "foo")
            yield AssertRaises(TypeError, cx.copy_database, "foo", 4)

            yield AssertRaises(
                pymongo.errors.InvalidName, cx.copy_database, "foo", "$foo")

            yield motor.Op(cx.pymongo_test.test.drop)
            yield motor.Op(cx.drop_database, "pymongo_test1")
            yield motor.Op(cx.drop_database, "pymongo_test2")

            yield motor.Op(cx.pymongo_test.test.insert, {"foo": "bar"})

            # Due to SERVER-2329, databases may not disappear from a master in a
            # master-slave pair
            if not server_is_master_with_slave(self.sync_cx):
                db_names = yield motor.Op(cx.database_names)
                self.assertFalse("pymongo_test1" in db_names)
                self.assertFalse("pymongo_test2" in db_names)

            yield motor.Op(cx.copy_database, "pymongo_test", "pymongo_test1")

            # copy_database() didn't accidentally end the request
            self.assertTrue(cx.in_request())

            db_names = yield motor.Op(cx.database_names)
            self.assertTrue("pymongo_test1" in db_names)
            result = yield motor.Op(cx.pymongo_test1.test.find_one)
            self.assertEqual("bar", result["foo"])

        self.assertFalse(cx.in_request())
        yield motor.Op(cx.copy_database, "pymongo_test", "pymongo_test2",
                        "%s:%d" % (host, port))
        # copy_database() didn't accidentally restart the request
        self.assertFalse(cx.in_request())

        db_names = yield motor.Op(cx.database_names)
        self.assertTrue("pymongo_test2" in db_names)
        result = yield motor.Op(cx.pymongo_test2.test.find_one)
        self.assertEqual("bar", result["foo"])

        if version.at_least(self.sync_cx, (1, 3, 3, 1)):
            yield motor.Op(cx.drop_database, "pymongo_test1")

            yield motor.Op(cx.pymongo_test.add_user, "mike", "password")

            yield AssertRaises(
                pymongo.errors.OperationFailure,
                cx.copy_database, "pymongo_test", "pymongo_test1",
                username="foo", password="bar")

            if not server_is_master_with_slave(self.sync_cx):
                db_names = yield motor.Op(cx.database_names)
                self.assertFalse("pymongo_test1" in db_names)

            yield AssertRaises(
                pymongo.errors.OperationFailure, cx.copy_database,
                "pymongo_test", "pymongo_test1",
                username="mike", password="bar")

            if not server_is_master_with_slave(self.sync_cx):
                db_names = yield motor.Op(cx.database_names)
                self.assertFalse("pymongo_test1" in db_names)

            yield motor.Op(
                cx.copy_database, "pymongo_test", "pymongo_test1",
                username="mike", password="password")

            db_names = yield motor.Op(cx.database_names)
            self.assertTrue("pymongo_test1" in db_names)
            result = yield motor.Op(cx.pymongo_test1.test.find_one)
            self.assertEqual("bar", result["foo"])

    def test_get_last_error(self):
        # Create a unique index on 'x', insert the same value for x twice,
        # assert DuplicateKeyError is passed to callback for second insert.
        # Try again in a request, with an unsafe insert followed by an explicit
        # call to database.error(), which raises InvalidOperation because we
        # can't make two concurrent operations in a request. Finally, insert
        # unsafely and call error() again, check that we get the getLastError
        # result correctly, which checks that we're using a single socket in
        # the request as expected.
        # TODO: test that ensure_index calls the callback even if the index
        # is already created and in the index cache - might be a special-case
        # optimization

        # Use a special collection for this test
        sync_coll = self.sync_db.test_get_last_error
        sync_coll.drop()
        cx = self.motor_connection(host, port)
        coll = cx.test.test_get_last_error

        results = []

        def ensured_index(result, error):
            if error:
                raise error

            results.append(result)
            coll.insert({'x':1}, callback=inserted1)

        def inserted1(result, error):
            if error:
                raise error

            results.append(result)
            coll.insert({'x':1}, callback=inserted2)

        def inserted2(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            results.append(result)

            with cx.start_request():
                coll.insert(
                    {'x':1},
                    safe=False,
                    callback=inserted3
                )

        def inserted3(result, error):
            # No error, since we passed safe=False to insert()
            self.assertEqual(None, error)
            results.append(result)

            # We're still in the request begun in inserted2
            cx.test.error(callback=on_get_last_error)

        def on_get_last_error(result, error):
            if error:
                # This is unexpected -- Motor raised an exception trying to
                # execute getLastError on the server
                raise error

            results.append(result)

        # start the sequence of callbacks
        cx.test.test_get_last_error.ensure_index(
            [('x', 1)], unique=True, callback=ensured_index
        )

        # index name
        self.assertEventuallyEqual('x_1', lambda: results[0])

        # result of first insert
        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[1], ObjectId)
        )

        # result of second insert - failed with DuplicateKeyError
        self.assertEventuallyEqual(None, lambda: results[2])

        # result of third insert - failed, but safe=False
        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[3], ObjectId)
        )

        # result of error()
        self.assertEventuallyEqual(
            11000,
            lambda: results[4]['code']
        )

        ioloop.IOLoop.instance().start()
        self.sync_db.test_get_last_error.drop()

    @async_test_engine()
    def test_get_last_error_gen(self):
        # Same as test_get_last_error, but using gen
        cx = self.motor_connection(host, port)
        coll = cx.text.test_get_last_error
        yield motor.Op(coll.drop)

        yield AssertEqual('x_1', coll.ensure_index, [('x', 1)], unique=True)
        result = yield motor.Op(coll.insert, {'x':1})
        self.assertTrue(isinstance(result, ObjectId))

        yield AssertRaises(DuplicateKeyError, coll.insert, {'x':1})

        with cx.start_request():
            result = yield motor.Op(
                coll.insert,
                    {'x':1},
                safe=False
            )

            # insert failed, but safe=False so it returned the
            # driver-generated _id
            self.assertTrue(isinstance(result, ObjectId))

            # We're still in the request, so getLastError will work
            result = yield motor.Op(cx.test.error)
            self.assertEqual(11000, result['code'])

        yield motor.Op(coll.drop)

    def test_no_concurrent_ops_in_request(self):
        # Check that an attempt to do two things at once in a request raises
        # InvalidOperation
        results = []
        cx = self.motor_connection(host, port)

        def inserted(result, error):
            results.append({
                'result': result,
                'error': error,
            })

        with cx.start_request():
            cx.test.test_collection.insert({})
            cx.test.test_collection.insert({}, callback=inserted)

        self.assertEventuallyEqual(
            None,
            lambda: results[0]['result']
        )

        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[0]['error'], InvalidOperation)
        )

        ioloop.IOLoop.instance().start()

    def _test_request(self, chain0_in_request, chain1_in_request):
        # Sequence:
        # We have two chains of callbacks, chain0 and chain1. A chain is a
        # sequence of callbacks, each spawned by the previous callback on the
        # chain. We test the following sequence:
        #
        # 0.00 sec: chain0 makes a bad insert
        # 0.25 sec: chain1 makes a good insert
        # 0.50 sec: chain0 checks getLastError
        # 0.75 sec: chain1 checks getLastError
        # 1.00 sec: IOLoop stops
        #
        # If start_request() works, then chain 0 gets the DuplicateKeyError
        # when it runs in a request, and neither chain gets the error when
        # they run with no request.
        gap_seconds = 0.25
        cx = self.motor_connection(host, port)
        loop = ioloop.IOLoop.instance()

        # Results for chain 0 and chain 1
        results = {
            0: [],
            1: [],
        }

        def insert(chain_num, use_request, doc):
            request = None
            if use_request:
                request = cx.start_request()
                request.__enter__()

            # Perhaps causes DuplicateKeyError, depending on doc
            cx.test.test_collection.insert(doc)
            loop.add_timeout(
                datetime.timedelta(seconds=2*gap_seconds),
                lambda: inserted(chain_num)
            )

            if use_request:
                request.__exit__(None, None, None)

        def inserted(chain_num):
            cb = lambda result, error: got_error(chain_num, result, error)
            cx.test.error(callback=cb)

        def got_error(chain_num, result, error):
            if error:
                raise error

            results[chain_num].append(result)

        # Start chain 0. Causes DuplicateKeyError.
        insert(chain_num=0, use_request=chain0_in_request, doc={'s': hex(4)})

        # Start chain 1, 0.25 seconds from now. Succeeds: no error on insert.
        loop.add_timeout(
            datetime.timedelta(seconds=gap_seconds),
            lambda: insert(
                chain_num=1, use_request=chain1_in_request, doc={'s': hex(201)}
            )
        )

        loop.add_timeout(datetime.timedelta(seconds=4*gap_seconds), loop.stop)
        loop.start()
        return results

    def test_start_request(self):
        # getLastError works correctly only chain 0 is in a request
        results = self._test_request(True, False)
        self.assertEqual(11000, results[0][0]['code'])
        self.assertEqual([None], results[1])

    def test_start_request2(self):
        # getLastError works correctly when *both* chains are in requests
        results = self._test_request(True, True)
        self.assertEqual(11000, results[0][0]['code'])
        self.assertEqual([None], results[1])

    def test_no_start_request(self):
        # getLastError didn't get the error: chain0 and chain1 used the
        # same socket, so chain0's getLastError was checking on chain1's
        # insert, which had no error.
        results = self._test_request(False, False)
        self.assertEqual([None], results[0])
        self.assertEqual([None], results[1])

    def test_no_start_request2(self):
        # getLastError didn't get the error: chain0 and chain1 used the
        # same socket, so chain0's getLastError was checking on chain1's
        # insert, which had no error.
        results = self._test_request(False, True)
        self.assertEqual([None], results[0])
        self.assertEqual([None], results[1])

    def test_timeout(self):
        # Launch two slow find_ones. The one with a timeout should get an error
        loop = ioloop.IOLoop.instance()
        no_timeout = self.motor_connection(host, port)
        timeout = self.motor_connection(host, port, socketTimeoutMS=100)

        results = []
        query = {
            '$where': delay(0.5),
            '_id': 1,
        }

        def callback(result, error):
            results.append({'result': result, 'error': error})

        no_timeout.test.test_collection.find_one(query, callback=callback)
        timeout.test.test_collection.find_one(query, callback=callback)

        self.assertEventuallyEqual(
            True,
            lambda: isinstance(
                results[0]['error'],
                pymongo.errors.AutoReconnect
            )
        )

        self.assertEventuallyEqual(
            {'_id':1, 's':hex(1)},
            lambda: results[1]['result']
        )

        loop.start()

        # Make sure the delay completes before we call tearDown() and try to
        # drop the collection
        time.sleep(0.5)

    @async_test_engine()
    def test_max_pool_size_validation(self):
        cx = motor.MotorConnection(host=host, port=port, max_pool_size=-1)
        yield AssertRaises(ConfigurationError, cx.open)

        cx = motor.MotorConnection(host=host, port=port, max_pool_size='foo')
        yield AssertRaises(ConfigurationError, cx.open)

        c = motor.MotorConnection(host=host, port=port, max_pool_size=100)
        yield motor.Op(c.open)
        self.assertEqual(c.max_pool_size, 100)

    def test_pool_request(self):
        # 1. Create a connection
        # 2. Get two sockets while keeping refs to both, check they're different
        # 3. Dereference both sockets, check they're reclaimed by pool
        # 4. Get a socket in a request
        # 5. Get a socket not in a request, check different
        # 6. Get another socket in request, check we get InvalidOperation (no
        #   concurrent ops in request)
        # 7. Dereference request socket, check it's reclaimed by pool
        # 8. Get two sockets, once in request and once not, check different
        # 9. Check that second socket in request is same as first

        # 1.
        motor.socket_uuid = True
        cx = self.motor_connection(host, port, max_pool_size=17)
        cx_pool = cx.delegate._Connection__pool
        loop = ioloop.IOLoop.instance()

        # Connection has needed one socket so far to call isMaster
        self.assertEqual(1, len(cx_pool.sockets))
        self.assertFalse(cx_pool.in_request())

        def get_socket():
            # Weirdness in PyMongo, which I hope will be fixed soon: Connection
            # does pool.get_socket(pair), while ReplicaSetConnection initializes
            # pool with pair and just does pool.get_socket(). We're using
            # Connection so we have to emulate its call.
            return cx_pool.get_socket((host, port))

        get_socket = motor.asynchronize(get_socket, False, True)

        def socket_ids():
            return [sock_info.sock.uuid for sock_info in cx_pool.sockets]

        # We need a place to keep refs to sockets so they're not reclaimed
        # before we're ready
        socks = {}
        results = set()

        # 2.
        def got_sock0(sock, error):
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            socks[0] = sock
            if 1 in socks:
                # got_sock1 has also run; let this callback finish so its refs
                # are deleted
                loop.add_callback(check_socks_different_and_reclaimed0)

        def got_sock1(sock, error):
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            socks[1] = sock
            if 0 in socks:
                # got_sock0 has also run; let this callback finish so its refs
                # are deleted
                loop.add_callback(check_socks_different_and_reclaimed0)

        # 3.
        def check_socks_different_and_reclaimed0():
            self.assertNotEqual(socks[0], socks[1])
            id_0, id_1 = socks[0].sock.uuid, socks[1].sock.uuid
            del socks[0]
            del socks[1]
            self.assertTrue(id_0 in socket_ids())
            self.assertTrue(id_1 in socket_ids())

            results.add('step3')

            # 4.
            with cx.start_request():
                get_socket(callback=got_sock2)

            # 5.
            get_socket(callback=got_sock3)

        sock2_id = [None]
        def got_sock2(sock, error):
            # Get request socket
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            self.assertTrue(cx_pool.in_request())
            socks[2] = sock
            sock2_id[0] = sock.sock.uuid
            if 3 in socks:
                # got_sock3 has also run
                loop.add_callback(check_socks_different_and_reclaimed1)

            # We're in a request in this function, so test step 6, after
            # check_socks_different_and_reclaimed1 has run.
            loop.add_timeout(
                time.time() + 0.25,
                lambda: get_socket(callback=check_invalid_op))

        def got_sock3(sock, error):
            # Get NON-request socket
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            self.assertFalse(cx_pool.in_request())
            socks[3] = sock
            if 2 in socks:
                # got_sock2 has also run
                loop.add_callback(check_socks_different_and_reclaimed1)

        def check_socks_different_and_reclaimed1():
            self.assertNotEqual(socks[2], socks[3])
            id_2, id_3 = socks[2].sock.uuid, socks[3].sock.uuid
            del socks[2]
            del socks[3]

            # sock 2 is the request socket, it hasn't been reclaimed yet
            # because we still have check_invalid_op() pending in that request
            self.assertFalse(id_2 in socket_ids())

            # sock 3 is done and it's been reclaimed
            self.assertTrue(id_3 in socket_ids())
            results.add('step5')

        # 6.
        def check_invalid_op(result, error):
            self.assertEqual(None, result)
            self.assertTrue(isinstance(error, InvalidOperation))
            self.assertTrue(cx_pool.in_request())
            results.add('step6')

            # 7.
            self.assertFalse(sock2_id[0] in socket_ids())

            # Schedule a callback *not* in a request
            with stack_context.NullContext():
                loop.add_callback(check_request_sock_reclaimed)

        def check_request_sock_reclaimed():
            self.assertFalse(cx_pool.in_request())

            # TODO: I can't figure out how to force GC reliably here
#            self.assertTrue(
#                sock2_id[0] in socket_ids(),
#                "Request socket not reclaimed by pool")

            results.add('step7')

            # 8.
            get_socket(callback=got_sock4)

            with cx.start_request():
                get_socket(callback=got_sock5)

        def got_sock4(sock, error):
            self.assertFalse(cx_pool.in_request())
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            socks[4] = sock
            if 5 in socks:
                # got_sock5 has also run
                check_socks_different()

        sock5_id = [None]
        def got_sock5(sock, error):
            self.assertTrue(cx_pool.in_request())
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))

            socks[5] = sock
            sock5_id[0] = sock.sock.uuid

            if 4 in socks:
                # got_sock4 has also run
                check_socks_different()

            # 9.
            get_socket(callback=got_sock6)

        def check_socks_different():
            self.assertNotEqual(socks[4], socks[5])

            # sock 5 is the request socket
            id_4 = socks[4].sock.uuid
            sock5_id[0] = socks[5].sock.uuid

            del socks[4]
            del socks[5]

            # sock 4 is done and it's been reclaimed
            self.assertTrue(id_4 in socket_ids())

            # sock 5 still in request
            self.assertFalse(sock5_id[0] in socket_ids())
            results.add('step8')

        def got_sock6(sock, error):
            self.assertTrue(cx_pool.in_request())
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            self.assertEqual(sock5_id[0], sock.sock.uuid)
            results.add('step9')

        # Knock over the first domino.
        get_socket(callback=got_sock0)
        get_socket(callback=got_sock1)

        for step in (3, 5, 6, 7, 8, 9):
            self.assertEventuallyEqual(
                True, lambda: 'step%s' % step in results)

        loop.start()


if __name__ == '__main__':
    unittest.main()
