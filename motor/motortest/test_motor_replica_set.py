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

import functools
import socket
import unittest

from tornado import ioloop, iostream

import motor
from motor.motortest import (
    MotorTest, async_test_engine, host, port, AssertEqual, AssertRaises)
import pymongo.errors


class MotorReplicaSetTest(MotorTest):
    @async_test_engine()
    def test_auto_reconnect_exception_when_read_preference_is_secondary(self):
        # TODO: find out the repl set name and so on the same way PyMongo's RSC
        # test does
        cx = motor.MotorReplicaSetConnection(
            'localhost:27017', replicaSet='repl0')

        yield motor.Op(cx.open)
        db = cx.pymongo_test

        def raise_socket_error(self, data, callback):
            ioloop.IOLoop.instance().add_callback(
                functools.partial(callback, None, socket.error('foo')))

        old_write = iostream.IOStream.write
        iostream.IOStream.write = raise_socket_error

        try:
            cursor = db.test.find(
                read_preference=pymongo.ReadPreference.SECONDARY)

            yield AssertRaises(pymongo.errors.AutoReconnect, cursor.each)
        finally:
            iostream.IOStream.write = old_write


if __name__ == '__main__':
    unittest.main()
