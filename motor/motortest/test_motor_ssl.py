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

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False

from tornado import ioloop
from nose.plugins.skip import SkipTest

import motor
from motor.motortest import (
    MotorTest, async_test_engine, host, port, AssertEqual)
from pymongo.errors import ConfigurationError


class MotorSSLTest(MotorTest):
    def test_no_ssl(self):
        if have_ssl:
            raise SkipTest(
                "We have SSL compiled into Python, can't test what happens "
                "without SSL"
            )

        self.assertRaises(ConfigurationError,
                          motor.MotorConnection, host, port, ssl=True)
        # TODO: test same thing with MotorReplicaSetConnection and MMSC
    #        self.assertRaises(ConfigurationError,
    #            ReplicaSetConnection, ssl=True)

    def test_simple_ops(self):
        # TODO: this is duplicative of test_nested_callbacks_2
        if not have_ssl:
            raise SkipTest()

        loop = ioloop.IOLoop.instance()
        cx = motor.MotorConnection(host, port, connectTimeoutMS=100, ssl=True)

        def connected(cx, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.insert({'ssl': True}, callback=inserted)

        def inserted(result, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.find_one(callback=found)

        def found(result, error):
            if error:
                raise error

            loop.stop()
            cx.drop_database('pymongo_ssl_test')

        cx.open(connected)
        loop.start()


if __name__ == '__main__':
    unittest.main()
