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

import motor
from motor.motortest import MotorTest, async_test_engine, host, port
from pymongo.son_manipulator import AutoReference, NamespaceInjector

# TODO: test that db = MotorDatabase(conn, 'test') works

class MotorDatabaseTest(MotorTest):
    def test_database_callbacks(self):
	db = self.motor_connection(host, port).test
	self.check_optional_callback(db.drop_collection, "collection")
	self.check_optional_callback(db.create_collection, "collection")
	self.check_required_callback(db.validate_collection, "collection")

    @async_test_engine()
    def test_command(self):
	cx = self.motor_connection(host, port)
	result = yield motor.Op(cx.admin.command, "buildinfo")
	self.assertEqual(int, type(result['bits']))

    def test_command_callback(self):
	cx = self.motor_connection(host, port)
	self.check_optional_callback(cx.admin.command, 'buildinfo', check=False)

    @async_test_engine()
    def test_auto_ref_and_deref(self):
	# Test same functionality as in PyMongo's test_database.py; the
	# implementation for Motor for async is a little complex so we test
	# that it works here, and we don't just rely on synchrotest
	# to cover it.
	cx = self.motor_connection(host, port)
	db = cx.test

	# We test a special hack where add_son_manipulator corrects our mistake
	# if we pass a MotorDatabase, instead of Database, to AutoReference.
	db.add_son_manipulator(AutoReference(db))
	db.add_son_manipulator(NamespaceInjector())

	a = {"hello": u"world"}
	b = {"test": a}
	c = {"another test": b}

	yield motor.Op(db.a.remove, {})
	yield motor.Op(db.b.remove, {})
	yield motor.Op(db.c.remove, {})
	yield motor.Op(db.a.save, a)
	yield motor.Op(db.b.save, b)
	yield motor.Op(db.c.save, c)
	a["hello"] = "mike"
	yield motor.Op(db.a.save, a)
	result_a = yield motor.Op(db.a.find_one)
	result_b = yield motor.Op(db.b.find_one)
	result_c = yield motor.Op(db.c.find_one)

	self.assertEqual(a, result_a)
	self.assertEqual(a, result_b["test"])
	self.assertEqual(a, result_c["another test"]["test"])
	self.assertEqual(b, result_b)
	self.assertEqual(b, result_c["another test"])
	self.assertEqual(c, result_c)


if __name__ == '__main__':
    unittest.main()
