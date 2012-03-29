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

"""Test Motor by testing that Synchro, a fake PyMongo implementation built on
top of Motor, passes the same unittests as PyMongo.

This program monkey-patches sys.modules, so run it alone, rather than as part
of a larger test suite.

The environment variable TIMEOUT_SEC controls how long Synchro waits for each
Motor operation to complete, default 5 seconds.
"""

import sys
from os import path

import nose
from nose.config import Config
from nose.plugins import Plugin
from nose.plugins.manager import PluginManager
from nose.selector import Selector

from motor.motortest import synchro
from motor.motortest.puritanical import PuritanicalIOLoop


# TODO: running this file without a test-module name does nothing; it should run
#   all PyMongo tests except test_gevent
# TODO: test for Motor all the things test_pooling tests

excluded_modules = [
    'test.test_gevent',
    'test.test_threads',
    'test.test_threads_replica_set_connection',
    'test.test_pooling',
    'test.test_paired',

    # TODO:
    'test.test_ssl',
]

excluded_tests = [
    # TODO: For each of these, examine why the Synchro test fails and either
    # fix the Synchro test or test the same functionality directly in Motor,
    # or document that Motor doesn't support the functionality

    # Motor's requests can't be simulated in Synchro, so the request-
    # handling part of test_copy_db is testing against Motor directly.
    'TestConnection.test_copy_db',

    # TODO: test the following in Motor if we haven't already ------>
    'TestMasterSlaveConnection.test_disconnect',
    'TestMasterSlaveConnection.test_raise_autoreconnect_if_all_slaves_fail',
    'TestDatabase.test_authenticate_and_request',
    'TestMasterSlaveConnection.test_insert_find_one_in_request',
    # <------------------- END TODO

    # This test requires a lot of PyMongo-specific monkey-patching, we're
    # not going to test this in Motor because it uses the same logic under the
    # hood and can be assumed to work.
    'TestMasterSlaveConnection.test_continue_until_slave_works',

    # Motor's reprs aren't the same as PyMongo's
    '*.test_repr',

    # Motor doesn't do auto_start_request at all
    'TestConnection.test_auto_start_request',
    'TestConnection.test_contextlib_auto_start_request',

    # Motor's requests can't be simulated in Synchro, so we test them
    # directly
    'TestConnection.test_with_start_request',

    # test_replica_set_connection: We test this directly, because it requires
    # monkey-patching either socket or IOStream, depending on whether it's
    # PyMongo or Motor
    'TestConnection.test_auto_reconnect_exception_when_read_preference_is_secondary',

    # Motor doesn't support forking or threading
    'TestConnection.test_fork',
    'TestConnection.test_interrupt_signal',
    'TestCollection.test_ensure_unique_index_threaded',
    'TestGridfs.test_threaded_writes',
    'TestGridfs.test_threaded_reads',

    # Motor doesn't support PyMongo's syntax, db.system.js['my_func'] = "code",
    # users should just use system.js as a regular collection
    'TestDatabase.test_system_js',
    'TestDatabase.test_system_js_list',

    # Motor's cursors don't have a 'collection' attribute
    'TestCursor.test_properties',

    # Motor can't raise an index error if a cursor slice is out of range; it
    # just gets no results
    'TestCursor.test_getitem_index_out_of_range',

    # Motor's tailing works differently
    'TestCursor.test_tailable',
]


class SynchroNosePlugin(Plugin):
    name = 'synchro'

    def __init__(self, *args, **kwargs):
	# We need a standard Nose selector in order to filter out methods that
	# don't match TestSuite.test_*
	self.selector = Selector(config=None)
	super(SynchroNosePlugin, self).__init__(*args, **kwargs)

    def configure(self, options, conf):
	super(SynchroNosePlugin, self).configure(options, conf)
	# TODO: Figure out less hacky way to enable this plugin
	# programmatically with Nose
	self.enabled = True

    def wantModule(self, module):
	return module.__name__ not in excluded_modules

    def wantMethod(self, method):
	# Run standard Nose checks on name, like "does it start with test_"?
	if not self.selector.matches(method.__name__):
	    return False

	for excluded_name in excluded_tests:
	    suite_name, method_name = excluded_name.split('.')
	    if ((method.im_class.__name__ == suite_name or suite_name == '*')
		and method.__name__ == method_name
	    ):
		return False

	return True


if __name__ == '__main__':
    PuritanicalIOLoop().install()

    # Monkey-patch all pymongo's unittests so they think Synchro is the
    # real PyMongo
    # TODO: try using 'from imp import new_module' instead of this?
    sys.modules['pymongo'] = synchro

    for submod in [
	'connection',
	'collection',
	'master_slave_connection',
	'replica_set_connection',
	'database',
	'pool',
    ]:
	# So that 'from pymongo.connection import Connection' gets the Synchro
	# Connection, not the real one.
	sys.modules['pymongo.%s' % submod] = synchro

    # Find our directory
    this_dir = path.dirname(__file__)

    # Find test dir
    test_dir = path.normpath(path.join(this_dir, '../../../test'))
    print 'Running tests in %s' % test_dir

    # Exclude a test that hangs and prevents the run from completing - we should
    # fix the test for async, eventually
    # TODO: fix these, or implement a Motor-specific test that exercises the
    #   same features as each of these
    # TODO: document these variations and omissions b/w PyMongo and the Motor API
    print 'WARNING: excluding some tests -- go in and fix them for async!'

    config = Config(
	plugins=PluginManager(),
    )

    nose.main(
	config=config,
	addplugins=[SynchroNosePlugin()],
	defaultTest=test_dir,
    )
