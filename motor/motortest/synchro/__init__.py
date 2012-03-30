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

"""Synchro, a fake synchronous PyMongo implementation built on top of Motor,
for the sole purpose of checking that Motor passes the same unittests as
PyMongo.

DO NOT USE THIS MODULE.
"""

import functools
import inspect
import os
import sys
import time
import traceback
from tornado.ioloop import IOLoop

import motor
from pymongo import son_manipulator
from pymongo.errors import ConnectionFailure, TimeoutError, OperationFailure


# So that synchronous unittests can import these names from Synchro,
# thinking it's really pymongo
from pymongo import (
    ASCENDING, DESCENDING, GEO2D, GEOHAYSTACK, ReadPreference,
    ALL, helpers, OFF, SLOW_ONLY, pool
)

from pymongo.pool import NO_REQUEST, NO_SOCKET_YET, SocketInfo, Pool
from pymongo.replica_set_connection import _partition_node

__all__ = [
    'ASCENDING', 'DESCENDING', 'GEO2D', 'GEOHAYSTACK', 'ReadPreference',
    'Connection', 'ReplicaSetConnection', 'Database', 'Collection',
    'Cursor', 'ALL', 'helpers', 'OFF', 'SLOW_ONLY', '_partition_node',
    'NO_REQUEST', 'NO_SOCKET_YET', 'SocketInfo', 'pool', 'Pool',
    'MasterSlaveConnection',
]

timeout_sec = float(os.environ.get('TIMEOUT_SEC', 5))


# TODO: better name or iface, document
# TODO: maybe just get rid of this and put it all in synchronize()?
def loop_timeout(kallable, exc=None, seconds=timeout_sec, name="<anon>"):
    loop = IOLoop.instance()
    assert not loop.running(), "Loop already running in method %s" % name
    loop._callbacks[:] = []
    loop._timeouts[:] = []
    outcome = {}

    def raise_timeout_err():
	loop.stop()
	outcome['error'] = (exc or TimeoutError("timeout"))

    timeout = loop.add_timeout(time.time() + seconds, raise_timeout_err)

    def callback(result, error):
	try:
	    loop.stop()
	    loop.remove_timeout(timeout)
	    outcome['result'] = result
	    outcome['error'] = error
	except Exception:
	    traceback.print_exc(sys.stderr)
	    raise

	# Special case: return False to stop iteration in case this callback is
	# being used in Motor's find().each()
	return False

    kallable(callback=callback)
    try:
	loop.start()
	if outcome.get('error'):
	    raise outcome['error']

	return outcome['result']
    finally:
	if loop.running():
	    loop.stop()


class Sync(object):
    def __init__(self, name, has_safe_arg):
	self.name = name
	self.has_safe_arg = has_safe_arg

    def __get__(self, obj, objtype):
	async_method = getattr(obj.delegate, self.name)
	return synchronize(obj, async_method, has_safe_arg=self.has_safe_arg)


def synchronize(self, async_method, has_safe_arg):
    """
    @param self:                A Synchro object, e.g. synchro.Connection
    @param async_method:        Bound method of a MotorConnection,
				MotorDatabase, etc.
    @param has_safe_arg:        Whether the method takes a 'safe' argument
    @return:                    A synchronous wrapper around the method
    """
    assert isinstance(self, Synchro)

    @functools.wraps(async_method)
    def synchronized_method(*args, **kwargs):
	assert 'callback' not in kwargs

	try:
	    base_object_safe = self.delegate.safe
	except Exception:
	    # delegate not set yet, or no 'safe' attribute
	    base_object_safe = False

	# TODO: is this right? Maybe use get_last_error_options()
	safe_arg_passed = (
	    'safe' in kwargs or 'w' in kwargs or 'j' in kwargs
	    or 'wtimeout' in kwargs or base_object_safe
	)

	if not safe_arg_passed and has_safe_arg:
	    # By default, Motor passes safe=True if there's a callback, but
	    # we're emulating PyMongo, which defaults safe to False, so we
	    # explicitly override.
	    kwargs['safe'] = False

	rv = None
	try:
	    rv = loop_timeout(
		functools.partial(async_method, *args, **kwargs),
		name=async_method.func_name
	    )
	except OperationFailure:
	    # Ignore OperationFailure for unsafe writes; synchronous pymongo
	    # wouldn't have known the operation failed.
	    if safe_arg_passed or not has_safe_arg:
		raise

	return rv

    return synchronized_method


def unwrap_synchro(fn):
    """If first argument to decorated function is a Synchro object, pass the
    wrapped Motor object into the function.
    """
    @functools.wraps(fn)
    def _unwrap_synchro(self, obj, *args, **kwargs):
	if isinstance(obj, Synchro):
	    obj = obj.delegate
	return fn(self, obj, *args, **kwargs)
    return _unwrap_synchro


def wrap_synchro(fn):
    """If decorated Synchro function returns a Motor object, wrap in a Synchro
    object.
    """
    @functools.wraps(fn)
    def _wrap_synchro(*args, **kwargs):
	motor_obj = fn(*args, **kwargs)

	# Not all Motor classes appear here, only those we need to return
	# from methods like map_reduce() or create_collection()
	if isinstance(motor_obj, motor.MotorConnection):
	    connection = Connection()
	    connection.delegate = motor_obj
	    return connection
	elif isinstance(motor_obj, motor.MotorCollection):
	    connection = Connection(delegate=motor_obj.database.connection)
	    database = Database(connection, motor_obj.database.name)
	    return Collection(database, motor_obj.name)
	if isinstance(motor_obj, motor.MotorCursor):
	    return Cursor(motor_obj)
	else:
	    return motor_obj
    return _wrap_synchro


class WrapOutgoing(motor.DelegateProperty):
    def __get__(self, obj, objtype):
	# self.name is set by SynchroMeta
	motor_method = getattr(obj.delegate, self.name)
	def synchro_method(*args, **kwargs):
	    return wrap_synchro(motor_method)(*args, **kwargs)

	return synchro_method


class SynchronizeAndWrapOutgoing(motor.DelegateProperty):
    def __init__(self, has_safe_arg):
	self.has_safe_arg = has_safe_arg

    def __get__(self, obj, objtype):
	# self.name is set by SynchroMeta
	motor_method = getattr(obj.delegate, self.name)
	synchro_method = synchronize(obj, motor_method, self.has_safe_arg)
	return wrap_synchro(synchro_method)


class SynchroProperty(object):
    def __init__(self):
	self.name = None

    def __get__(self, obj, objtype):
	# self.name is set by SynchroMeta
	return getattr(obj.delegate.delegate, self.name)

    def __set__(self, obj, val):
	# self.name is set by SynchroMeta
	return setattr(obj.delegate.delegate, self.name, val)


class SynchroMeta(type):
    """This metaclass customizes creation of Synchro's Connection, Database,
    etc., classes:

    - All asynchronized methods of Motor classes, such as
      MotorDatabase.command(), are re-synchronized.

    - Properties delegated from Motor's classes to PyMongo's, such as ``name``
      or ``host``, are delegated **again** from Synchro's class to Motor's.

    - MotorCursor's methods which return a clone of the MotorCursor are wrapped
      so they return a Synchro Cursor.

    - Certain properties that are included only because PyMongo's unittests
      access them, such as _BaseObject__set_slave_okay, are simulated.
    """

    def __new__(cls, name, bases, attrs):
	# Create the class, e.g. the Synchro Connection or Database class
	new_class = type.__new__(cls, name, bases, attrs)

	# delegate_class is a Motor class like MotorConnection
	delegate_class = new_class.__delegate_class__

	if delegate_class:
	    delegated_attrs = {}

	    for klass in reversed(inspect.getmro(delegate_class)):
		delegated_attrs.update(klass.__dict__)

	    for attrname, delegate_attr in delegated_attrs.items():
		# If attrname is in attrs, it means Synchro has overridden
		# this attribute, e.g. Database.create_collection which is
		# special-cased. Ignore such attrs.
		if attrname not in attrs:
		    if isinstance(delegate_attr, motor.Async):
			# Re-synchronize the method
			sync_method = Sync(attrname, delegate_attr.has_safe_arg)
			setattr(new_class, attrname, sync_method)
		    elif isinstance(delegate_attr, motor.MotorCursorChainingMethod):
			# Wrap MotorCursors in Synchro Cursors
			wrapper = WrapOutgoing()
			wrapper.name = attrname
			setattr(new_class, attrname, wrapper)
		    elif isinstance(delegate_attr, motor.DelegateProperty):
			# Delegate the property from Synchro to Motor
			setattr(new_class, attrname, delegate_attr)

	# Set DelegateProperties' and SynchroProperties' names
	for name, attr in attrs.items():
	    if isinstance(attr, (motor.DelegateProperty, SynchroProperty)):
		attr.name = name

	return new_class


class Synchro(object):
    """
    Wraps a MotorConnection, MotorDatabase, or MotorCollection and
    makes it act like the synchronous pymongo equivalent
    """
    __metaclass__ = SynchroMeta
    __delegate_class__ = None

    def __cmp__(self, other):
	return cmp(self.delegate, other.delegate)

    _BaseObject__set_slave_okay = SynchroProperty()
    _BaseObject__set_safe       = SynchroProperty()


class Connection(Synchro):
    HOST = 'localhost'
    PORT = 27017

    __delegate_class__ = motor.MotorConnection

    def __init__(self, host=None, port=None, *args, **kwargs):
	# Motor doesn't implement auto_start_request
	kwargs.pop('auto_start_request', None)

	# So that TestConnection.test_constants and test_types work
	self.host = host if host is not None else self.HOST
	self.port = port if port is not None else self.PORT
	self.delegate = kwargs.pop('delegate', None)

	if not self.delegate:
	    self.delegate = self.__delegate_class__(
		self.host, self.port, *args, **kwargs
	    )
	    self.synchro_connect()

    def synchro_connect(self):
	# Try to connect the MotorConnection before continuing; raise
	# ConnectionFailure if it times out.
	sync_method = synchronize(self, self.delegate.open, has_safe_arg=False)
	sync_method()

    @unwrap_synchro
    def drop_database(self, name_or_database):
	synchronize(self, self.delegate.drop_database, has_safe_arg=False)(
	    name_or_database)

    def start_request(self):
	self.request = self.delegate.start_request()
	self.request.__enter__()

    def end_request(self):
	if self.request:
	    self.request.__exit__(None, None, None)

    @property
    def is_locked(self):
	return synchronize(self, self.delegate.is_locked, has_safe_arg=False)()

    def __enter__(self):
	return self

    def __exit__(self, *args):
	self.delegate.disconnect()

    def __getattr__(self, name):
	# If this is like connection.db, then wrap the outgoing object with
	# Synchro's Database
	return Database(self, name)

    __getitem__ = __getattr__

    _Connection__pool           = SynchroProperty()


class ReplicaSetConnection(Connection):
    __delegate_class__ = motor.MotorReplicaSetConnection

    def __init__(self, *args, **kwargs):
	# Motor doesn't implement auto_start_request
	kwargs.pop('auto_start_request', None)

	self.delegate = self.__delegate_class__(
	    *args, **kwargs
	)

	self.synchro_connect()


class MasterSlaveConnection(Connection):
    __delegate_class__ = motor.MotorMasterSlaveConnection

    def __init__(self, master, slaves, *args, **kwargs):
	# MotorMasterSlaveConnection expects MotorConnections or regular
	# pymongo Connections as arguments, rather than Synchro Connections
	if isinstance(master, Connection):
	    master = master.delegate

	slaves = [s.delegate if isinstance(s, Connection) else s
		  for s in slaves]

	self.delegate = self.__delegate_class__(
	    master, slaves, *args, **kwargs
	)

	self.synchro_connect()

    @property
    def master(self):
	synchro_master = Connection()
	synchro_master.delegate = self.delegate.master
	return synchro_master

    @property
    def slaves(self):
	synchro_slaves = []
	for s in self.delegate.slaves:
	    synchro_slave = Connection()
	    synchro_slave.delegate = s
	    synchro_slaves.append(synchro_slave)

	return synchro_slaves


class Database(Synchro):
    __delegate_class__ = motor.MotorDatabase

    def __init__(self, connection, name):
	assert isinstance(connection, Connection), (
	    "Expected Connection, got %s" % repr(connection)
	)
	self.connection = connection

	self.delegate = connection.delegate[name]
	assert isinstance(self.delegate, motor.MotorDatabase)

    def add_son_manipulator(self, manipulator):
	if isinstance(manipulator, son_manipulator.AutoReference):
	    db = manipulator.database
	    if isinstance(db, Database):
		manipulator.database = db.delegate.delegate

	self.delegate.add_son_manipulator(manipulator)

    @unwrap_synchro
    def drop_collection(self, name_or_collection):
	sync_method = synchronize(
	    self, self.delegate.drop_collection, has_safe_arg=False)
	return sync_method(name_or_collection)

    @unwrap_synchro
    def validate_collection(self, name_or_collection, *args, **kwargs):
	sync_method = synchronize(
	    self, self.delegate.validate_collection, has_safe_arg=False)
	return sync_method(name_or_collection, *args, **kwargs)

    @wrap_synchro
    def create_collection(self, *args, **kwargs):
	sync_method = synchronize(
	    self, self.delegate.create_collection, has_safe_arg=False)

	return sync_method(*args, **kwargs)

    def __getattr__(self, name):
	return Collection(self, name)

    __getitem__ = __getattr__


class Collection(Synchro):
    __delegate_class__ = motor.MotorCollection

    def __init__(self, database, name):
	assert isinstance(database, Database)
	self.database = database

	self.delegate = database.delegate[name]
	assert isinstance(self.delegate, motor.MotorCollection)

    find       = WrapOutgoing()
    map_reduce = SynchronizeAndWrapOutgoing(has_safe_arg=False)

    def __getattr__(self, name):
	# Access to collections with dotted names, like db.test.mike
	return Collection(self.database, self.name + '.' + name)

    __getitem__ = __getattr__


class Cursor(Synchro):
    __delegate_class__ = motor.MotorCursor

    close                      = motor.ReadOnlyDelegateProperty()
    rewind                     = WrapOutgoing()
    clone                      = WrapOutgoing()
    where                      = WrapOutgoing()
    sort                       = WrapOutgoing()
    explain                    = SynchronizeAndWrapOutgoing(has_safe_arg=False)

    def __init__(self, motor_cursor):
	self.delegate = motor_cursor

    def __iter__(self):
	return self

    def next(self):
	sync_next = synchronize(self, self.delegate.each, has_safe_arg=False)
	rv = sync_next()
	if rv is not None:
	    return rv
	else:
	    raise StopIteration

    def __getitem__(self, index):
	if isinstance(index, slice):
	    return Cursor(self.delegate[index])
	else:
	    sync_next = synchronize(
		self, self.delegate[index].each, has_safe_arg=False)
	    return sync_next()

    _Cursor__query_options     = SynchroProperty()
    _Cursor__retrieved         = SynchroProperty()
    _Cursor__skip              = SynchroProperty()
    _Cursor__limit             = SynchroProperty()
    _Cursor__timeout           = SynchroProperty()
    _Cursor__snapshot          = SynchroProperty()
    _Cursor__tailable          = SynchroProperty()
    _Cursor__as_class          = SynchroProperty()
    _Cursor__slave_okay        = SynchroProperty()
    _Cursor__await_data        = SynchroProperty()
    _Cursor__partial           = SynchroProperty()
    _Cursor__manipulate        = SynchroProperty()
    _Cursor__query_flags       = SynchroProperty()
    _Cursor__connection_id     = SynchroProperty()
    _Cursor__read_preference   = SynchroProperty()