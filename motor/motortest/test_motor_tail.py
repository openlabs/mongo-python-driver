import functools
import threading
import time

from tornado import ioloop, gen

import motor
from motor.motortest import (
    MotorTest, async_test_engine, host, port, AssertRaises)
from pymongo.errors import OperationFailure


class MotorTailTest(MotorTest):
    def setUp(self):
        super(MotorTailTest, self).setUp()
        self.sync_db.capped.drop()
        self.sync_db.create_collection('capped', capped=True, size=1000)

        self.sync_db.uncapped.drop()
        self.sync_db.uncapped.insert({}, safe=True)

        test_db = self.motor_connection(host, port).test
        self.capped = test_db.capped
        self.uncapped = test_db.uncapped

    def start_insertion_thread(self, collection_name, pauses):
        """A thread that gradually inserts documents into a capped collection
        """
        def add_docs():
            i = 0
            for pause in pauses:
                if pause == 'drop':
                    self.sync_db.capped.drop()
                else:
                    time.sleep(pause)
                    self.sync_db[collection_name].insert({'_id': i}, safe=True)
                    i += 1

        t = threading.Thread(target=add_docs)
        t.start()
        return t

    def each(self, results, n_expected, result, error):
        if error:
            results.append(type(error))
        elif result:
            results.append(result)

        if len(results) == n_expected:
            # Cancel iteration
            results.append('cancelled')
            return False

    def _test_tail(self, await_data):
        pauses = (1, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0.1, 0.1, 0, 0)

        t = self.start_insertion_thread('capped', pauses)

        results = []
        each = functools.partial(self.each, results, len(pauses))

        # Note we do *not* pass tailable or await_data to find(), the
        # convenience method handles it for us.
        self.capped.find().tail(each, await_data=await_data)

        self.assertEventuallyEqual(
            results,
            lambda: [{'_id': i} for i in range(len(pauses))] + ['cancelled'],
            timeout_sec=sum(pauses) + 1
        )

        ioloop.IOLoop.instance().start()
        t.join()
        self.wait_for_cursors()

    def test_tail_await(self):
        self._test_tail(True)

    def test_tail_no_await(self):
        self._test_tail(False)

    def _test_tail_drop_collection(self, await_data):
        # Ensure tail() throws error when its collection is dropped
        pauses = (0, 0, 1, 'drop', 1, 0, 0)
        t = self.start_insertion_thread('capped', pauses)

        results = []
        each = functools.partial(self.each, results, len(pauses) - 1)
        self.capped.find().tail(each, await_data=await_data)

        self.assertEventuallyEqual(
            True,
            lambda: (
                results[-1] is OperationFailure and len(results) <= 4
                and 'cancelled' not in results
            )
        )

        ioloop.IOLoop.instance().start()
        t.join()
        self.wait_for_cursors()

    def test_tail_drop_collection_await(self):
        self._test_tail_drop_collection(True)

    def test_tail_drop_collection_no_await(self):
        self._test_tail_drop_collection(False)

    @async_test_engine()
    def test_tail_uncapped_collection(self):
        yield AssertRaises(
            OperationFailure,
            self.uncapped.find().tail, await_data=True)

        yield AssertRaises(
            OperationFailure,
            self.uncapped.find().tail, await_data=False)

    def test_tail_nonempty_collection(self):
        self.sync_db.capped.insert([{'_id': -2}, {'_id': -1}], safe=True)

        pauses = (0, 0, 1, 0, 0)
        t = self.start_insertion_thread('capped', pauses)

        results = []
        each = functools.partial(self.each, results, len(pauses) + 2)
        self.capped.find().tail(each, await_data=False)

        self.assertEventuallyEqual(
            [{'_id': i} for i in range(-2, len(pauses))] + ['cancelled'],
            lambda: results,
            timeout_sec=sum(pauses) + 1
        )

        ioloop.IOLoop.instance().start()
        t.join()
        self.wait_for_cursors()

    @async_test_engine()
    def test_tail_gen(self):
        pauses = (1, 0.5, 1, 0, 0)
        t = self.start_insertion_thread('capped', pauses)

        loop = ioloop.IOLoop.instance()
        results = []
        cursor = self.capped.find(tailable=True, await_data=False)
        while len(results) < len(pauses):
            if not cursor.alive:
                # While collection is empty, tailable cursor dies immediately
                yield gen.Task(loop.add_timeout, time.time() + 0.1)
                cursor = self.capped.find(tailable=True)

            result = yield motor.Op(cursor.next)
            if result:
                results.append(result)
            else:
                yield gen.Task(loop.add_timeout, time.time() + 0.1)

        cursor.close()
        t.join()
        self.assertEqual([{'_id': i} for i in range(len(pauses))], results)
        self.wait_for_cursors()
