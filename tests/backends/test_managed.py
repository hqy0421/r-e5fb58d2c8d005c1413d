import io
import os
import tempfile
import unittest

from ..common import data_path

from bubbles import FieldList
from bubbles.errors import (
    ClosedObjectError,
    ResourceCleanupError,
    BubblesError,
)
from bubbles.execution import ManagedExecution, managed
from bubbles.backends.text.objects import CSVSource, CSVTarget
from bubbles.backends.sql.objects import SQLDataStore


def _csv_target_path():
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    return path


class ManagedCSVTestCase(unittest.TestCase):
    def setUp(self):
        self.fields = FieldList(
            ("id", "string"), ("fruit", "string"), ("type", "string"))

    def test_lazy_read_and_close_gates(self):
        source = CSVSource(data_path("fruits-sk.csv"))
        with managed(source) as run:
            self.assertFalse(run.closed)
            iterator = source.rows()          # nothing materialized yet
            first = next(iterator)
            self.assertEqual(["1", "jablko", "malvice"], first)

        self.assertTrue(run.closed)

        # Already taken plain row values stay usable.
        self.assertEqual("jablko", first[1])

        # Reads after close are refused on every representation.
        with self.assertRaises(ClosedObjectError):
            list(source.rows())
        with self.assertRaises(ClosedObjectError):
            list(source.records())
        with self.assertRaises(ClosedObjectError):
            source.csv_stream()

        # Owned file handle is actually closed.
        self.assertTrue(source.handle.closed)

    def test_records_and_mid_iteration_gate(self):
        source = CSVSource(data_path("fruits-sk.csv"))
        run = ManagedExecution()
        run.manage(source)

        rows = source.records()
        next(rows)
        run.close()

        with self.assertRaises(ClosedObjectError):
            list(rows)

    def test_success_flushes_and_closes_target(self):
        path = _csv_target_path()
        try:
            target = CSVTarget(path, fields=self.fields)
            with managed(target):
                target.append(["1", "jablko", "malvice"])
                # Not flushed to disk yet beyond the buffered writer.
                target.flush()
                self.assertFalse(target.handle.closed)

            self.assertTrue(target.handle.closed)

            # Output is finalized and re-readable.
            with open(path, encoding="utf-8") as f:
                content = f.read()
            self.assertIn("id,fruit,type", content)
            self.assertIn("jablko", content)

            # Loading path is closed as well.
            with self.assertRaises(ClosedObjectError):
                target.append(["2", "hruška", "malvice"])
        finally:
            os.remove(path)

    def test_failure_releases_without_losing_error(self):
        source = CSVSource(data_path("fruits-sk.csv"))
        path = _csv_target_path()
        target = CSVTarget(path, fields=self.fields)
        try:
            boom = ValueError("processing failed")
            with self.assertRaises(ValueError) as caught:
                with managed(source, target) as run:
                    target.append(["1", "a", "b"])
                    raise boom

            self.assertIs(caught.exception, boom)
            self.assertTrue(run.closed)
            self.assertTrue(source.handle.closed)
            self.assertTrue(target.handle.closed)
        finally:
            os.remove(path)

    def test_borrowed_handle_is_not_closed(self):
        stream = io.StringIO("id,fruit,type\n1,jablko,malvice\n")
        source = CSVSource(stream)
        with managed(source):
            rows = list(source.rows())
            self.assertEqual(1, len(rows))

        self.assertFalse(stream.closed)
        stream.close()

    def test_borrowed_target_handle_is_not_closed(self):
        stream = io.StringIO()
        target = CSVTarget(stream, fields=self.fields,
                           write_headers=True)
        with managed(target):
            target.append(["1", "jablko", "malvice"])

        self.assertFalse(stream.closed)
        # The borrowed handle still contains the flushed output.
        self.assertIn("jablko", stream.getvalue())
        stream.close()

    def test_cleanup_error_is_visible(self):
        source = CSVSource(data_path("fruits-sk.csv"))
        run = ManagedExecution()

        def bad_releaser():
            raise OSError("cannot release")

        run.manage(source)
        run.own_resource(object(), bad_releaser, label="broken-resource")

        with self.assertRaises(ResourceCleanupError) as caught:
            run.close()

        self.assertTrue(run.closed)
        self.assertEqual(1, len(caught.exception.errors))
        self.assertIn("cannot release", str(caught.exception.errors[0]))
        # The source was still reclaimed despite the failing sibling.
        self.assertTrue(source.handle.closed)

    def test_processing_and_cleanup_errors_both_visible(self):
        source = CSVSource(data_path("fruits-sk.csv"))
        run = ManagedExecution()

        def bad_releaser():
            raise OSError("cleanup boom")

        run.own_resource(object(), bad_releaser, label="broken-resource")

        processing = RuntimeError("processing boom")
        try:
            run.fail(processing)
        except ResourceCleanupError as e:
            self.assertIs(e.__cause__, processing)
            self.assertEqual(1, len(e.errors))
        else:
            self.fail("ResourceCleanupError was not raised")

        self.assertTrue(run.closed)

    def test_repeated_close_is_idempotent(self):
        source = CSVSource(data_path("fruits-sk.csv"))
        run = ManagedExecution()
        run.manage(source)
        run.close()
        run.close()
        run.fail()
        self.assertTrue(run.closed)


class ManagedSQLTestCase(unittest.TestCase):
    def setUp(self):
        self.store = SQLDataStore('sqlite:///')
        self.fields = FieldList(
            ('a', 'integer'), ('b', 'integer'), ('c', 'integer'))
        self.table = self.store.create('test', self.fields, replace=True)
        self.table.append_from_iterable(
            [(1, 2, 3), (1, 2, 4), (1, 3, 5)])

    def test_lazy_rows_derivation_and_gate(self):
        with managed(self.store, self.table) as run:
            # Native SQL derivation still works after the builder "returns".
            derived = self.table.clone_statement(
                statement=self.table.table.select().limit(2))
            iterator = derived.rows()                 # lazy
            first = next(iterator)
            self.assertEqual((1, 2, 3), tuple(first))
            self.assertFalse(run.closed)

        self.assertTrue(run.closed)

        # Plain row value already fetched stays usable.
        self.assertEqual(1, first[0])

        # Existing source and derived object refuse further reads.
        with self.assertRaises(ClosedObjectError):
            list(self.table.rows())
        with self.assertRaises(ClosedObjectError):
            list(derived.rows())
        with self.assertRaises(ClosedObjectError):
            len(derived)
        # SQL derivation path is gated too.
        with self.assertRaises(ClosedObjectError):
            derived.selectable()
        # Loading path is gated.
        with self.assertRaises(ClosedObjectError):
            self.table.append((7, 8, 9))
        with self.assertRaises(ClosedObjectError):
            self.table.flush()
        with self.assertRaises(ClosedObjectError):
            self.table.append_from_iterable([(7, 8, 9)])

    def test_cursor_closed_mid_iteration(self):
        run = ManagedExecution()
        run.manage(self.store)
        run.manage(self.table)

        iterator = self.table.rows()
        next(iterator)
        run.close()

        with self.assertRaises(ClosedObjectError):
            list(iterator)

    def test_no_full_listing_until_consumer_drives(self):
        run = ManagedExecution()
        run.manage(self.store)
        run.manage(self.table)

        # rows() returns a generator without touching the result set eagerly.
        iterator = self.table.rows()
        import types
        self.assertIsInstance(iterator, types.GeneratorType)
        self.assertEqual(3, len(list(iterator)))
        run.close()

    def test_borrowed_connectable_not_disposed(self):
        engine = self.store.connectable
        # A store built around a borrowed connectable must not dispose it.
        borrowed_store = SQLDataStore(connectable=engine)
        table = borrowed_store.create('borrowed', self.fields, replace=True)
        table.append_from_iterable([(1, 1, 1)])

        with managed(borrowed_store, table):
            self.assertEqual(1, len(list(table.rows())))

        # Engine still works after the managed run.
        self.assertEqual(
            1, engine.scalar(borrowed_store.table('borrowed').count()))

    def test_shared_store_registered_once(self):
        run = ManagedExecution()
        run.manage(self.table)
        derived = self.table.clone_statement(
            statement=self.table.table.select())
        run.manage(derived)
        run.manage(self.store)

        store_entries = [r for r in run._resources
                         if r.key == id(self.store)]
        self.assertEqual(1, len(store_entries))
        run.close()

    def test_buffered_insert_flushed_on_success(self):
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        url = "sqlite:///%s" % db_path
        target_store = SQLDataStore(url)
        target = target_store.create('loaded', self.fields, replace=True)
        try:
            with managed(target_store, target):
                target.append((9, 9, 9))
                # Buffered, not yet inserted.
                self.assertEqual(1, len(target._insert_buffer))
            # Buffer flushed, run closed and the owned engine disposed.
            self.assertEqual(0, len(target._insert_buffer))
            self.assertTrue(target_store._closed)

            # Data persisted in the on-disk database and can be reopened.
            reopen = SQLDataStore(url)
            self.assertEqual(
                1, reopen.connectable.scalar(reopen.table('loaded').count()))
            reopen.close()
        finally:
            os.remove(db_path)


class ManagedRejectsUnknownObjectsTestCase(unittest.TestCase):
    def test_unknown_object_raises(self):
        run = ManagedExecution()
        with self.assertRaises(BubblesError):
            run.manage(object())


if __name__ == "__main__":
    unittest.main()
