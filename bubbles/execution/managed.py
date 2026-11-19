# -*- coding: utf-8 -*-
"""Managed execution context for a data cleaning/loading run.

A :class:`ManagedExecution` groups the resources opened during one run –
sources (open files), SQL cursors/connectables and output targets – and owns
their lifecycle:

* objects returned by a builder keep working exactly as before and read
  ``rows``/``records`` lazily; nothing is materialized into a list;
* resources stay open until the execution is closed explicitly or leaves a
  ``with`` block, so data can be read or derived lazily after the builder
  function returns;
* on successful completion every output is flushed/finalized before success
  is reported;
* on failure or early close all *owned* resources are reclaimed. The
  processing error and any cleanup error are both visible – cleanup errors
  are wrapped in :class:`~bubbles.errors.ResourceCleanupError` with the
  original error kept as exception cause;
* borrowed files or connectables are never closed (only flushed when they
  are outputs);
* a shared store and objects derived from the same source are registered
  once and therefore released only once;
* once the execution is closed, every registered object – including
  previously created derived objects and loading paths – refuses further
  reads or loads with :class:`~bubbles.errors.ClosedObjectError`. Plain row
  values (lists/dicts) that were already taken out remain usable, as they do
  not hold any resource.
"""

from ..errors import (
    ArgumentError,
    ClosedObjectError,
    ResourceCleanupError,
)
from ..common import get_logger

__all__ = (
    "ManagedExecution",
    "managed",
)


class _OwnedResource(object):
    """An entry in the managed execution.

    * `key` – identity used for de-duplication (usually ``id(owner)``)
    * `finalizer` – called on the success path, flushes/finalizes an output
    * `releaser` – always called when the execution ends, releases the owned
      resource (must be idempotent)
    """

    def __init__(self, key, label, releaser, finalizer=None):
        self.key = key
        self.label = label
        self.releaser = releaser
        self.finalizer = finalizer
        self.finalized = False
        self.released = False


class ManagedExecution(object):
    """Owns the resources of one data cleaning/loading run.

    Typical use::

        with managed(CSVSource("in.csv")) as run:
            target = run.manage(CSVTarget("out.csv", fields=...))
            for row in source.rows():   # lazy, no full listing
                target.append(transform(row))
        # exiting the block flushes/closes the target and closes the source;
        # any further read raises ClosedObjectError.

    The same instance can also be driven manually with :meth:`close` (success)
    or :meth:`fail` (error path).
    """

    def __init__(self, name=None):
        self.name = name or "managed-execution-%d" % id(self)
        self.logger = get_logger()

        self._resources = []
        self._seen_keys = set()

        # Active SQLAlchemy result cursors handed out by managed SQL objects.
        self._cursors = set()

        self._closed = False

    @property
    def closed(self):
        """``True`` once the execution has finished; registered objects then
        refuse further reads and loads."""
        return self._closed

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def own_resource(self, owner, releaser, finalizer=None, label=None):
        """Registers a generic owned resource.

        * `owner` – any object whose identity de-duplicates the registration;
          registering the same owner twice is a no-op.
        * `releaser` – zero-argument callable that releases the resource; it
          should be idempotent.
        * `finalizer` – optional zero-argument callable used on the success
          path to flush/finalize an output before it is released.
        """
        key = id(owner)
        if key in self._seen_keys:
            return owner

        self._seen_keys.add(key)
        self._resources.append(
            _OwnedResource(key, label or str(owner), releaser, finalizer))
        return owner

    def manage(self, obj):
        """Puts a data object or SQL store under this execution's management.

        Known backend objects are wired with the appropriate flush/finalize
        and release callables. The object is returned unchanged and gets a
        back-reference to this execution so that its read/load paths are
        gated. Registering the same object, a shared store or several derived
        objects more than once releases each underlying resource only once.
        """
        # Local imports avoid loading backend modules (and sqlalchemy) when
        # the managed execution is used with other objects.
        from ..backends.text.objects import CSVSource, CSVTarget
        from ..backends.sql.objects import SQLDataObject, SQLDataStore

        if isinstance(obj, SQLDataStore):
            self.own_resource(obj, obj.close, label="sql-store:%s" % obj)
            return obj

        if isinstance(obj, SQLDataObject):
            # The store is shared by every derived object; it is registered
            # separately and de-duplicated, so it is closed only once.
            self.manage(obj.store)
            self.own_resource(
                obj,
                releaser=lambda: None,
                finalizer=obj.flush,
                label="sql-object:%s" % getattr(obj, "name", obj))
            obj._managed_execution = self
            return obj

        if isinstance(obj, CSVTarget):
            # finalize() flushes and closes owned handles, idempotently.
            self.own_resource(obj, obj.finalize, finalizer=obj.finalize,
                              label="csv-target:%s" % obj.resource)
            obj._managed_execution = self
            return obj

        if isinstance(obj, CSVSource):
            self.own_resource(obj, obj.release, label="csv-source:%s"
                              % obj.resource)
            obj._managed_execution = self
            return obj

        raise ArgumentError(
            "Don't know how to manage object of type %s. Use "
            "own_resource() with explicit releaser/finalizer callables."
            % type(obj).__name__)

    # ------------------------------------------------------------------
    # Cursor tracking (SQL objects)
    # ------------------------------------------------------------------

    def iter_cursor(self, cursor):
        """Yields rows of an open SQL result `cursor` and makes sure the
        cursor is closed when iteration ends or the execution is shut down.
        The cursor stays open while the consumer reads lazily. Reading once
        the execution is closed raises
        :class:`~bubbles.errors.ClosedObjectError`."""
        self._check_running()
        self._cursors.add(cursor)
        try:
            while True:
                # Check before every fetch so closing the execution while a
                # consumer is still iterating raises ClosedObjectError instead
                # of leaking the backend's closed-cursor error.
                self._check_running()
                try:
                    row = next(cursor)
                except StopIteration:
                    break
                yield row
        finally:
            self._cursors.discard(cursor)
            # Closing an already exhausted/closed result is harmless.
            try:
                cursor.close()
            except Exception as e:
                self.logger.debug("Error closing SQL cursor: %s" % e)

    def _check_running(self):
        if self._closed:
            raise ClosedObjectError(
                "Managed execution '%s' is already closed; the SQL result "
                "can no longer be read." % self.name)

    def _close_cursors(self, errors):
        for cursor in list(self._cursors):
            try:
                cursor.close()
            except Exception as e:
                errors.append(e)
                self.logger.warn("Error while closing SQL cursor: %s" % e)
        self._cursors.clear()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _finalize_outputs(self, errors):
        for resource in self._resources:
            if resource.finalized or resource.finalizer is None:
                continue
            try:
                resource.finalizer()
            except Exception as e:
                errors.append(e)
                self.logger.warn(
                    "Error while finalizing %s: %s"
                    % (resource.label, e))
            finally:
                resource.finalized = True

    def _release_resources(self, errors):
        # Release in reverse registration order so that outputs are released
        # before their backing sources/stores where possible.
        for resource in reversed(self._resources):
            if resource.released:
                continue
            try:
                resource.releaser()
            except Exception as e:
                errors.append(e)
                self.logger.warn(
                    "Error while releasing %s: %s"
                    % (resource.label, e))
            finally:
                resource.released = True

    def _finish(self, finalize, processing_error=None):
        """Common shutdown. `finalize` says whether output finalizers run
        (success path). Always marks the execution closed and, on any
        shutdown error, raises :class:`ResourceCleanupError`, keeping
        `processing_error` as the exception cause."""
        if self._closed:
            if processing_error is not None:
                raise processing_error
            return

        errors = []

        if finalize:
            self._finalize_outputs(errors)

        self._close_cursors(errors)
        self._release_resources(errors)

        # Gate every registered object before propagating anything, so no
        # further reads/loads can happen even when cleanup failed.
        self._closed = True

        if errors:
            cleanup_error = ResourceCleanupError(
                "Managed execution '%s' finished with %d resource "
                "error(s): %s"
                % (self.name, len(errors),
                   "; ".join(str(e) for e in errors)),
                errors=errors)
            if processing_error is not None:
                raise cleanup_error from processing_error
            raise cleanup_error

        if processing_error is not None:
            raise processing_error

    def close(self):
        """Success path: flush/finalize all outputs, then release every owned
        resource. Safe to call repeatedly. Raises
        :class:`ResourceCleanupError` if any finalization or release fails;
        all resources are reclaimed regardless."""
        self._finish(finalize=True)

    def fail(self, exc=None):
        """Failure/early-close path: release every owned resource without
        finalizing outputs. If `exc` is given it is re-raised; when cleanup
        errors also occur, a :class:`ResourceCleanupError` is raised with
        `exc` as its cause so both stay visible."""
        self._finish(finalize=False, processing_error=exc)

    # Context manager protocol ------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is None:
            # May raise ResourceCleanupError, which then propagates.
            self.close()
        else:
            try:
                self.fail(exc)
            except ResourceCleanupError as cleanup_error:
                # Re-raise explicitly so the processing error remains the
                # cause of the visible cleanup error.
                raise cleanup_error from exc
        # Never suppress the original exception.
        return False


def managed(*objects):
    """Convenience entry point creating a :class:`ManagedExecution` and
    managing the given source/store/target objects::

        with managed(source) as run:
            target = run.manage(CSVTarget(path, fields=fields))
            ...
    """
    execution = ManagedExecution()
    for obj in objects:
        execution.manage(obj)
    return execution
