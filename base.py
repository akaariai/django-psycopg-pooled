"""
This module provides pooled psycopg2 connections for Django.

As the base of the implementation we have a "pool of pools", named
simply pools. The pools is a dictionary of connection tuple (that is
user_name, db_name, host, port) to a ConnectionPool. That is, we have
different pools for different connection strings. Each pool contains
potentially unlimited amount of connections. The connections are kept
open until they have been unused for 30 seconds. The clean interval
is also 30 seconds, so the maximum amount of time an unused connection
is kept hanging around is 60 seconds.

The ConnectionPool object contains the pool (a simple list of
WrappedConnections). The WrappedConnections in turn know if they are in
use currently, when they were last released into the pool, and they know
how to return self back to the pool when closed.

In addition the ConnectionPool has a couple of methods to get a connection
from the pool, release a connection back to the pool, and finally a method
for cleaning up the pool.

Finally we have a single lock to guard against all the possible concurrency
problems. Whenever altering the main pools dictionary, or altering any of
the ConnectionPools contained in the pools object, the lock must be held.
"""

# TODO: use weakref for connection -> pool references.
# If possible, it would be nice to get rid of that two-directional reference
# chain, but I don't know how. Cursor needs to know about the connection, so
# that it can communicate state information, and connection needs to know the
# pool, as it needs to tell the pool to release the connection.
#
# One idea is to track the connection -> pool with identifiers and then
# connection could find out its pool by the id.

# Get rid of connection_state. It doesn't actually do anything useful, it is
# just one step more in the chain...

import logging
import sys
import time
import traceback
from datetime import datetime, timedelta
from threading import Thread, Lock

from .helpers import tablify
from django.db import utils
from django.db.backends.postgresql_psycopg2.base import Database
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as DjangoDatabaseWrapper
from django.conf import settings

logger = logging.getLogger("psycopg_pooled")

DEFAULT_SETTINGS = {
    # Time between checking for abandoned and long-unused connections
    'CLEANING_INTERVAL': 5,
    # How long a connection can be unused in the pool before it is closed
    # The maximum time a connection can hang around is CLEANING_INTERVAL + 
    # IDLE_INTERVAL.
    'IDLE_INTERVAL': 20,
    'CONNECTION_RESET_SQL': """
          ABORT; -- Stop doing whatever the connection might be doing
          SET SESSION AUTHORIZATION DEFAULT; -- remove name information...
          RESET ALL; -- reset SET TIME ZONE etc
          CLOSE ALL; -- close cursors
          SELECT pg_advisory_unlock_all(); -- as the name says
          DISCARD TEMP; -- remove temporary tables
    """,
    # How often should we recycle a connection. After this interval,
    # a connection that is returned to the pool will be abandoned.
    'RECYCLE_INTERVAL': 100,
    'MAX_CONNECTIONS': 20, # max non-abandoned connections one pool can have
    # Should the pool try to check for LIMIT / OFFSET conditions in the
    # end of the query and make those part of the prepared plan?
    'PREPARE_REWRITE_LIMITS': False,
    'TRACE_IDLE_IN_TX': 3,
}

def get_setting(name):
    return getattr(settings, 'POOL_' + name, DEFAULT_SETTINGS[name])

# Some settings
CLEANING_INTERVAL = get_setting('CLEANING_INTERVAL')
IDLE_INTERVAL = get_setting('IDLE_INTERVAL')
RESET_QUERY = get_setting('CONNECTION_RESET_SQL')
RECYCLE_INTERVAL = get_setting('RECYCLE_INTERVAL')
MAX_CONNECTIONS = get_setting('MAX_CONNECTIONS')
PREPARE_REWRITE_LIMITS = get_setting('PREPARE_REWRITE_LIMITS')
TRACE_IDLE_IN_TX = get_setting('TRACE_IDLE_IN_TX')

# And then some constants
PREPARED_PREFIX = '_psycopg_pool_prepared'
LOG_QUERY_COUNTS = False
queries = {}
total_count = 0

class PoolOverflowException(Exception):
    pass

def _get_conn_key(conn_kwargs):
     if 'NAME' in conn_kwargs:
         lower_keys = dict([(k.lower(), v) for k, v in conn_kwargs.items()])
         lower_keys['database'] = lower_keys['name']
     else:
         lower_keys = conn_kwargs
     return (lower_keys['user'], lower_keys['database'],
             lower_keys.get('host', ''), lower_keys.get('port', ''))


class CursorWrapper(object):
    database = Database
    """
    A thin wrapper around psycopg2's normal cursor class so that we can catch
    particular exception instances and reraise them with the right types.

    This is like Django's CursorWrapper, but in addition we check for DROP
    DATABASE commands, in which case we must close all connections to prevent
    an error after test running.

    Subclasses can rewrite queries by using the _rewrite_queries hook.
    """

    def __init__(self, cursor, connection_state):
        self.connection_state = connection_state
        self.cursor = cursor

    def execute(self, query, args=None):
        # A hack to make sure there are no open connections to the
        # database when dropping it.
        if query.startswith("DROP DATABASE"):
            for pool in DatabaseWrapper.pools.values():
                pool.clean(timeout=0)
        if LOG_QUERY_COUNTS:
            global total_count
            if total_count % 100 == 0:
                querybuf = [(v, k) for k, v in queries.items()]
                querybuf.sort()
                querybuf.reverse()
                lines = querybuf[0:100]
                lines.reverse()
                for v, k in lines:
                    print v, k
            total_count += 1
            if query not in queries:
                queries[query] = 0
            queries[query] += 1
        self.connection_state.last_used = datetime.now()
        self.connection_state.query_count += 1
        query, args = self._rewrite_query(query, args)
        """
        logger.debug("executing %s with args %s" % (query, args))
        try:
            self.cursor.execute("EXPLAIN " + query, args)
            print self.cursor.fetchall()
        except Exception:
            logger.exception('Shit happened...')
        """
        try:
            return self.cursor.execute(query, args)
        except self.database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except self.database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def _rewrite_query(self, query, args):
        prepared_name, prep_query, args_count = \
            self.connection_state.prepare_targets.get(query, (None, None, None))
        if prepared_name is not None:
            assert args_count == len(args)
            if prepared_name not in self.connection_state.prepared_queries:
                if args_count > 0:
                    # (unknown, unknown, ...)
                    prep_args = ', '.join(['unknown'] * args_count)
                    prep_args = '(' + prep_args + ')'
                else:
                    prep_args = ''
                self.cursor.execute("PREPARE %s%s AS (%s)" %
                                    (prepared_name, prep_args, prep_query))
                self.connection_state.prepared_queries.add(prepared_name)
            if args_count > 0:
                # (%s, %s, ...)
                execute_args = ', '.join(['%s'] * args_count)
                execute_args = '(' + execute_args + ')'
            else:
                execute_args = '' 
            execute_str = 'EXECUTE %s%s' % (prepared_name, execute_args)
            return execute_str, args
            
        return query, args

    def executemany(self, query, args):
        try:
            return self.cursor.executemany(query, args)
        except self.database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except self.database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

class ConnectionState(object):
    """
    A class to pass info about the connection (prepared queries, perhaps)
    between a cursor and connection. 
    """
    def __init__(self, prepare_targets):
        self.prepare_targets = prepare_targets
        self.prepared_queries = set()
        self.last_used = datetime.now()
        self.query_count = 0

class PooledConnectionWrapper(object):
    state_object = ConnectionState

    def __init__(self, connection, pool, conn_key):
        self.connection = connection
        self.connection_state = self.state_object(pool.prepare_targets)
        self.pool = pool
        self.in_use = True
        # Abandoned connections are removable directly. Connection
        # will be abandoned if it seems broken.
        self.abandoned = False
        self.created_at = datetime.now()
        self.last_released = datetime.now()
        self.conn_key = conn_key
        self.last_connect_tb = None
        
    def close(self):
        self.pool.release(self)

    def __repr__(self):
        return "<%s, %s, %s>" % (self.in_use, self.last_released, self.connection)

    def __getattr__(self, attr):
        try:
            return self.__dict__[attr]
        except KeyError:
            return getattr(self.connection, attr)

class ConnectionPool(object):
    """
    Fakes the psycopg2 module by implementing one method, connect()

    In addition this class contains a pool of connections, a lock for
    this pool, and some methods to handle the pool & prepared plans.
    """
    def __init__(self, alias):
        #logger.debug('Initializing a new pool')
        self.alias = alias
        self.pool = [] # A list of connections this pool contains
        self.lock = Lock()
        self.database = Database
        self.connwrapper = PooledConnectionWrapper
        self.cursor_wrapper = CursorWrapper
        self.prepare_targets = {}
        self.prep_name_idx = 0

    def add_prepare_target(self, query, args_count=None):
        # Avoid circular import...
        from django.db.models.query import QuerySet
        if isinstance(query, QuerySet):
            query, args = self.qs_to_query(query)
            args_count = len(args)
        if args_count is None:
            raise ValueError("query not instance of QuerySet and "
                             "args_count is None")
        prepare_str = self.to_prepare_str(query, args_count)
        # We need a lock here because it is possible that multiple threads try
        # to simultaneously run this command, and += 1 is not thread safe...
        with self.lock:
            self.prep_name_idx += 1
            name = '%s_%s' % (PREPARED_PREFIX, self.prep_name_idx)
            self.prepare_targets[query] = (name, prepare_str, args_count)

    def qs_to_query(self, qs):
        return qs.query.get_compiler(self.alias).as_sql()

    def to_prepare_str(self, query, args_count):
        return query % tuple(['$%s' % i for i in range(1, args_count + 1)])

    def as_table(self):
        buf = []
        buf.append('*** database alias %s,  pool_id: %s' %
                  (self.alias, id(self)))
        rows = [['db', 'conn_id', 'created', 'lastrel', 'in_use', 'aband.', 'status', 'lastact']]
        for conn in self.pool[:]:
            status = self.get_tx_status(conn)
            rows.append([
                conn.conn_key[1], id(conn),
                conn.created_at.strftime("%H:%M:%S"),
                conn.last_released.strftime("%H:%M:%S"),
                conn.in_use, conn.abandoned,
                conn.abandoned and 'Aband.' or status,
                conn.connection_state.last_used.strftime("%H:%M:%S")])
        buf.extend(tablify(rows))
        return '\n'.join(buf)

    def idle_in_tx_stacks(self):
        if TRACE_IDLE_IN_TX < 1:
            return ''
        cutpoint = datetime.now() - timedelta(seconds=TRACE_IDLE_IN_TX)
        buf = []
        for conn in self.pool[:]:
            status = self.get_tx_status(conn)
            if (TRACE_IDLE_IN_TX > 0 and status in ('In tx', 'tx, error')
                    and conn.connection_state.last_used < cutpoint):
                tb = u''.join(traceback.format_list(conn.last_connect_tb))
                buf.append(tb)
        if buf:
            buf.insert(0, 'The following connect locations have created idle in '
                       'tx connections\n')
        return ''.join(buf)

    def get_tx_status(self, conn):
        status = conn.get_transaction_status()
        if status == Database.extensions.TRANSACTION_STATUS_IDLE:
            return 'Idle'
        elif status == Database.extensions.TRANSACTION_STATUS_ACTIVE:
            return 'In query'
        elif status == Database.extensions.TRANSACTION_STATUS_INTRANS:
            return 'In tx'
        elif status == Database.extensions.TRANSACTION_STATUS_INERROR:
            return 'tx, error'
        elif status == Database.extensions.TRANSACTION_STATUS_UNKNOWN:
            return 'Unknown'
        else:
            logger.error('Unknown tx status %s returned!', status)
            return 'Invalid status!'

    def connect(self, *args, **kwargs):
        while True:
            conn = self._connect(*args, **kwargs)
            if self._check_connection(conn):
               #logger.debug("got pooled connection %s" % id(conn))
               if TRACE_IDLE_IN_TX > 0:
                   conn.last_connect_tb = traceback.extract_stack()
               return conn

    def _connect(self, *args, **kwargs):
        #logger.debug("getting connection...")
        #logger.debug(self.as_table())
        conn_key = _get_conn_key(kwargs) 
        with self.lock:
            conn_count = 0
            for conn in self.pool:
                if not conn.abandoned:
                    conn_count += 1
                    if MAX_CONNECTIONS > 0 and conn_count > MAX_CONNECTIONS:
                        raise PoolOverflowException()
                if conn.in_use or conn.abandoned:
                    continue
                """
                A hack - Django's test runner will muck with the connection
                info. This means that we need to guard against Django fetching
                a connection from the pool with different connection arguments
                than this pool is defined with...
                """
                if conn_key <> conn.conn_key:
                    continue
                return conn
        conn = self.database.connect(*args, **kwargs)
        wrapped = self.connwrapper(conn, self, conn_key)
        with self.lock:
            self.pool.append(wrapped)
        #logger.debug("got new connection %s" % id(conn))
        return wrapped 

    def _check_connection(self, conn):
        try:
            cur = conn.cursor()
            cur.execute("select 1") 
            if cur.fetchone()[0] == 1:
                cur.close()
                conn.in_use = True
                return True
        except self.database.Error, e:
            logger.warn("psycopg2 exception in _check_connection()",
                        exc_info=sys.exc_info())
            self._close(conn)
            return False

    def _close(self, conn):
         conn.abandoned = True
         if not conn.closed:
             try:
                 conn.connection.close()
             except self.database.Error:
                 logger.warn("psycopg2 error in _close()",
                             exc_info=sys.exc_info())

    def clean(self, timeout=IDLE_INTERVAL):
        try:
            #logger.debug("Pool before clean (at: %s, timeout: %s):" % (
            #    datetime.now().strftime("%M:%S"),
            #    timeout
            #))
            #logger.debug(self.as_table())
            self.lock.acquire()
            new_pool = []
            cutpoint = datetime.now() - timedelta(seconds=timeout)
            for conn in self.pool:
                if conn.abandoned:
                    assert conn.connection.closed
                elif conn.last_released < cutpoint and not conn.in_use:
                    self._close(conn)
                else:
                    new_pool.append(conn)
            self.pool = new_pool
            #logger.debug("Pool after clean:")
            #logger.debug(self.as_table())
        finally:
            self.lock.release()

    def release(self, conn):
        #logger.debug("releasing %s" % id(conn))
        try:
            self.lock.acquire()
            conn.in_use = False
            cursor = conn.cursor()
            cursor.execute(RESET_QUERY)
            cursor.close()

            # Check if the connection has been open for too long.
            if RECYCLE_INTERVAL >= 0:
                cutpoint = datetime.now() - timedelta(seconds=RECYCLE_INTERVAL)
                if conn.created_at < cutpoint:
                    self._close(conn)
            else:
                conn.last_released = datetime.now()
        except self.database.Error, e:
            logger.warn("psycopg2 exception in release()",
                        exc_info=sys.exc_info())
            self._close(conn)
        finally:
            self.lock.release()

    def __getattr__(self, attr):
        try:
            return self.__dict__[attr]
        except KeyError:
            return getattr(self.database, attr)

def pool_cleaner(pools, interval):
    import time
    while True:
        try:
            time.sleep(interval)
            # Clean all pools - remove abandoned connections and those
            # connections that haven't been used in IDLE_INTERVAL.
            logger.info("Cleaning pools...")
            pool_info = []
            idle_in_tx = []
            for pool in pools.values():
                pool.clean()
                pool_info.append(pool.as_table())
                idle_in_tx_stacks = pool.idle_in_tx_stacks()
                if idle_in_tx_stacks:
                    idle_in_tx.append(idle_in_tx_stacks)
            logger.info("All pool contents after clean:\n" + '\n'.join(pool_info))
            if idle_in_tx:
                logger.info(u'\n'.join(idle_in_tx))
        except Exception, e:
            logger.error("cleaner thread got exception!",
                            exc_info=sys.exc_info())

class DatabaseWrapper(DjangoDatabaseWrapper):
    pools = {}
    pool_class = ConnectionPool
    pools_lock = Lock()
    cleaner_thread = None
    
    def __init__(self, *args, **kwargs):
        with self.pools_lock:
            super(DatabaseWrapper, self).__init__(*args, **kwargs)
            key = _get_conn_key(args[0])
            if key not in self.pools:
                pool = self.pool_class(args[1])
                self.pools[key] = pool
            self.database = self.pools[key]
            if self.cleaner_thread is None and CLEANING_INTERVAL > 0:
                 DatabaseWrapper.cleaner_thread = Thread(
                     target=pool_cleaner,
                     args=[self.pools, CLEANING_INTERVAL],
                     name='pool_cleaner_thread')
                 self.cleaner_thread.daemon = True
                 self.cleaner_thread.start()
    
    def _cursor(self, *args, **kwargs):
        """
        Change the cursorwrapper to our own cursorwrapper.
        """
        wrapped = super(DatabaseWrapper, self)._cursor(*args, **kwargs)
        return self.database.cursor_wrapper(wrapped.cursor,
                                            self.connection.connection_state)

    def __del__(self):
        # I haven't seen this method being called, ever. threading.local?
        logger.critical('Deleting id: ' + id(self))
        with self.pools_lock:
            new_pools = {}
            for key, val in DatabaseWrapper.pools.items():
                if id(val) == self.pool_id:
                    logger.critical('found my pool, removing...')
                    continue
                new_pools[key] = val
            DatabaseWrapper.pools = new_pools
