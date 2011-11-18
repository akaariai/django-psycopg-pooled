import logging
import re
import sys
import time
import traceback
from datetime import datetime, timedelta
from threading import Thread, Lock

from django.conf import settings
from django.db import utils
from django.db.backends.postgresql_psycopg2.base import Database
from django.db.backends.postgresql_psycopg2.base import (
        DatabaseWrapper as DjangoDatabaseWrapper)
from django.db.backends.signals import connection_created

from .helpers import tablify

logger = logging.getLogger("psycopg_pooled")

# TODO: write a nice little explanation of what is actually happening in the
# code. Must wait until the code stabilises.

DEFAULT_SETTINGS = {
    # Time between checking for abandoned and long-unused connections
    'CLEANING_INTERVAL': 5,
    # How long a connection can be unused in the pool before it is closed
    # The maximum time a connection can hang around is CLEANING_INTERVAL +
    # IDLE_INTERVAL.
    'IDLE_INTERVAL': 20,
    #'CONNECTION_RESET_SQL': """
    #     -- ABORT; -- Stop doing whatever the connection might be doing
    #     -- SET SESSION AUTHORIZATION DEFAULT; -- remove name information...
    #     -- RESET ALL;  reset SET TIME ZONE etc
    #     -- CLOSE ALL; -- close cursors
    #     -- SELECT pg_advisory_unlock_all(); -- as the name says
    #     -- DISCARD TEMP; -- remove temporary tables
    #""",
    'CONNECTION_RESET_SQL': "",
    # How often should we recycle a connection. After this interval,
    # a connection that is returned to the pool will be abandoned.
    'RECYCLE_INTERVAL': 100,
    'MAX_CONNECTIONS': 20, # max non-abandoned connections one pool can have
    # Should the pool try to check for LIMIT / OFFSET conditions in the
    # end of the query and make those part of the prepared plan?
    'PREPARE_REWRITE_LIMITS': False,
    # The amount of seconds a connection must be in idle in tx (or error in
    # tx) status before it is reported as erroneous. < 0 disables this
    # reporting and also disables traceback collection.
    'TRACE_IDLE_IN_TX': 3,
    # Try to track the state of the connection. This can some "SET TIME ZONE"
    # and SHOW default_transaction_isolation calls. However, this can lead to
    # invalid state. The default False is safe, but True will give a slight
    # performance increase. Do not use RESET ALL in the reset query!
    # Not safe for now, for some reason this gives test failures...
    'TRACK_CONNECTION_STATE': True,
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
TRACK_CONNECTION_STATE = get_setting('TRACK_CONNECTION_STATE')

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
    # Matches set clauses, except when they are SET CONSTRAINTS or SET LOCAL
    must_reset_re = re.compile(r'^\s*set\s(?!\s*(local)|(constraints)\s)',
                               re.IGNORECASE)
    """
    A thin wrapper around psycopg2's normal cursor class so that we can catch
    particular exception instances and reraise them with the right types.

    This is like Django's CursorWrapper, but in addition we check for DROP
    DATABASE commands, in which case we must close all connections to prevent
    an error after test running.

    Subclasses can rewrite queries by using the _rewrite_queries hook.
    """

    def __init__(self, cursor, pool_conn):
        self.pool_conn = pool_conn
        self.cursor = cursor

    def execute(self, query, args=None):
        # A hack to make sure there are no open connections to the
        # database when dropping it. Needed for test suite cleanup.
        if query.startswith("DROP DATABASE"):
            for pool in DatabaseWrapper.pools.values():
                pool.clean(timeout=0)

        if TRACK_CONNECTION_STATE and self.must_reset_re.search(query):
            self.pool_conn.must_reset_all = True
            print 'got query %s' % query

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
        self.pool_conn.last_used = datetime.now()
        self.pool_conn.query_count += 1
        query, args = self._rewrite_query(query, args)
        """
        logger.debug("executing %s with args %s" % (query, args))
        self.cursor.execute("EXPLAIN " + query, args)
        print u'\n'.join(row[0] for row in self.cursor.fetchall())
        """
        try:
            return self.cursor.execute(query, args)
        except self.database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)),\
                  sys.exc_info()[2]
        except self.database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)),\
                  sys.exc_info()[2]

    def _rewrite_query(self, query, args):
        prepared_name, prep_query, args_count = \
            self.pool_conn.prepare_targets.get(query, (None, None, None))
        if prepared_name is not None:
            assert args_count == len(args)
            if prepared_name not in self.pool_conn.prepared_queries:
                if args_count > 0:
                    # (unknown, unknown, ...)
                    prep_args = ', '.join(['unknown'] * args_count)
                    prep_args = '(' + prep_args + ')'
                else:
                    prep_args = ''
                self.cursor.execute("PREPARE %s%s AS (%s)" %
                                    (prepared_name, prep_args, prep_query))
                self.pool_conn.prepared_queries.add(prepared_name)
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
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)),\
                  sys.exc_info()[2]
        except self.database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)),\
                  sys.exc_info()[2]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

class PooledConnection(object):

    def __init__(self, connection, pool, conn_key):
        self.connection = connection
        self.pool = pool
        self.in_use = True
        # Abandoned connections are removable directly. Connection
        # will be abandoned if it seems broken.
        self.abandoned = False
        self.created_at = datetime.now()
        self.last_released = datetime.now()
        self.conn_key = conn_key
        self.last_connect_tb = None
        self.prepare_targets = pool.prepare_targets
        self.prepared_queries = set()
        self.last_used = datetime.now()
        self.query_count = 0
        self.isolation_level = None
        self.timezone = None
        self.must_reset_all = False

    def close(self):
        assert self.connection.closed == False
        self.pool.release(self)

    def __repr__(self):
        return "<%s, %s, %s>" % (self.in_use, self.last_released,
                                 self.connection)

class ConnectionWrapper(object):
    def __init__(self, pooled_connection):
        self.connection = pooled_connection.connection
        self.pool_conn = pooled_connection

    def close(self):
        self.connection = None
        self.pool_conn.close()

    def cursor(self):
        if self.connection is None:
            raise Database.InterfaceError('This connection is closed!')
        return CursorWrapper(self.connection.cursor(), self.pool_conn)

    def __del__(self):
        if self.connection is not None:
            self.close()

    def __getattr__(self, attr):
        try:
            return self.__dict__[attr]
        except KeyError:
            # TODO: keep around a closed connection so that we can just
            # call it and that will give us proper connections...
            if self.connection is None and attr <> 'closed':
                raise Database.InterfaceError('This connection is closed!')
            return getattr(self.connection, attr)

    def set_isolation_level(self, level):
        if TRACK_CONNECTION_STATE and level == self.pool_conn.isolation_level:
            return
        else:
            self.connection.set_isolation_level(level)
            self.pool_conn.isolation_level = level

    def set_timezone(self, tz):
        if TRACK_CONNECTION_STATE and tz == self.pool_conn.timezone:
            return
        else:
            self.set_isolation_level(0)
            self.cursor().cursor.execute("SET TIME ZONE %s", (tz,))
            self.pool_conn.timezone = tz


class ConnectionPool(object):
    """
    Fakes the psycopg2 module by implementing one method, connect()

    In addition this class contains a pool of connections, a lock for
    this pool, and some methods to handle the pool & prepared plans.
    """
    def __init__(self, alias):
        #logger.debug('Initializing a new pool')
        self.alias = alias
        self.pool = []
        self.lock = Lock()
        self.database = Database
        self.connwrapper = PooledConnection
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
        buf.append('*** database alias %s, pool_id: %s' %
                  (self.alias, id(self)))

        # The first row is the headers...
        rows = [['db', 'conn_id', 'created', 'lastrel', 'in_use',
                 'aband.', 'status', 'lastact']]

        for conn in self.pool[:]:
            status = self.get_tx_status(conn)
            rows.append([
                conn.conn_key[1], id(conn),
                conn.created_at.strftime("%H:%M:%S"),
                conn.last_released.strftime("%H:%M:%S"),
                conn.in_use, conn.abandoned,
                conn.abandoned and 'Aband.' or status,
                conn.last_used.strftime("%H:%M:%S")])
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
                    and conn.last_used < cutpoint):
                tb = 'Not available...'
                buf.append(tb)
        if buf:
            buf.insert(0, 'The following connect locations have created'
                       ' idle in tx connections\n')
        return ''.join(buf)

    def get_tx_status(self, conn):
        status = conn.connection.get_transaction_status()
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
        """
        Fetch a connection from the pool (or create a new pooled connection).
        """
        # We implement this by fetching a connection from pool, and checking
        # it is valid. We want to do the checking outside of the lock to
        # avoid contention.
        for i in range(0, 100):
            pooled_conn = self._connect(*args, **kwargs)
            if self._check_connection(pooled_conn):
                #logger.debug("got pooled connection %s" % id(pooled_conn))
                if TRACE_IDLE_IN_TX > 0:
                    pooled_conn.last_connect_tb = traceback.extract_stack()
                return ConnectionWrapper(pooled_conn)
        # TODO: proper exception, proper handling of timeouts etc..
        raise Exception("100 connects didn't produce a connection...")

    def _connect(self, *args, **kwargs):
        """
        Go through the self.pool and check if we have open and unused
        connections available. Otherwise create a connection.
        Note that anything given out from here must have in_use = True,
        otherwise the cleaner thread might smack the connection, even
        if you set in_use = True just after releasing self.lock.
        """
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
                conn.in_use = True
                return conn
        # fetch the connection without locks - avoid lock contention.
        conn = self.database.connect(*args, **kwargs)
        wrapped = self.connwrapper(conn, self, conn_key)
        with self.lock:
            self.pool.append(wrapped)
        #logger.debug("got new connection %s" % id(conn))
        return wrapped

    def _check_connection(self, conn):
        try:
            cur = conn.connection.cursor()
            cur.execute("select 1")
            if cur.fetchone()[0] == 1:
                cur.close()
                return True
        except self.database.Error, e:
            logger.warn("psycopg2 exception in _check_connection()",
                        exc_info=sys.exc_info())
            self._close(conn)
            return False

    def _close(self, conn):
         conn.abandoned = True
         if not conn.connection.closed:
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
            conn.connection.rollback()
            if TRACK_CONNECTION_STATE and conn.must_reset_all:
                cursor = conn.connection.cursor()
                print 'ran reset all...'
                cursor.execute('RESET ALL')
                cursor.close()
                conn.must_reset_all = False
                conn.timezone = None

            if RESET_QUERY:
                print 'running reset query...'
                cursor = conn.connection.cursor()
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
            logger.info("All pool contents after clean:\n%s"
                         % '\n'.join(pool_info))
            if idle_in_tx:
                logger.info(u'\n'.join(idle_in_tx))
        except Exception, e:
            logger.exception('Got exception in pool_cleaner thread!')

class DatabaseWrapper(DjangoDatabaseWrapper):
    pools = {}
    pool_class = ConnectionPool
    pools_lock = Lock()
    cleaner_thread = None

    def __init__(self, *args, **kwargs):
        with self.pools_lock:
            super(DatabaseWrapper, self).__init__(*args, **kwargs)
            # The tests will create a DatabaseWrapper without an alias..
            try:
                key = args[1]
            except IndexError:
                key = 'anonymous'
            if key not in self.pools:
                pool = self.pool_class(key)
                self.pools[key] = pool
            self.database = self.pools[key]
            self.pool_id = id(self.database)
            if self.cleaner_thread is None and CLEANING_INTERVAL > 0:
                 DatabaseWrapper.cleaner_thread = Thread(
                     target=pool_cleaner,
                     args=[self.pools, CLEANING_INTERVAL],
                     name='pool_cleaner_thread')
                 self.cleaner_thread.daemon = True
                 self.cleaner_thread.start()

    def _cursor(self):
        """
        This is a copy-paste of postgresql_psycopg2 backend's ._cursor()
        method, except this calls the new connection from self.database,
        and this does not wrap the cursor, ConnectionWrapper already does
        that...
        """
        new_connection = False
        set_tz = False
        settings_dict = self.settings_dict
        if self.connection is None:
            new_connection = True
            set_tz = settings_dict.get('TIME_ZONE')
            if settings_dict['NAME'] == '':
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured(
                    "You need to specify NAME in your Django settings file.")
            conn_params = {
                'database': settings_dict['NAME'],
            }
            conn_params.update(settings_dict['OPTIONS'])
            if 'autocommit' in conn_params:
                del conn_params['autocommit']
            if settings_dict['USER']:
                conn_params['user'] = settings_dict['USER']
            if settings_dict['PASSWORD']:
                conn_params['password'] = settings_dict['PASSWORD']
            if settings_dict['HOST']:
                conn_params['host'] = settings_dict['HOST']
            if settings_dict['PORT']:
                conn_params['port'] = settings_dict['PORT']
            self.connection = self.database.connect(**conn_params)
            self.connection.set_client_encoding('UTF8')
            if set_tz:
                self.connection.set_timezone(settings_dict['TIME_ZONE'])
            self.connection.set_isolation_level(self.isolation_level)
            self._get_pg_version()
            connection_created.send(sender=self.__class__, connection=self)
        cursor = self.connection.cursor()
        cursor.cursor.tzinfo_factory = None
        return cursor

    def __del__(self):
        logger.critical('Deleting id: ' + id(self))
        with self.pools_lock:
            new_pools = {}
            for key, val in DatabaseWrapper.pools.items():
                if id(val) == self.pool_id:
                    logger.critical('found my pool, removing...')
                    continue
                new_pools[key] = val
            DatabaseWrapper.pools = new_pools
