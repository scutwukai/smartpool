#!/usr/bin/env python
#-*- coding:utf-8 -*-


import os
import weakref
import threading
from functools import wraps

####### copy from werkzeug.local #######
# since each thread has its own greenlet we can just use those as identifiers
# for the context.  If greenlets are not available we fall back to the
# current thread ident depending on where it is.
try:
    from greenlet import getcurrent as get_ident
except ImportError:
    try:
        from thread import get_ident
    except ImportError:
        from _thread import get_ident


######## global vars ########


__all__ = [
    "getconn",
    "init_pool",
    "ConnectionProxy",
]


lock = threading.RLock()
pools = {}


######## setting ########


# the logger for logging, set outside
pool_logger = None


########### utils ############

class EmptyPoolError(Exception):
    pass

def hex_ident():
    ident = get_ident()
    if not isinstance(ident, int):
        ident = id(ident)

    return "%x" % ident

def log(msg):
    if pool_logger is None:
        return

    pid = os.getpid()
    ident = hex_ident()

    pool_logger("[%d][%s]%s" % (pid, ident, msg))

def plog(pool, msg):
    full_msg = "[%s][idle/total/min: %d/%d/%d] - %s" % \
        (pool.name, pool.idle, pool.total, pool.min, msg)

    log(full_msg)

def safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        pass

def total_seconds(td):
    return td.days * 60 * 60 * 24 + td.seconds


########### local storage ############


class Local(object):
    def __init__(self):
        self._storage = {}

    @property
    def ident(self):
        return get_ident()

    @property
    def local_storage(self):
        return self._storage.setdefault(self.ident, {})

    def __getitem__(self, key):
        return self.local_storage[key]

    def __setitem__(self, key, value):
        self.local_storage[key] = value

    def get(self, key, default=None):
        return self.local_storage.get(key, default)

    def pop(self, key):
        return self.local_storage.pop(key)


############ pool logic ###########



class Connection(object):
    """the connection class base"""

    def __init__(self, **db_config):
        self._db_config = db_config

    def __del__(self):
        """last chance for close"""
        try:
            self.close()
        except:
            pass

    @property
    def reusable(self):
        """identify this connection can be reuse or not.
        for example, if connection is in a started transaction, then it's not reusable.
        if you don't set this sign properly, the pool might be useless.
        """
        return True

    @property
    def idle(self):
        """the idle seconds after last action."""
        return 0

    def ping(self):
        """just as it says"""
        return False

    def connect(self):
        """just as it says"""
        pass

    def close(self):
        """just as it says"""
        pass

    def make_reusable(self):
        """call by pool to make sure this connetion reusable.
        for example, if this kind of connections support transaction,
        call rollback to be sure it's not in a started transaction.
        """
        pass


class ConnectionPool(object):
    def __init__(self, db_name, db_config, conn_cls, minnum, maxnum=None, maxidle=60, clean_interval=100):
        self._db_name = db_name
        self._db_config = db_config
        self._conn_cls = conn_cls
        self._min = minnum
        self._max = minnum if maxnum is None else maxnum
        self._maxidle = maxidle
        self._clean_interval = clean_interval

        self._pool = []
        self._clean_counter = 0

        plog(self, "init finished!")

    @property
    def name(self):
        return self._db_name

    @property
    def min(self):
        return self._min

    @property
    def busy_array(self):
        return sorted(self._pool, key=(lambda v: v.idle))

    @property
    def idle_array(self):
        return sorted(self._pool, key=(lambda v: v.idle), reverse=True)

    @property
    def total(self):
        return len(self._pool)

    @property
    def idle(self):
        counter = 0
        for conn in self._pool:
            if weakref.getweakrefcount(conn) < 1:
                counter += 1
        return counter

    @property
    def used(self):
        return self.deep - self.idle


    def _clean(self):
        self._clean_counter = 0

        plog(self, "clean: prepare to clean!")

        if self.total <= self._min:
            plog(self, "clean: pool is tiny, do nothing!")
            return

        if self.idle < 1:
            plog(self, "clean: no idle conn found, do nothing!")
            return

        total, found = (self.total - self._min), []
        for conn in self.idle_array:
            if conn.idle > self._maxidle:
                found.append(conn)
                if len(found) >= total:
                    break

        if len(found) < 1:
            plog(self, "clean: no idle conn found, do nothing!")
            return

        # be sure to remove from pool first
        for conn in found:
            self._pool.remove(conn)

        # do close
        for conn in found:
            safe_call(conn.close)

        plog(self, "clean: idle conn found, %d closed!" % len(found))


    def _pop_idle(self):
        for conn in self.busy_array:
            if weakref.getweakrefcount(conn) > 0:
                continue

            return id(conn), weakref.proxy(conn)

        return None, None


    def get(self):

        with lock:
            # clean if need
            if self._clean_counter >= self._clean_interval:
                self._clean()

            self._clean_counter += 1

            # do grant
            plog(self, "prepare to grant conn!")
            conn_ident, conn = self._pop_idle()


        if conn is not None:
            if not conn.ping():
                conn.connect()

            elif not conn.reusable:
                conn.make_reusable()

            plog(self, "idle conn(%x) was granted!" % conn_ident)
            return conn

        elif self.total < self._max:
            conn = self._conn_cls(**self._db_config)
            self._pool.append(conn)

            conn_ident = id(conn)

            conn = weakref.proxy(conn)
            conn.connect()

            # dig the pool

            plog(self, "not idle conn found, new conn(%x) was granted!" % conn_ident)
            return conn

        return None


############ lazy proxy  #############


def lazy(db_name, local, name):
    def wrap_func(*args, **kwargs):
        conn = local.get("conn")
        if conn is None:
            conn = getconn(db_name)
            if conn is None:
                raise EmptyPoolError()

        try:
            return getattr(conn, name)(*args, **kwargs)
        finally:
            if conn.reusable:
                safe_call(local.pop, "conn")
                del conn
            else:
                local["conn"] = conn

    return wrap_func


class ConnectionProxy(object):
    def __init__(self, db_name):
        self._db_name = db_name
        self._local = Local()

    def __getattr__(self, name):
        return lazy(self._db_name, self._local, name)


########################  free to use  ########################


def getconn(db_name):
    return pools[db_name].get()


# init before concurrence
def init_pool(db_name, *args, **kwargs):
    pools[db_name] = ConnectionPool(db_name, *args, **kwargs)
