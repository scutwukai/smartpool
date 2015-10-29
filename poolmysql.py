#!/usr/bin/env python
#-*- coding:utf-8 -*-


import MySQLdb
from datetime import datetime, timedelta
from contextlib import contextmanager, closing
from base.smartpool import Connection



query_logger = None

__all__ = [
    "transaction",
    "lock_str",
    "MySQLdbConnection",
]


#################


def total_seconds(td):
    return td.days * 60 * 60 * 24 + td.seconds

def log(ident, msg, logger):
    if logger is None:
        return
    logger("%d - %s" % (ident, msg))

def qlog(conn, msg):
    cid = id(conn)
    log(cid, msg, query_logger)

def active_time(old_handler):
    def new_handler(self, *args, **kwargs):
        self._last_active_time = datetime.now()
        ret = old_handler(self, *args, **kwargs)
        self._last_active_time = datetime.now()
        return ret
    return new_handler

def ready(old_handler):
    def new_handler(self, *args, **kwargs):
        if self._conn is None and self.reusable:
            self.connect()
        return old_handler(self, *args, **kwargs)
    return new_handler


#######################


class Job(object):
    """
    A indicator to mark whether the job is finished.
    """
    def __init__(self):
        self._finished = False

    def is_finished(self):
        return self._finished

    def finish(self):
        self._finished = True



@contextmanager
def transaction(conn):
    """
    Automatic handle transaction COMMIT/ROLLBACK. You MUST call trans.finish(),
    if you want to COMMIT; Otherwise(not call or exception occurs), ROLLBACK.

    >>> with transaction(conn) as trans:
    >>>     do something...
    >>>     if xxxxx:
    >>>         # if you don't want to commit, you just not call trans.finish().
    >>>         return error_page("xxxxxx")
    >>>     # if you want to commit, you call:
    >>>     trans.finish()

    @param conn: database connection
    """
    trans = Job()
    conn.begin()

    try:
        yield trans
    except:
        conn.rollback()
        raise

    if trans.is_finished():
        conn.commit()
    else:
        conn.rollback()


@contextmanager
def lock_str(conn, s, timeout=0):
    """
    Automatic handle lock/release a database string lock.

    >>> with lock_str(conn, s, timeout) as locked:
    >>>     if not locked:
    >>>         # lock 's' failed
    >>>     do something
    >>>     # after the block, the lock will be automatic released

    @param conn: database connection
    @param s: the string wanted to lock
    @param timeout: how many seconds to wait for getting the lock
    """
    locked = False

    try:
        locked = conn.lock(s, timeout)
        yield locked

    finally:
        if locked:
            conn.release(s)


###########################################


class MySQLdbConnection(Connection):
    def __init__(self, **db_config):
        super(MySQLdbConnection, self).__init__(**db_config)

        # private
        self._conn = None
        self._locks = []
        self._in_trans = False
        self._last_active_time = None

    def __deepcopy__(self, memo):
        return self

    @property
    def in_trans(self):
        return self._in_trans

    @property
    def has_lock(self):
        return len(self._locks) > 0

    ################ pool interface #################

    @property
    def reusable(self):
        return not (self.in_trans or self.has_lock)

    @property
    def idle(self):
        if self._last_active_time is None:
            return 0

        nowtime = datetime.now()
        return total_seconds(nowtime - self._last_active_time)

    def ping(self):
        qlog(self, "ping")

        if self._conn is None:
            return False

        try:
            self._conn.ping()
            return True
        except MySQLdb.DatabaseError:
            return False

    @active_time
    def connect(self):
        qlog(self, "connect")
        self._conn = MySQLdb.connect(**self._db_config)
        self._conn.autocommit(True)

    def close(self):
        qlog(self, "close")

        if self._conn is None:
            return

        try:
            self._conn.close()
        finally:
            self._conn = None

    def make_reusable(self):
        if self.in_trans:
            self.rollback()

        if self.has_lock:
            for key in self._locks:
                self.release(key)


    ############## base dbop #############

    @ready
    @active_time
    def select(self, sql, params=None, dict_cursor=False):
        qlog(self, "execute: %s - %s" % (sql, repr(params)))

        cursor = None
        if dict_cursor:
            cursor = self._conn.cursor(MySQLdb.cursors.DictCursor)
        else:
            cursor = self._conn.cursor()

        with closing(cursor) as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)

            return cur.fetchall()

    @ready
    @active_time
    def insert(self, sql, params=None):
        qlog(self, "execute: %s - %s" % (sql, repr(params)))

        cursor = self._conn.cursor()
        with closing(cursor) as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)

            return cur.lastrowid

    @ready
    @active_time
    def execute(self, sql, params=None):
        qlog(self, "execute: %s - %s" % (sql, repr(params)))

        cursor = self._conn.cursor()
        with closing(cursor) as cur:
            if params is None:
                return cur.execute(sql)
            else:
                return cur.execute(sql, params)


    ############### unreusable #############

    def begin(self):
        if self._in_trans:
            raise Exception("nested trans is not allowed")

        self.execute("begin")
        self._in_trans = True

    def rollback(self):
        self.execute("rollback")
        self._in_trans = False

    def commit(self):
        self.execute("commit")
        self._in_trans = False

    def lock(self, key, timeout=0):
        locked = self.select("SELECT GET_LOCK(%s, %s)", (key, timeout))[0][0] == 1

        if locked:
            self._locks.append(key)

        return locked

    def release(self, key):
        released = self.select("SELECT RELEASE_LOCK(%s)", (key, ))[0][0] == 1

        if released and key in self._locks:
            self._locks.remove(key)

        return released
