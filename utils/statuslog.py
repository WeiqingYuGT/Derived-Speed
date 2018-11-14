# -*- coding: utf-8 -*-

"""
Copyright (C) 2017. xAd, Inc.  All Rights Reserved.
"""

import logging
import os.path
import re
import mysql.connector

from conf import Conf


class StatusLog:
    """StatusLog

    This utility provides access functions to a status log table in a
    database.  It allows the user to get the status from the table
    and to add records into the table.  Using database table to track
    status is one good to communicate status among different
    projects.  The status is available to every process that has
    access to the database.
    
    This is meant to be the Python equivalent of the
    xAd Perl StatusLog.pm.  Currently, only MySQL is supported.
    
    
    SCHEMA
    
    The status_log table is composed of the following fields:
    
       id
       timestamp
       key VARCHAR
       year SMALLINT
       month TINYINT
       day TINYINT
       hour TINYINT
       status TINYINT
       meta VARCHAR
    
    Among them, year,month,day,hour are time related to an event.
    Some of them can be NULL (e.g. hour can be NULL if it is a daily event).
    "key" is a unique task identify.   The id and timestamp are automatically
    handled by the tool.
    
    
    INITIALIZATION
    
    First, it can be initialized through a Conf object, thich can be passed to
    StatusLog through the constructor or the confInit() function.
    The Conf object has the following properties:
    
        <prefix>.table   = status_log
        <prefix>.db.type = mysql
        <prefix>.db.conn.mysql.user      = ?
        <prefix>.db.conn.mysql.password  = ?
        <prefix>.db.conn.mysql.host      = ?
        <prefix>.db.conn.mysql.port      = ?
        <prefix>.db.conn.mysql.dbname    = ?

    The default prefix is 'status_log'.   Each instance of StatusLog
    holds the connection information for one database.   It is possible
    to have other instances.  
    
    Another way to initialize the StatusLog is through the init() function.
    The caller needs to pass all of the connection information through
    the function.
    
    
    EXAMPLES
    
    See the test functions in this file.
    """
    
    #------------------
    # Class Variables
    #------------------
    # Fields
    FIELD_TIMESTAMP = 'timestamp'
    FIELD_KEY       = '`key`'
    FIELD_YEAR      = 'year'
    FIELD_MONTH     = 'month'
    FIELD_DAY       = 'day'
    FIELD_HOUR      = 'hour'
    FIELD_STATUS    = 'status'
    FIELD_METADATA  = 'metadata'
    
    # Database Functions
    TIMESTAMP_MYSQL = 'now()'
    
    # Entry Type
    ENTRY_TYPE_FILE    = 1
    ENTRY_TYPE_DIR     = 2
    ENTRY_TYPE_DONTCARE = 0


    def __init__(self, cfg=None, prefix='status_log'):
        """Constructor """
        self.cfg = cfg
        self.prefix = prefix
        self.dbtype = 'mysql'
        self.proto = 'DBI:mysql'
        self.table  = 'status_log'
        self.host = ''
        self.port = '3306'
        self.dbname = ''
        self.user = 'etl'
        self.password = ''
        self.initialized = False

    def init(self, host, port, dbname, user, password,
             dbtype='mysql', table='status_log'):
        """Initialize StatusLog with connection credeitials"""
        
        # DB Type
        if (dbtype):
            self.dbtype = dbtype
        
        if (dbtype == 'mysql'):
            self.timestamp = StatusLog.TIMESTAMP_MYSQL
        else:
            logging.error("Unknown dbtype: {}".format(dbtype))
        
        # Table
        if (table):
            self.table = table
        
        # Host
        if (host):
            self.host = host
        else:
            logging.error('host is not defined')
        
        # Port
        if (port):
            self.port = port
        else:
            logging.error('port is not defined')
            
        # Database Name
        if (dbname):
            self.dbname = dbname
        else:
            logging.error('dbname is not defined')
            
        # User
        if (user):
            self.user = user
        else:
            logging.error('user is not defined')
            
        # Password
        if (password):
            self.password = password
        else:
            logging.error('password is missing')
                
        self.initialized = True


    def confInit(self, conf, prefix='status_log'):
        """Initialize through a Conf object"""
        
        self.prefix = prefix
        
        dbtype = conf.get(prefix + '.db.type')
        if (dbtype == 'mysql'):
            self.init(
                dbtype = dbtype,
                user = conf.get(prefix + '.db.conn.mysql.user'),
                password = conf.get(prefix + '.db.conn.mysql.password'),
                host = conf.get(prefix + '.db.conn.mysql.host'),
                port = conf.get(prefix + '.db.conn.mysql.port'),
                dbname = conf.get(prefix + '.db.conn.mysql.dbname'),
                table = conf.get(prefix + '.table.mysql', prefix + '.table')
            )
        else:
            # Throw exception
            logging.error('DBType "{}" is not supported.'.format(dbtype))

    def _checkInit(self):
        if (not self.initialized):
            if (self.cfg):
                self.confInit(self.cfg, self.prefix)
            else:
                logging.error("StatusLog is not initialized!")
        return self.initialized
    
    
    def dump(self):
        """Dump the contents"""
        logging.info('#-----------------')
        logging.info('# StatusLog Dump')
        logging.info('#-----------------')
        logging.info("- prefix = '{}'".format(self.prefix))
        logging.info("- dbtype = '{}'".format(self.dbtype))
        logging.info("- host = '{}'".format(self.host))
        logging.info("- port = {}".format(self.port))
        logging.info("- dbname = '{}'".format(self.dbname))
        logging.info("- table = '{}'".format(self.table))
        logging.info("- user = '{}'".format(self.user))
        logging.info("- password = '{}'".format(self.password))
        logging.info("- initialized = {}".format(self.initialized))


    def getConnection(self):
        """Get the Database Connection"""
        if (not self.initialized):
            logging.error("Module is not initialized")
        
        conn_options = {
            'user': self.user,
            'password' : self.password,
            'host' : self.host,
            'port' : self.port,
            'database' : self.dbname,
            'raise_on_warnings': True
        }
        db = mysql.connector.connect(**conn_options)
        return db


    def addStatus(self,key,time,status=1,meta=None):
        """Add a record into the status log table.
        
        Parameters:
         key: a unique key that identify the event type.
         time: in the format of yyyy/mm/dd/hh.  Fields can be empty
               For example, 2017/01/12/00 will be an hourly event, while
               2017/01/12  will be an daily event. 
         status: default is 1
         meta: default is None.
        """
        
        # Check connection
        self._checkInit()
        
        # Split the time string
        (year,month,day,hour) = self._splitTime(time)

        # Construct the query
        query = "INSERT INTO {} {} VALUES {}".format(
             self.table,
             self._fieldList(key,year,month,day,hour,status,meta),
             self._valueList(key,year,month,day,hour,status,meta))

        logging.debug("query= \"{}\"".format(query))
        
        # Get the connection
        cnx = self.getConnection()
        cur = cnx.cursor(buffered=True)
        
        cur.execute(query)
        cnx.commit()
        cnx.close()

    
    def getStatus(self, key, time):
        """Get the status of the specified key and time"""
        return self.get("status", key, time)

    def getTimestamp(self, key, time):
        """Get the timestamp of the specified key and time"""
        return self.get("timestamp", key, time)

    def get(self, column, key, time):
        """Get a column for the specified key and time"""
        # Check connection
        self._checkInit()
        
        # Split the time string
        (year,month,day,hour) = self._splitTime(time)

        # Construct the query
        query = "SELECT {} FROM {} WHERE {} ORDER BY TIMESTAMP DESC LIMIT 1".format(
             column,
             self.table,
             self._exactMatchClause(key,year,month,day,hour))       

        #logging.debug("query: \"{}\"".format(query))

        # Get Connection
        cnx = self.getConnection()
        cur = cnx.cursor()
        cur.execute(query)
        retval = None
        for fields in cur:
            retval = fields[0]
            break
        cur.close()
        cnx.close()    
        return retval      

    def get_latest(self, key):
        """Get a column for the specified key and time"""
        # Check connection
        self._checkInit()
        
        # Construct the query
        query = """SELECT 
            date_format(max(str_to_date(concat(year,',',month,',',day,',',hour),'%Y,%c,%e,%k')),'%Y-%m-%d-%H')
            FROM {} WHERE `key`='{}'""".format(
             self.table,
             key)

        #logging.debug("query: \"{}\"".format(query))

        # Get Connection
        cnx = self.getConnection()
        cur = cnx.cursor()
        cur.execute(query)
        retval = None
        for fields in cur:
            retval = fields[0]
            break
        cur.close()
        cnx.close()
        return retval

    def _splitTime(self, time):
        """Split a time string, which can be empty or
        year, month, day, hour delimited by [/ - :] and spaces.
        Right part of the time information can be missing.  Thus, time can be
        year/month/day/hh, year/month/day, year/month, and year.
        """         
        if (time):
            x = re.split("[-\/\s:]", time)
        else:
            x = []
        # Pad the list to four elements (year,month,day,hour)
        while (len(x) < 4):
            x.append(None)
        return x
    

    def _fieldList(self, key, year, month=None, day=None, hour=None, status=1, metaData=None):
        """Construct field list for INSERT"""
        fields = [StatusLog.FIELD_TIMESTAMP]
        if (key is not None):
            fields.append(StatusLog.FIELD_KEY)
        if (year is not None):
            fields.append(StatusLog.FIELD_YEAR)
        if (month is not None):
            fields.append(StatusLog.FIELD_MONTH)
        if (day is not None):
            fields.append(StatusLog.FIELD_DAY)
        if (hour is not None):
            fields.append(StatusLog.FIELD_HOUR)
        if (status is not None):
            fields.append(StatusLog.FIELD_STATUS)
        if (metaData is not None):
            fields.append(StatusLog.FIELD_METADATA)
        
        # Make a string
        return  '(' + ', '.join(fields) + ')'


    def _valueList(self, key, year, month=None, day=None, hour=None, status='1', metaData=None):
        """Construct field list for INSERT"""
        vals = [self.timestamp]
        if (key is not None):
            vals.append("'{}'".format(key))
        if (year is not None):
            vals.append(str(year))
        if (month is not None):
            vals.append(str(month))
        if (day is not None):
            vals.append(str(day))
        if (hour is not None):
            vals.append(str(hour))
        if (status is not None):
            vals.append(str(status))
        if (metaData is not None):
            vals.append("'{}'".format(meta))

        return '(' + ', '.join(vals) + ')'


    def _exactMatchClause(self, key, year, month=None, day=None, hour=None):
        
        clauses = list()
        if (key is not None):
            clauses.append("{}='{}'".format(StatusLog.FIELD_KEY, key))
        if (year is not None):
            clauses.append("{}={}".format(StatusLog.FIELD_YEAR, year))           
        if (month is not None):
            clauses.append("{} = {}".format(StatusLog.FIELD_MONTH, month))           
        if (day is not None):
            clauses.append("{} = {}".format(StatusLog.FIELD_DAY, day))           
        if (hour is not None):
            clauses.append("{} = {}".format(StatusLog.FIELD_HOUR, hour))
            
        return ' AND '.join(clauses)


    def _selectAll(self, limit=50):
        """Select All from the table for testing"""
        self._checkInit()
        
        query = 'SELECT `key`,year,month,day,hour,status from {} limit {}'.format(self.table, limit)
        cnx = self.getConnection()
        cur = cnx.cursor()
        cur.execute(query)
        print "# Select ALL"
        for (key, year, month, day, hour, status) in cur:
            print("'{}', {}/{}/{}/{} = {}".format(key,year,month,day,hour,status))
        cur.close()
        cnx.close()    


def _testStatus(statusLog, key, time):
    
    status = statusLog.getStatus(key, time)
    logging.info("getStatus({}, {}) = {}".format(key, time, status))


def _test1():
    """Load the full coniguration"""
    logging.info("### TEST1 ###")
    mainDirs = "../../../../test/config"
    conf = Conf()
    conf.load("status_log.properties", mainDirs)
    conf.dump()

    # Default.  No connection information
    sl0 = StatusLog()
    sl0.dump()
    
    # Use init() to initialize
    sl1 = StatusLog()
    sl1.init(host='nn02.corp.xad.com', port=3306, user='etl', password='foobar',
             dbname='xad_etl')
    sl1.dump()
    
    # Use conf to initialize.  Prefix = 'status_log'
    sl2 = StatusLog(conf)
    sl2._checkInit()
    sl2.dump()
    
    # Use conf to initialize.  Prefix = 'status_log_local'
    sl3 = StatusLog()
    sl3.confInit(conf, prefix='status_log_local')
    sl3.dump()
    
    #sl3.addStatus('TEST/statuslog_py/hourly', '2017/01/22/11')
    #sl3.addStatus('TEST/statuslog_py/DAILY', '2017/01/22')

    sl3._selectAll(50);
    
    _testStatus(sl3, 'TEST/statuslog_py/hourly', '2017/01/22/11')
    _testStatus(sl3, 'TEST/statuslog_py/hourly', '2017/01/22/12')
    




def main():
    """ Unit Test.  Run from the current directory."""
    # Set up logging
    fmt = ("[%(module)s:%(funcName)s] %(levelname)s %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=datefmt, level=logging.DEBUG)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    _test1()
    logging.info('Done!')


if (__name__ == "__main__"):
    """Unit Test"""
    main()
