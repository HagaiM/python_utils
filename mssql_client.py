# uncompyle6 version 3.6.2
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.7.5 (tags/v3.7.5:5c02a39a0b, Oct 15 2019, 00:11:34) [MSC v.1916 64 bit (AMD64)]
# Embedded file name: /home/atoms/brinks/POSTMAN_UPDATE/db_connection.py
# Compiled at: 2019-07-18 15:10:16
# Size of source mod 2**32: 2901 bytes
import pyodbc, logging
from datetime import datetime
from pathlib import Path
# from send_mail import mail
logger = logging.getLogger(__name__)
dirpath = Path(__file__).parent
curtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
curdate = datetime.now().strftime('%Y-%m-%d')



def load_html(filepath):
    """
    Loading the html file which contain the body of the message.
    """
    with open(filepath) as f:
        email_template = f.read()
        return email_template


# alert_html = load_html(dirpath / 'alert.html')

class SqlServer:
    r"""'\n    Create a connection for the sql server using pyodbc.\n    '"""

    def __init__(self, config=None):
        constr = 'DRIVER={drv};SERVER={srv};DATABASE={db};UID={uid};PWD={pwd};'.format(drv=(config['sqlserver']['driver']),
          srv=(config['sqlserver']['server']),
          db=(config['sqlserver']['database']),
          uid=(config['sqlserver']['user']),
          pwd=(config['sqlserver']['pass']))
        try:
            self._conn = pyodbc.connect(constr)
            self._cursor = self._conn.cursor()
            logger.info('Connected to SQL Server')
        except Exception as e:
            logger.info('Failed to connect to SQL server')
            logger.error(e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()
        logger.info('Connection to SQL Server is Closed')

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def query_one(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchone()

    def rows(self):
        return self.cursor.rowcount

# create connection to DB and then close it
# with SqlServer(config) as db:  
#     current_date = get_distinct_value_from_db(db,query)
#     print(current_date)