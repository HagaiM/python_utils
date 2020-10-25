#!/usr/bin/python
from pandas import DataFrame
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import sys
from io import StringIO
import shutil



class PostgresClient:
    def __init__(self,host, user, password, database,table = None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.param_dic = {
            "host": self.host,
            "database": self.user,
            "user": self.password,
            "password": self.database
        }

    def connect(self):
        """ Connect to the PostgreSQL database server """

        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.param_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1)
        print("Connection successful")
        return conn
    def read_csv(self, filename):
        'converts a filename to a pandas dataframe'
        try:
            return pd.read_csv(filename)
        except:
            old_file_name = filename
            new_file = filename.replace(self.files_to_load_path[:-1],self.failure_files_path[:-1])
            shutil.move(old_file_name , new_file)

    def copy_from_stringio_to_db(self,conn, df, table):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep=",")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:

            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("copy_from_stringio() done")
        cursor.close()
        conn.close()

    def df_to_postgresql_bulk(self,df, dest_table, type='append'):
        sh = df.shape[0]
        bulk_number = 50000
        reminder = sh % bulk_number
        engine = create_engine('postgresql+psycopg2://' + self.user + ':' + self.password + '@' + self.host + '/' + self.database,
                               use_batch_mode=True)
        num = int((sh - reminder) / bulk_number)
        fromrow = 0
        torow = bulk_number
        for i in range(0, num + 1):
            with engine.connect() as conn, conn.begin():
                print("start iteration {}".format(i))
                df[fromrow:torow].to_sql(dest_table, conn, if_exists=type, index=False)
                print("end iteration {}".format(i))
            if i == num:
                fromrow += reminder
                torow += reminder
            fromrow += bulk_number
            torow += bulk_number

    def postgresql_to_df(self,arg_query):
        alchemyEngine = create_engine('postgresql+psycopg2://' + self.user + ':' + self.password + '@' + self.host + '/' + self.database)
        # Connect to PostgreSQL server
        dbConnection = alchemyEngine.connect()
        query = dbConnection.execute(arg_query)  ##rows_count
        rows = query.fetchall()
        columns = query.keys()
        if len(rows) > 0:
            df = DataFrame(rows)
            df.columns = columns
            return df
        else:
            print("no data")
            return None

    def run_query_postgresql(self,query):
        alchemyEngine = create_engine('postgresql+psycopg2://' + self.user + ':' + self.password + '@' + self.host + '/' + self.database)
        dbConnection = alchemyEngine.connect()
        if "truncate" in query.lower() or "update" in query.lower() or "insert" in query.lower():
            query = dbConnection.execute(query)  ##rows_count
        else:
            query = dbConnection.execute(query)
            rows = query.fetchall()
            if rows != None:
                return rows

    def copy_csv_to_postgres(self, csv_file_path, dest_table, header = None):
        con = None
        f = None

        try:
            con = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password)
            cur = con.cursor()
            f = open(csv_file_path, 'r')
            if header == True:
                next(f)
            cur.copy_from(f, dest_table, sep=",")
            con.commit()

        except psycopg2.DatabaseError as e:
            if con:
                con.rollback()
            print(f'Error {e}')
            sys.exit(1)

        except IOError as e:
            if con:
                con.rollback()
            print(f'Error {e}')
            sys.exit(1)

        finally:
            if con:
                con.close()
            if f:
                f.close()


######################################################### Function Instructions #########################################################

######################################################### Example for load df to postgres with copy function
# import time
# start_time = time.time()
# df = pd.read_csv('')
# host ="localhost"
# database= "postgres"
# user= "postgres"
# password= "postgres"
# pc = PostgresClient(host, database, user,password)
# conn = pc.connect()  # connect to the database
# pc.copy_from_stringio_to_db(conn, df, 'dest_table')  # copy the dataframe to SQL
# print("--- %s seconds ---" % (time.time() - start_time))
######################################################### Example for load df to postgres with copy function




######################################################### Example for load csv direct to postgres with copy function
# import time
# start_time = time.time()
# csv_file_path = ''
# dest_table = 'dest_table'
# header = True
# host ="localhost"
# database= "postgres"
# user= "postgres"
# password= "postgres"
# pc = PostgresClient(host, database, user,password)
# conn = pc.connect()  # connect to the database
# pc.copy_csv_to_postgres(csv_file_path, dest_table, header)
# print("--- %s seconds ---" % (time.time() - start_time))
######################################################### Example for load csv direct to postgres with copy function

