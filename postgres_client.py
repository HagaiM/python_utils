#!/usr/bin/python
from pandas import DataFrame
import pandas as pd
from sqlalchemy import create_engine


def df_to_postgresql_bulk(df,dest_table,host, user, passwd, db, type = 'append'):
    df = df[1:]
    sh = df.shape[0]
    bulk_number = 50000
    reminder = sh % bulk_number
    engine = create_engine('postgresql+psycopg2://'+user+':'+passwd+'@'+host+'/'+db,
    use_batch_mode=True)
    num = int((sh-reminder)/bulk_number)
    fromrow = 0
    torow = bulk_number
    for i in range(0,num+1):
        with engine.connect() as conn, conn.begin():
            print("start iteration {}".format(i))
            df[fromrow:torow].to_sql(dest_table, conn, if_exists=type,index=False)
            print("end iteration {}".format(i))
        if i == num:
            fromrow += reminder
            torow += reminder
        fromrow += bulk_number
        torow += bulk_number


def postgresql_to_df(host, user, passwd, db,arg_query):
    alchemyEngine = create_engine('postgresql+psycopg2://'+user+':'+passwd+'@'+host+'/'+db)

    # Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect();
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

def run_query_postgresql(host, user, passwd, db, query):

    alchemyEngine = create_engine('postgresql+psycopg2://'+user+':'+passwd+'@'+host+'/'+db)
    dbConnection = alchemyEngine.connect();
    if "truncate" in query.lower() or "update" in query.lower() or "insert" in query.lower():
        query = dbConnection.execute(query)  ##rows_count
    else:
        query = dbConnection.execute(query)
        rows = query.fetchall()
        if rows != None:
            return rows



