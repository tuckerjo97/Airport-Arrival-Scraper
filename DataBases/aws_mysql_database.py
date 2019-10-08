import sqlalchemy
import pandas as pd
import traceback

def downloading(query, **kwargs):
    #connecting to mysql database via the previously declared
    #credentials

    user=kwargs['user']
    password=kwargs['password']
    host=kwargs['host']
    database=kwargs['db']

    #opens a cursor to write sql code through
    #using pandas.to_sql method read here for documentation https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html

    #error handling
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(
        user,
        password,
        host,
        database))
    try:
        raw_data = pd.read_sql(query, con=database_connection)
        print("data downloaded from server")
        database_connection.dispose()
        return raw_data
    except:
        database_connection.dispose()
        traceback.print_exc(limit=1)

# upload function
def uploading(upload_data, **kwargs):

    user=kwargs['user']
    password=kwargs['password']
    host=kwargs['host']
    database=kwargs['db']
    table=kwargs['table']

    #opens a cursor to write sql code through
    #using pandas.to_sql method read here for documentation https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html

    #error handling
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(
        user,
        password,
        host,
        database))
    try:
        upload_data.to_sql(
                name=table,
                con=database_connection,
                if_exists='append',
                index=False,
                chunksize=3000,
                method='multi'
                )
        print("data uploaded to server")
        database_connection.dispose()
    except:
        database_connection.dispose()
        traceback.print_exc(limit=1)
