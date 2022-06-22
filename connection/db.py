import sqlalchemy
from sqlalchemy import exc
from config import URL_DATABASE, logging

def connect():
    '''
    Connect to the database
        Returns: 
            engine or None if connection failed
    '''
    engine = sqlalchemy.create_engine(URL_DATABASE)
    try:
        engine.connect()
        logging.info(' Conectado a la base de datos')
        return engine
    except exc.SQLAlchemyError as e:
        logging.error(' Error al conectar a la base de datos')
        return None

def upload_to_db(df, name):
    '''
    Upload the dataframe to the database
        Parameters:
            df: dataframe with the data
            name: name of the table
    '''
    try:
        engine = connect()
        if(engine):
            engine.execute(f'DROP TABLE IF EXISTS {name}')
            df.to_sql(name, engine, if_exists='append', index=False)
            logging.info("Tabla {} subida a la base de datos".format(name))
            return df
    except Exception as e:
        logging.error(' Error: {}'.format(e))