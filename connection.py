import sqlalchemy
from sqlalchemy import exc
from decouple import config

#Connect to the database
def connect():
    try:
        engine = sqlalchemy.create_engine(config('DATABASE_URL'))
        engine.connect()
        print('Connected to the database')
        return engine
    except exc.SQLAlchemyError as e:
        print(e.code)
        print('Error al conectar a la base de datos')
        return None

x = connect()