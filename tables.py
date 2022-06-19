import pandas as pd
from .app import df_museos, df_cines, df_bibliotecas
import sqlalchemy

def changeHeader(df):
    df.rename({'cod_loc': 'cod_localidad', 'idprovincia': 'id_provincia', 'iddepartamento': 'id_departamento', 'cp': 'cod_postal', 'telefono': 'numero_telefono'}, axis=1, inplace=True)
    return df

#Normalizar la información creando una unica tabla
def normalize_table(df_museos, df_cines, df_bibliotecas):   
    headersBiliotecas = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 'nombre', 'cp', 'telefono', 'mail', 'web','domicilio']
    headers = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 'nombre','cp', 'telefono', 'mail', 'web']
    
    df_museos = df_museos[headers]
    df_cines = df_cines[headers]
    df_bibliotecas = df_bibliotecas[headersBiliotecas]

    #Merge the dataframes
    df_normalize = pd.concat([df_museos, df_cines, df_bibliotecas])
    return changeHeader(df_normalize)

#Procesar la información de museos cines y bibliotecas
#para obtener la cantidad  de registros por categoria provincia y fuente
def register_count_table():
    
    #Get the dataframe with categoria, provincia and fuente
    df_bibliotecas2 = df_bibliotecas[['categoria', 'provincia', 'fuente']]
    df_cines2 = df_cines[['categoria', 'provincia', 'fuente']]
    df_museos2 = df_museos[['categoria', 'provincia', 'fuente']]

    #Count the registers by categoria, provincia and fuente of bibliotecas
    categoria_biblioteca = df_bibliotecas2.groupby(['categoria']).size().reset_index(name='registros_categorias')
    fuente_biblioteca = df_bibliotecas2.groupby(['fuente']).size().reset_index(name='registros_fuentes')
    provincia_categoria_biblioteca = df_bibliotecas2.groupby(['provincia', 'categoria']).size().reset_index(name='registros_provincia_categoria')

    #Count categories  from cines
    categoria_cine = df_cines2.groupby(['categoria']).size().reset_index(name='registros_categorias')
    fuente_cine = df_cines2.groupby(['fuente']).size().reset_index(name='registros_fuentes')
    provincia_categoria_cine = df_cines2.groupby(['provincia', 'categoria']).size().reset_index(name='registros_provincia_categoria')

    #Count categories  from museos
    categoria_museo = df_museos2.groupby(['categoria']).size().reset_index(name='registros_categorias')
    fuente_museo = df_museos2.groupby(['fuente']).size().reset_index(name='registros_fuentes')
    provincia_categoria_museo = df_museos2.groupby(['provincia', 'categoria']).size().reset_index(name='registros_provincia_categoria')


    #Merge the dataframes
    df_normalize = pd.concat([categoria_biblioteca, fuente_biblioteca, provincia_categoria_biblioteca, categoria_cine, fuente_cine, provincia_categoria_cine, categoria_museo, fuente_museo, provincia_categoria_museo])

    #Export the dataframe to a csv file
    df_normalize.to_csv('cantidad_categorias.csv', index=False)
    return df_normalize


#Procesar la información de cines 
def info_cine():
   df_cines2 = df_cines[['provincia', 'pantallas', 'butacas', 'espacio_incaa']]
   return df_cines2
def connect():
        engine = sqlalchemy.create_engine(config('DATABASE_URL'))
        try:
            connect().begin()
        except:
            Exception('Error al conectar a la base de datos')
            return False
        return engine
#Función para subir la información a la base de datos
def upload_to_db(df):
    try:
        engine = connect()
        df.to_sql('cines', engine, if_exists='append', index=False)
        return True
    except Exception as e:
        print(e)
        return False

