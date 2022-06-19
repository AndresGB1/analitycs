import requests
from datetime import date
import pandas as pd
import csv
import re
from decouple import config
from unicodedata import normalize
import sqlalchemy
from sqlalchemy import exc
import logging

#Getting the urls from the config file
url_museos = config('URL_MUSEOS')
url_cines = config('URL_CINES')
url_bibliotecas = config('URL_BIBLIOTECAS')
url_database = config('DATABASE_URL')

#Connect to the database
def connect():
    try:
        engine = sqlalchemy.create_engine(config('DATABASE_URL'))
        engine.connect()
        logging.info(' Connected to the database')
        return engine
    except exc.SQLAlchemyError as e:
        logging.error(' Error al conectar a la base de datos')
        return None

#Function to create the name of the csv files with the date
def create_name(categoria):
    name = categoria + '\\' + date.today().strftime('%Y-%m') + '\\' + categoria + '-' + date.today().strftime('%d-%m-%Y') + '.csv'
    return name

#Creating the names of the csv files 
name_museos = create_name('museos')
name_cines = create_name('cines')
name_bibliotecas = create_name('bibliotecas')

#Function to normalize the string headers
def normalize_string(s):
    s = re.sub(r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1", normalize('NFD', s), 0, re.I)
    return normalize('NFC', s).lower()


#Function to change the headers of the csv files
def change_header_csv(name):
    try:
        with open(name, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  
            header = list(map(normalize_string, header))
            rows = [header] + list(reader)  
        with open(name, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows) 
    except:
        logging.error('Archivo no encontrado')


#Function to get the csv files
def get_csv(url, name):
    try:
        r = requests.get(url)
        with open(name, 'wb') as f:
            f.write(r.content)
        change_header_csv(name)
    except:
        logging.error('Error al descargar el archivo ' + name+'\nUrl:'+url)

#Downloading the csv files
def download_data():
    get_csv(url_museos, name_museos)
    get_csv(url_cines, name_cines)
    get_csv(url_bibliotecas, name_bibliotecas)

download_data()

#Read CSV To dataframe
df_museos = pd.read_csv(name_museos)
df_cines = pd.read_csv(name_cines)
df_bibliotecas = pd.read_csv(name_bibliotecas)

#Creating the DataFrame 1
def changeHeader(df):
    df.rename({'cod_loc': 'cod_localidad', 'idprovincia': 'id_provincia', 'iddepartamento': 'id_departamento', 'cp': 'cod_postal', 'telefono': 'numero_telefono'}, axis=1, inplace=True)
    return df

def normalize_table(df_museos,df_cines,df_bibliotecas):
    headersBiliotecas = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 'nombre', 'cp', 'telefono', 'mail', 'web','domicilio']
    headers = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 'nombre','cp', 'telefono', 'mail', 'web']
    
    new_df_museos = df_museos[headers]
    new_df_cines = df_cines[headers]
    new_df_bibliotecas = df_bibliotecas[headersBiliotecas]

    #Merge the dataframes
    df_normalize = pd.concat([new_df_museos, new_df_cines, new_df_bibliotecas])
    return changeHeader(df_normalize)

table1 = normalize_table(df_museos,df_cines,df_bibliotecas) 

#Creating the DataFrame 2
def register_count_table(df_museos,df_cines,df_bibliotecas):
   
    #Get the dataframe with categoria, provincia and fuente
    new_df_bibliotecas = df_bibliotecas[['categoria', 'provincia', 'fuente']]
    new_df_cines2 = df_cines[['categoria', 'provincia', 'fuente']]
    new_df_museos2 = df_museos[['categoria', 'provincia', 'fuente']]

    #Count the registers by categoria, provincia and fuente of bibliotecas
    categoria_biblioteca = new_df_bibliotecas.groupby(['categoria']).size().reset_index(name='registros_categorias')
    fuente_biblioteca = new_df_bibliotecas.groupby(['fuente']).size().reset_index(name='registros_fuentes')
    provincia_categoria_biblioteca = new_df_bibliotecas.groupby(['provincia', 'categoria']).size().reset_index(name='registros_provincia_categoria')

    #Count the registers by categoria, provincia and fuente of cines
    categoria_cine = new_df_cines2.groupby(['categoria']).size().reset_index(name='registros_categorias')
    fuente_cine = new_df_cines2.groupby(['fuente']).size().reset_index(name='registros_fuentes')
    provincia_categoria_cine = new_df_cines2.groupby(['provincia', 'categoria']).size().reset_index(name='registros_provincia_categoria')

    #Count the registers by categoria, provincia and fuente of museos
    categoria_museo = new_df_museos2.groupby(['categoria']).size().reset_index(name='registros_categorias')
    fuente_museo = new_df_museos2.groupby(['fuente']).size().reset_index(name='registros_fuentes')
    provincia_categoria_museo = new_df_museos2.groupby(['provincia', 'categoria']).size().reset_index(name='registros_provincia_categoria')
    
    #Merge or concat the dataframes
    df_normalize = pd.concat([categoria_biblioteca, fuente_biblioteca, provincia_categoria_biblioteca, categoria_cine, fuente_cine, provincia_categoria_cine, categoria_museo, fuente_museo, provincia_categoria_museo])
    return df_normalize
    
table2 = register_count_table(df_museos,df_cines,df_bibliotecas)

#Creating the DataFrame 3
def info_cine():
   df_cines2 = df_cines[['provincia', 'pantallas', 'butacas', 'espacio_incaa']]
   return df_cines2

table3 = info_cine()


# Creation of tables in the Database
def upload_to_db(df, name):
    try:
        engine = connect()
        if(engine):
            df.to_sql(name, engine, if_exists='append', index=False)
            return df
    except exc.SQLAlchemyError as e:
        logging.error(' could not create table')

upload_to_db(table1,'table1')
upload_to_db(table2,'table2')
upload_to_db(table3,'table3')
