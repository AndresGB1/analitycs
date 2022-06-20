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

logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w', 
                    format='%(asctime)s - %(levelname)s - %(message)s')

#Getting the urls from the config file
url_museos = config('URL_MUSEOS')
url_cines = config('URL_CINES')
url_bibliotecas = config('URL_BIBLIOTECAS')
url_database = config('DATABASE_URL')

def connect():
    '''
    Connect to the database
        Returns: 
            engine or None if connection failed
    '''
    engine = sqlalchemy.create_engine(config('DATABASE_URL'))
    try:
        engine.connect()
        logging.info(' Connected to the database')
        return engine
    except exc.SQLAlchemyError as e:
        logging.error(' Error al conectar a la base de datos')
        return None

def create_name(categoria):
    '''
    Create the name of the csv files with the date
        Parameters:
            categoria: string with the name of the category
        Returns:
            name of the csv file'''
    name = categoria + '\\' + date.today().strftime('%Y-%m') + '\\' + categoria + '-' + date.today().strftime('%d-%m-%Y') + '.csv'
    return name

def normalize_string(s):
    '''
    Normalize the string headers
        Parameters:
            s: string with the name of the header
        Returns:
            string with the name of the header
    '''

    s = re.sub(r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1", normalize('NFD', s), 0, re.I)
    return normalize('NFC', s).lower()


#Function to change the headers of the csv files
def change_header_csv(name):
    '''
    Change the headers of the csv files
        Parameters:
            name: string with the name of the csv file
    '''
    try:
        with open(name, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  
            header = list(map(normalize_string, header))
            rows = [header] + list(reader)  
        with open(name, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows) 
            logging.info(' Se cambiaron los headers del archivo ' + name)
    except:
        logging.error('Archivo no encontrado')

def get_csv(url, name):
    '''
    Download the csv file
        Parameters:
            url: string with the url of the csv file
            name: string with the name of the csv file
    '''
    try:
        r = requests.get(url)
        with open(name, 'wb') as f:
            f.write(r.content)
            logging.info(' Se descargó el archivo ' + name)
        change_header_csv(name)
    except:
        logging.error('Error al descargar el archivo ' + name+'\nUrl:'+url)

def download_data():
    '''
    Download the csv files
    '''
    get_csv(url_museos, name_museos)
    get_csv(url_cines, name_cines)
    get_csv(url_bibliotecas, name_bibliotecas)

def changeHeader(df):
    '''
    Change the headers of the dataframe
        Parameters:
            df: dataframe with the data
        Returns:
            dataframe with the data 
    '''

    df.rename({'cod_loc': 'cod_localidad', 'idprovincia': 'id_provincia', 'iddepartamento': 'id_departamento', 'cp': 'cod_postal', 'telefono': 'numero_telefono'}, axis=1, inplace=True)
    return df

def normalize_table(df_museos,df_cines,df_bibliotecas):
    '''
    Create the dataframe with the data of the csv files
        Parameters:
            df_museos: dataframe with the data of the museos
            df_cines: dataframe with the data of the cines
            df_bibliotecas: dataframe with the data of the bibliotecas
        Returns:
            dataframe with the data normalized of the csv files
    '''
    headersBiliotecas = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 'nombre', 'cp', 'telefono', 'mail', 'web','domicilio']
    headers = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 'nombre','cp', 'telefono', 'mail', 'web']

    try:
        new_df_museos = df_museos[headers]
        new_df_cines = df_cines[headers]
        new_df_bibliotecas = df_bibliotecas[headersBiliotecas]
    except:
        logging.error('Error al obtener los headers de los dataframes')
        return None

    #Merge the dataframes
    df_normalize = pd.concat([new_df_museos, new_df_cines, new_df_bibliotecas])
    logging.info(' Se normalizaron los datos, primer dataframe: ' + str(len(df_normalize)))
    return changeHeader(df_normalize)

#Creating the DataFrame 2
def register_count_table(df_museos,df_cines,df_bibliotecas):
    '''
    Create the dataframe with the data of the csv files
        Parameters:
            df_museos: dataframe with the data of the museos
            df_cines: dataframe with the data of the cines
            df_bibliotecas: dataframe with the data of the bibliotecas
        Returns:
            dataframe with the data normalized of the csv files
    '''   
    #Get the dataframe with categoria, provincia and fuente
    try:
        new_df_bibliotecas = df_bibliotecas[['categoria', 'provincia', 'fuente']]
        new_df_cines2 = df_cines[['categoria', 'provincia', 'fuente']]
        new_df_museos2 = df_museos[['categoria', 'provincia', 'fuente']]
    except:
        logging.error('Error al obtener los headers de los dataframes')
        return None

    try:
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
        logging.info(' Se normalizaron los datos, segundo dataframe: ' + str(len(df_normalize)))
    except:
        logging.error('Error al normalizar los datos')
        return None
    return df_normalize

#Creating the DataFrame 3
def info_cine(df_cines):
    '''
    Create the dataframe with the data of the cines csv file
        Parameters:
            df_cines: dataframe with the data of the cines
        Returns:
            dataframe with the data normalized of the cines csv file
    '''
    try:
        df_cines2 = df_cines[['provincia', 'pantallas', 'butacas', 'espacio_incaa']]
    except:
        logging.error('Error al obtener los headers de los dataframes')
        return None
    logging.info(' Se normalizaron los datos, tercer dataframe: ' + str(len(df_cines2)))
    return df_cines2

# Creation of tables in the Database
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
            logging.info("Table {} uploaded to database".format(name))
            return df
    except exc.SQLAlchemyError as e:
        logging.error(' Error: {}'.format(e))

#Creating the names of the csv files 
name_museos = create_name('museos')
name_cines = create_name('cines')
name_bibliotecas = create_name('bibliotecas')

#Download CSV files
download_data()

#Read CSV To dataframe
df_museos = pd.read_csv(name_museos)
df_cines = pd.read_csv(name_cines)
df_bibliotecas = pd.read_csv(name_bibliotecas)

#Process the data
table1 = normalize_table(df_museos,df_cines,df_bibliotecas) 
table2 = register_count_table(df_museos,df_cines,df_bibliotecas)
table3 = info_cine(df_cines)

#Upload the data to the database
upload_to_db(table1,'table1')
upload_to_db(table2,'table2')
upload_to_db(table3,'table3')

