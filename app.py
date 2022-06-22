import requests
from datetime import date
import pandas as pd
import csv
import re
from unicodedata import normalize
from connection.db import upload_to_db
import os
from config import URL_CINES, URL_MUSEOS, URL_BIBLIOTECAS, logging

def create_path(categoria):
    '''
    Create the name of the csv files with the date and the folders
        Parameters:
            categoria: string with the name of the category
        Returns:
            name of the csv file'''
    try:
        name =  categoria + '-' + date.today().strftime('%d-%m-%Y') + '.csv'
        path = categoria+'/'+ date.today().strftime('%Y-%m') 
        if not os.path.exists(path):
            os.makedirs(path)
        return path +'/'+ name
    except Exception as e:
        logging.error('Error al crear el path')
        return None
    
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

def change_header_csv(name):
    '''
    Change the headers of the csv files
        Parameters:
            name: string with the name of the csv file
    '''
    try:
        with open(name, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  
            header = list(map(normalize_string, header))
            rows = [header] + list(reader)  
        with open(name, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows) 
    except Exception as e:
        logging.error('Archivo ' + name + ' no encontrado')

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
    except Exception as e:
        print(e)    
        logging.error('Error al descargar el archivo ' + name+'\nUrl:'+url)   

def change_header_df(df):
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
    logging.info(' Previsualización del dataframe:\n ' + str(change_header_df(df_normalize)))
    return change_header_df(df_normalize)

def arr_provincia_categoria(df):
    '''
    Create the array with the provincia and the categoria
        Parameters:
            df: dataframe with the data
        Returns:
            array with the provincia and the categoria
    '''
    try:
        new_df = []
        for x in range(len(df.index.values)):
            new_df.append(df.index.values[x][0]+'_'+df.index.values[x][1])
        return new_df
    except:
        logging.error('Error al crear el array')
        return None

def concat_df_2(df,tipo_registro,valor_registro,numero_registros):
    return pd.concat([df, pd.DataFrame({'tipo_registro':tipo_registro, 'valor_registro': valor_registro, 'numero_registros': numero_registros})], ignore_index=True )   

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
    try:
        new_df_bibliotecas = df_bibliotecas[['categoria', 'provincia', 'fuente']]
        new_df_cines2 = df_cines[['categoria', 'provincia', 'fuente']]
        new_df_museos2 = df_museos[['categoria', 'provincia', 'fuente']]
    except:
        logging.error('Error al obtener los headers de los dataframes')
        return None

    try:
        header = ['tipo_registro','valor_registro','numero_registros']
        df = pd.DataFrame(columns=header)

        #Count the registers by categoria, provincia and fuente of bibliotecas
        categoria_biblioteca = new_df_bibliotecas.groupby(['categoria']).size()
        fuente_biblioteca = new_df_bibliotecas.groupby(['fuente']).size()
        provincia_categoria_biblioteca = new_df_bibliotecas.groupby(['provincia', 'categoria']).size()
        v_provincia_categoria_biblioteca = arr_provincia_categoria(provincia_categoria_biblioteca)
        
        #Count the registers by categoria, provincia and fuente of cines
        categoria_cine = new_df_cines2.groupby(['categoria']).size()
        fuente_cine = new_df_cines2.groupby(['fuente']).size()
        provincia_categoria_cine = new_df_cines2.groupby(['provincia', 'categoria']).size()
        v_provincia_categoria_cine = arr_provincia_categoria(provincia_categoria_cine)

        #Count the registers by categoria, provincia and fuente of museos
        categoria_museo = new_df_museos2.groupby(['categoria']).size()
        fuente_museo = new_df_museos2.groupby(['fuente']).size()
        provincia_categoria_museo = new_df_museos2.groupby(['provincia', 'categoria']).size()
        v_provincia_categoria_museo = arr_provincia_categoria(provincia_categoria_museo)
        
        #Merge or concat the dataframes
        df = concat_df_2(df,'categoria',categoria_biblioteca.index.values,categoria_biblioteca.values)
        df = concat_df_2(df,'fuente',fuente_biblioteca.index.values,fuente_biblioteca.values)
        df = concat_df_2(df,'provincia_categoria',v_provincia_categoria_biblioteca,provincia_categoria_biblioteca.values)
        df = concat_df_2(df,'categoria',categoria_cine.index.values,categoria_cine.values)
        df = concat_df_2(df,'fuente',fuente_cine.index.values,fuente_cine.values)
        df = concat_df_2(df,'provincia_categoria',v_provincia_categoria_cine,provincia_categoria_cine.values)
        df = concat_df_2(df,'categoria',categoria_museo.index.values,categoria_museo.values)
        df = concat_df_2(df,'fuente',fuente_museo.index.values,fuente_museo.values)        
        df = concat_df_2(df,'provincia_categoria',v_provincia_categoria_museo,provincia_categoria_museo.values)

        logging.info(' Se normalizaron los datos, segundo dataframe: ' + str(len(df)))
        logging.info(' Previsualización del dataframe:\n ' + str(df))
    except:
        logging.error('Error al normalizar los datos')
        return None
    return df

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
    logging.info(' Previsualización del dataframe:\n ' + str(df_cines2))
    return df_cines2

#Creating the path of the csv files 
name_museos = create_path('museos')
name_cines = create_path('cines')
name_bibliotecas = create_path('bibliotecas')

#Download CSV files
get_csv(URL_MUSEOS, name_museos)
get_csv(URL_CINES, name_cines)
get_csv(URL_BIBLIOTECAS, name_bibliotecas)

#Read CSV To dataframe
df_museos = pd.read_csv(name_museos)
df_cines = pd.read_csv(name_cines)
df_bibliotecas = pd.read_csv(name_bibliotecas)

#Process the data
table1 = normalize_table(df_museos,df_cines,df_bibliotecas) 
table2 = register_count_table(df_museos,df_cines,df_bibliotecas)
table3 = info_cine(df_cines)

#Upload the data to the database
upload_to_db(table1, 'table1')
upload_to_db(table2, 'table2')
upload_to_db(table3, 'table3')