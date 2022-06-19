import requests
from datetime import date
import pandas as pd
from decouple import config
import csv
import re
from unicodedata import normalize

#Getting the urls from the config file
url_museos = config('URL_MUSEOS')
url_cines = config('URL_CINES')
url_bibliotecas = config('URL_BIBLIOTECAS')

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
    with open(name, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  
        header = list(map(normalize_string, header))
        rows = [header] + list(reader)  
    with open(name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows) 


#Function to get the csv files
def get_csv(url, name):
    r = requests.get(url)
    with open(name, 'wb') as f:
        f.write(r.content)
    change_header_csv(name)

#Downloading the csv files
def download_data():
    get_csv(url_museos, name_museos)
    get_csv(url_cines, name_cines)
    get_csv(url_bibliotecas, name_bibliotecas)

df_museos = pd.read_csv(name_museos)
df_cines = pd.read_csv(name_cines)
df_bibliotecas = pd.read_csv(name_bibliotecas)