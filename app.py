import requests
from datetime import date
import pandas as pd
from decouple import config


url_museos = config('URL_MUSEOS')
url_cines = config('URL_CINES')
url_bibliotecas = config('URL_BIBLIOTECAS')

print(url_bibliotecas)
def create_name(categoria):
    name = categoria + '\\' + date.today().strftime('%Y-%m') + '\\' + categoria + '-' + date.today().strftime('%d-%m-%Y') + '.csv'
    return name


name_museos = create_name('museos')
name_cines = create_name('cines')
name_bibliotecas = create_name('bibliotecas')


def get_csv(url, name):
    r = requests.get(url)
    with open(name, 'wb') as f:
        f.write(r.content)


def download_data():
    get_csv(url_museos, name_museos)
    get_csv(url_cines, name_cines)
    get_csv(url_bibliotecas, name_bibliotecas)
