import logging
from decouple import config

URL_DATABASE = config('DATABASE_URL')
URL_MUSEOS = config('URL_MUSEOS')
URL_CINES = config('URL_CINES')
URL_BIBLIOTECAS = config('URL_BIBLIOTECAS')

logging.basicConfig(level=logging.INFO, filename='app.log',encoding='UTF-8', filemode='w', 
                    format='%(asctime)s - %(levelname)s - %(message)s')