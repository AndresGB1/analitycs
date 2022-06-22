# Challenge Data Analytics - Python - Alkemy
### Introducción
- Challenge de Data Analytics con Python!
- Base de datos alojada en la nube, (postgresql)
- Las tablas de la BD se generan en python
### INSTRUCCIONES entorno y archivo .env:
### Configurar la conexión a la base de datos.

- Archivo "[Archivo.env](https://drive.google.com/file/d/17G8r-z5-rqG7qFj8n5C8hWnihdtqQwGb/view?usp=sharing)" con la url de la configuración, IMPORTANTE descargar el archivo en la carpeta y renombrar de "Archivo.env" a ".env"



### Entorno Virtual e instalación de dependencias:
- [Link guía para instalar virtualenv windows](https://sectorgeek.com/instalar-python-pip-y-virtualenv-en-windows-10/)
- [Link guía para instalar virtualenv linux-mac](https://rukbottoland.com/blog/tutorial-de-python-virtualenv/)

-> Ingresar a la carpeta donde se encuentra el archivo app.py 
-> abrir el cmd, shell terminal etc... en la carpeta y (copy paste)

- Windows:


      #Generar entorno virtual   
      python3 -m virtualenv venv
      #Activar entorno
      venv\Scripts\activate
      #Instalación de dependencias
      pip install requests
      pip install pandas 
      pip install python-decouple 
      pip install SQLAlchemy
      pip install psycopg2-binary
      #Correr projecto
      python app.py

- Linux: 
      
      #Generar entorno virtual     
      virtualenv venv
      #Activar entorno
      source venv/bin/activate 
      #Instalación de dependencias
      pip install requests
      pip install pandas 
      pip install python-decouple 
      pip install SQLAlchemy
      pip install psycopg2-binary
      #Correr projecto
      python app.py
      
 ### Extra
 Existe un archivo app2.ipynb en el que se podrá ver el código y las tablas generadas 
