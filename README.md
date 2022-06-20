# Challenge Data Analytics - Python - Alkemy
### INSTRUCCIONES:
### Entorno Virtual e instalación de dependencias:
- [Link guía para instalar virtualenv windows](https://help.dreamhost.com/hc/es/articles/115000695551-Instalar-y-usar-virtualenv-con-Python-3)
- [Link guía para instalar virtualenv linux-mac](https://rukbottoland.com/blog/tutorial-de-python-virtualenv/)

-> Ingresar a la carpeta donde se encuentra el archivo app.py
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
      
### Configurar la conexión a la base de datos.
- Base de datos alojada en la nube, (postgresql)

- Archivo "[Archivo.env](https://drive.google.com/file/d/1fPRdCx6onmjJzPXaU-jrANLVP8OQXLty/view?usp=sharing)" con la url de la configuración, IMPORTANTE descargar el archivo en la carpeta y renombrar de "Archivo.env" a ".env"

- Las tablas de la BD se generan en python

