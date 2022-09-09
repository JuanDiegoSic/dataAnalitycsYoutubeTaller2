# -*- coding: utf-8 -*-
"""
@author: Hugo Franco, Roberto Arias.
"""
try:
    import os
    from pathlib import Path as p
    import sys
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))


dir_root = p(__file__).parents[1]
sys.path.append(str(p(dir_root) /'source' / 'clases'))

from cls_extract_data_mf import extract_data_mf as get_data
from cls_azure_blob_storage import AzureBlob_container_bs_ops as new_container


#%%
''' 
Asignar valores para cada atributo de la clase AzureBlob_container_bs_ops

'''

# Cadena de conexión a Azure Storage Blob
cnx = "DefaultEndpointsProtocol=https;AccountName=analitycs2022;AccountKey=sCzxNesKqNeGKfcU5ZnnS1PF4TJgHf0gGofaIMMxY33cSaixhZIFj9MUNtRaSk5QMPNA6MXsJfU9+AStzcSl/A==;EndpointSuffix=core.windows.net"

'''
El directorio de carga de archivos al datalake es el mismo de salida de
la transformación
'''

dpath = os.path.abspath(os.path.join(__file__, '..','..'))
folder_upload = str(p(dpath) / 'output' )
cmd_folder = folder_upload

def init_load():
    try:
        
        ''' Asignar valores a los atributos de la clase en la instancia '''
        loader_blob = new_container(cnx = cnx)
        loader_blob.set_connection()
    
        ''' Listar todos los contenedores de una cuenta '''
        loader_blob.list_container(Show=True)
        
        ''' Crear un nuevo contenedor '''
        loader_blob.create_container('contenedoryoutube')
    
        ''' Listar todos los blobs de un contenedor '''
        loader_blob.list_files_container('contenedoryoutube',Show=True)
    
        ''' Listar todos los blobs de un contenedor '''
        loader_blob.list_files_container('contenedoryoutube',Show=True)
    
        ''' Eliminar un contenedor y sus contenido '''
        loader_blob.delete_container('contenedoryoutube')
    
        ''' Inicializar la carga de datos '''
        extractor = get_data()
    
        ''' Lista los archivos descargados '''
        extractor.get_lst_files(folder_upload,'csv')

        print('{}Instancia CSV:'.format(os.linesep))
        extractor.muestra_archivos()
    
        # Cargar el archivo transformado al datalake de Azure (BLOB)
        loader_blob.upload_file('contenedoryoutube',folder_upload,extractor.lst_files[0],'csv')
    except Exception as exc:
        extractor.show_error(exc)

#%%
''' Asignar valores a los atributos de la clase en la instancia '''
loader_blob = new_container(cnx = cnx)
loader_blob.set_connection()

#%%
''' Listar todos los contenedores de una cuenta '''
loader_blob.list_container(Show=True)


#%%
''' Crear un nuevo contenedor '''
loader_blob.create_container('contenedoryoutube')


#%%
''' Listar todos los blobs de un contenedor '''
loader_blob.list_files_container('contenedoryoutube',Show=True)

#%%
''' Eliminar un contenedor y sus contenido '''
loader_blob.delete_container('contenedoryoutube')

#%%
''' Inicializar la carga de datos '''
extractor = get_data()

#%%
''' Lista los archivos descargados '''
extractor.get_lst_files(folder_upload,'csv')

print('{}Instancia CSV:'.format(os.linesep))
extractor.muestra_archivos()
#%%

# Cargar el archivo transformado al datalake de Azure (BLOB)
loader_blob.upload_file('contenedoryoutube',folder_upload,extractor.lst_files[0],'csv')

#%%
#init_load()