# -*- coding: utf-8 -*-
"""
@author: Hugo Franco, Roberto Arias
"""
try:
    import re
    import pandas as pd
    import os, sys
    from pathlib import Path as p


except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

dir_root = p(__file__).parents[1]
sys.path.append(str(p(dir_root) /'source' / 'clases'))

from cls_extract_data_mf import extract_data_mf as data_extractor
from cls_transform_data import transform_data as data_transformer



#%%
''' Crear una instancia de la clase (objeto) '''
extractor = data_extractor()
extractor.path = dir_root

transform=data_transformer()


#%%
'''Autenticación en el api de Kaggle'''
path_auth = str(p(extractor.path) / 'kaggle')
extractor.set_kaggle_api(path_auth)

#%%
def init_extraction():
    '''Autenticación en el api de Kaggle'''
    path_auth = str(p(extractor.path) / 'kaggle')
    extractor.set_kaggle_api(path_auth)
    
    ''' Listar datasets'''
    extractor.list_dataset_kaggle('youtube')
    extractor.show_kaggle_datasets()
    
    ''' Descargar todos los archivos de un dataset alojado kaggle '''

    path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
    extractor.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')
    
    ''' Listar archivos según el tipo'''
    path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
    extractor.get_lst_files(path_data,'csv')
    print('{}Instancia kaggle - archivos formato {}:'.format(os.linesep,'csv'))
    extractor.muestra_archivos()
    
    ''' Ejemplo: Cargar datos "Canada" a memoria, i.e. CAvideos.csv'''
    extractor.get_data_csv(extractor.lst_files[0])
    extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)
    
    ''' Mostrar datos para información'''
    print(extractor.data)


def init_transform():
    transform.set_data(extractor.data.copy())
    print("{}Nombres de las columnas:".format(os.linesep))
    [print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]
    
    transform.drop_columns(['thumbnail_link', 'comments_disabled', 'ratings_disabled', 'video_error_or_removed','description'])
    print("{}Nombres de las columnas:".format(os.linesep))
    [print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]
    
    transform.normalize_trending_date('trending_date')
    
    transform.encode('title')
    transform.encode('channel_title')
    transform.encode('tags')
    
    output_data = str(p(extractor.path) / 'output' / 'youtube-transformed.csv')
    print (output_data)
    transform.save_data_csv(output_data)
    
    transform.normalize_published_time('publish_time', 'date_publish_time', 'hours_publish_time')
    
    transform.impresionChimba("trending_date")
    transform.impresionChimba("date_publish_time")
    
    transform.time_diff('date_publish_time', 'trending_date')
    transform.impresionChimba("tendence_distance_date")
    
    transform.impresionChimba("publish_time")
    transform.impresionChimba("date_publish_time")
    transform.impresionChimba("hours_publish_time")
    
    transform.changeDate('publish_time')
    transform.impresionChimba("publish_time")
    transform.normalize_simple_Date('publish_time')
    transform.impresionChimba("publish_time")
    
    transform.impresionChimba("category_id")
    
    try:
        data_frame = list()
        temp_data_frame = pd.DataFrame()
        for csv in extractor.lst_files:
            extractor.get_data_csv(csv)
            file_name = csv.split(path_data, 1)[1]
            extractor.data['country'] = re.search('\\\(.*).csv',file_name).group(1)[:2]
            data_frame.append(extractor.data)
            temp_data_frame = pd.concat(data_frame)
        
        extractor.data = temp_data_frame
        extractor.data.reset_index(drop=True, inplace=True)
        extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)
        #write('Extraccion',2)
        #step+=1
        #transformation(0)
    except Exception as exc:
        extractor.show_error(exc)
            
    print(extractor.data)
    
    transform.structured_data("tags")
#%%
''' Listar datasets'''
extractor.list_dataset_kaggle('youtube')
extractor.show_kaggle_datasets()

#%%
''' Descargar todos los archivos de un dataset alojado kaggle '''

path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
extractor.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')


#%%
''' Listar archivos según el tipo'''
path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
extractor.get_lst_files(path_data,'csv')
print('{}Instancia kaggle - archivos formato {}:'.format(os.linesep,'csv'))
extractor.muestra_archivos()

#%%
''' Ejemplo: Cargar datos "Canada" a memoria, i.e. CAvideos.csv'''
extractor.get_data_csv(extractor.lst_files[0])
extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)


#%%
''' Mostrar datos para información'''
print(extractor.data)

#%%

transform.set_data(extractor.data.copy())
print("{}Nombres de las columnas:".format(os.linesep))
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]


#%% 
transform.drop_columns(['thumbnail_link', 'comments_disabled', 'ratings_disabled', 'video_error_or_removed','description'])
print("{}Nombres de las columnas:".format(os.linesep))
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]

#%%
transform.normalize_trending_date('trending_date')


#%%


#%%
transform.encode('title')
transform.encode('channel_title')
transform.encode('tags')
#%%
output_data = str(p(extractor.path) / 'output' / 'youtube-transformed.csv')
print (output_data)
transform.save_data_csv(output_data)

#%%
transform.normalize_published_time('publish_time', 'date_publish_time', 'hours_publish_time')
#%%
transform.impresionChimba("trending_date")
transform.impresionChimba("date_publish_time")


#%%
transform.time_diff('date_publish_time', 'trending_date')
transform.impresionChimba("tendence_distance_date")


#%%
transform.impresionChimba("publish_time")
transform.impresionChimba("date_publish_time")
transform.impresionChimba("hours_publish_time")
#%%
transform.changeDate('publish_time')
transform.impresionChimba("publish_time")
transform.normalize_simple_Date('publish_time')
transform.impresionChimba("publish_time")


#%%
transform.impresionChimba("category_id")


#%%

try:
    data_frame = list()
    temp_data_frame = pd.DataFrame()
    for csv in extractor.lst_files:
        extractor.get_data_csv(csv)
        file_name = csv.split(path_data, 1)[1]
        extractor.data['country'] = re.search('\\\(.*).csv',file_name).group(1)[:2]
        data_frame.append(extractor.data)
        temp_data_frame = pd.concat(data_frame)
    
    extractor.data = temp_data_frame
    extractor.data.reset_index(drop=True, inplace=True)
    extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)
    #write('Extraccion',2)
    #step+=1
    #transformation(0)
except Exception as exc:
    extractor.show_error(exc)
        
print(extractor.data)
#%%

transform.structured_data("tags")
#%%
#init_extraction()
#init_transform()

#%%
def show_error(self,ex):
        '''
        Captura el tipo de error, su description y localización.

        Parameters
        ----------
        ex : Object
            Exception generada por el sistema.

        Returns
        -------
        None.

        '''
        trace = []
        tb = ex.__traceback__
        while tb is not None:
            trace.append({
                          "filename": tb.tb_frame.f_code.co_filename,
                          "name": tb.tb_frame.f_code.co_name,
                          "lineno": tb.tb_lineno
                          })
            
            tb = tb.tb_next
            
        print('{}Something went wrong:'.format(os.linesep))
        print('---type:{}'.format(str(type(ex).__name__)))
        print('---message:{}'.format(str(type(ex))))
        print('---trace:{}'.format(str(trace)))

