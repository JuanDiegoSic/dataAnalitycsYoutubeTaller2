# -*- coding: utf-8 -*-
__author__ = "Hugo Franco, Roberto Arias"
__maintainer__ = "Asignatura: Data Analytics - ETL"
__copyright__ = "Copyright 2021 - Asignatura Big Data"
__version__ = "0.0.1"

try:
    #from pathlib import Path as p
    import pandas as pd
    from datetime import datetime
    import os
    
    
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

#%%
class transform_data(object):
    def __init__(self, path=None,percent=None):
        self.data = None
        self.status = False
        self.extractor = None
   
#%%    
    def set_data(self, data=None, path=None):
        if path is None:
            self.data=data            
        else:
            try:
                self.data=pd.read_csv(path)
            except Exception as exc:
                self.show_error(exc)
 
#%%    
    def drop_columns(self, col_list):
        if len(col_list)>0:
               self.data=self.data.drop(columns=col_list)
               print("columnas borradas")
 
#%%    
    def normalize_trending_date(self, column_name):
        self.data[column_name]=self.data[column_name].apply(lambda date:self.format_date_us_to_latam(str('20'+date)))
 
#%%    
    def format_date_us_to_latam(self, date_str):
        return datetime.strptime(date_str, "%Y.%d.%m").strftime('%d/%m/%Y')
  
#%%   
    def save_data_csv(self, path):
        if self.data is not None:
            self.data.to_csv(path)
 
#%%
    # Control de excepciones
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
             
 #%%  
    def impresionChimba(self, column_name):
        print(self.data[column_name])
 #%%  
    def encode(self,column_name):

        try:
             self.data[column_name] = self.data[column_name].apply(lambda c_name:self.encodeutf8(c_name))
        except Exception as exc:
            self.show_error(exc)
        print(self.data[column_name])
 #%% 
    def encodeutf8(self,column_name):
        return column_name.encode(encoding = 'utf8')
    
 #%%  
    def normalize_published_time(self, column_name, date_column_name, hours_column_name):
        try:             
            self.data[[date_column_name,hours_column_name]] = self.data[column_name].str.split('T', expand=True)
            self.data[date_column_name]=self.data[date_column_name].apply(lambda date:self.format_publish_date_us_to_latam(str(date)))
        except Exception as exc:
            self.show_error(exc)
        print(self.data[column_name], self.data[date_column_name],self.data[hours_column_name] )
 #%%  
    def format_publish_date_us_to_latam(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime('%d/%m/%Y')
        except Exception as exc:
            self.show_error(exc)
            
 #%%          
    def time_diff(self, column_1, column_2):
        self.data['tendence_distance_date'] = abs((self.data[column_1].apply(pd.to_datetime) - self.data[column_2].apply(pd.to_datetime)))    
    
  
    #%% 
    def changeDate(self, date):
        self.data[date] = pd.to_datetime(self.data[date]).dt.date
    #%% 
    def format_date_us_to_latam_new(self, date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").strftime('%d/%m/%Y')
    
    #%% 
    def normalize_simple_Date(self, column_name):
        self.data[column_name]=self.data[column_name].apply(lambda date:self.format_date_us_to_latam_new(str(date)))
        
        
    #%% 
    def structured_data(self,column_name):
        df = self.data[column_name]#.split('|',expand=True)
        print(df)
        js=df.to_json('semistructured_tag_videos.json')        


