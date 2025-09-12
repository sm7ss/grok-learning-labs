#Importamos librerías
import logging
import polars as pl
from dataclasses import dataclass
from pydantic import BaseModel
from enum import Enum
from typing import List, Type

#Configuracion logging
logging.basicConfig(level=logging.basicConfig, format='%(asctime)s-%(levelname)s-(message)s')
logger = logging.getLogger(__name__)

class EstrategiaRolling(str, Enum): 
    rolling_mean = 'rolling_mean'
    rolling_sum = 'rolling_sum'
    rolling_min = 'rolling_min'
    rolling_max = 'rolling_max'

class EstretegiaEscalado(str, Enum): 
    min_max = 'minmax'
    standard = 'standard'

class EstretegiaAgregaciones(str, Enum): 
    sum = 'sum'
    mean = 'mean'

class RollingDataExpresion: 
    def __init__(self, df_lazy: pl.LazyFrame, model: BaseModel):
        self.df_lazy = df_lazy
        self.config_etl = model.rolling_data
        self.columnas = self.config_etl.feature_columns
        self.window = self.config_etl.window_sizes
        self.lag = self.config_etl.lags
        
        for columna in self.columnas: 
            if columna not in self.df_lazy.collect_schema().keys(): 
                logger.error(f'La columna {columna} no se encuentra en el DataFrame')
                raise ValueError(f'La columna {columna} no se encuentra en el DataFrame')
        
        if len(self.columnas) != len(self.window) != len(self.lag): 
            logger.error('Las longitudes de las listas son diferentes, necesitan ser iguales')
            raise ValueError('Las longitudes de las listas son diferentes, necesitan ser iguales')
    
    def expresiones_rolling(self, estrategia: EstrategiaRolling) -> List[pl.Expr]: 
        try: 
            expresiones = [
            getattr(pl.col(self.columnas[rango]), estrategia)
            (window_size=self.window[rango])
            .shift(self.lag[rango])
            .over(self.columnas[rango])
            .alias(f'{estrategia}_{self.columnas[rango]}')
            for rango in range(len(self.columnas))
            ]
        except AttributeError: 
            logger.error(f'La estrategia {estrategia} no es valida')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error durante la ejecución: {e}')
            raise
        
        return expresiones

class ScalerData: 
    def __init__(self, df: pl.LazyFrame, model: Type[BaseModel]):
        self.df_lazy = df
        self.config_ml = model.ml
    
    def minmax_data(self):
        schema = self.df_lazy.collect_schema()
        for col in self.config_ml.feature_columns: 
            if col not in schema: 
                logger.error(f'La columna {col} no se encuuentra en el DataFrame')
                raise ValueError(f'La columna {col} no se encuuentra en el DataFrame')
            
            if not schema[col].is_numeric(): 
                logger.error(f'La columna {col} no es una columna númerica')
                raise ValueError(f'La columna {col} no es una columna númerica')
        
        minmax_lista = [
            (pl.col(col) - pl.col(col).min()) / (pl.col(col).max() - pl.col(col).min())
            .alias(f'MinMax_{col}')
            for col in self.config_ml.feature_columns
        ]
        return minmax_lista
    
    def agregaciones_data(self): 
        schema = self.df_lazy.collect_schema()
        for col in self.config_ml.aggregations.keys(): 
            if col not in schema: 
                logger.error(f'La columna {col} no se encuuentra en el DataFrame')
                raise ValueError(f'La columna {col} no se encuuentra en el DataFrame')
            
            if not schema[col].is_numeric(): 
                logger.error(f'La columna {col} no es una columna númerica')
                raise ValueError(f'La columna {col} no es una columna númerica')
        
        lista_agregaciones = [
            getattr(pl.col(col), agregacion.value)()
            .alias(f'{agregacion}_{col}')
            for col, agregacion in self.config_ml.aggregations.items()
        ]
        
        return lista_agregaciones
    
    def standard_data(self): 
        schema = self.df_lazy.collect_schema()
        for col in self.config_ml.feature_columns: 
            if col not in schema: 
                logger.error(f'La columna {col} no se encuentra en el DataFrame')
                raise ValueError(f'La columna {col} no se encuentra en el DataFrame')
            
            if not schema[col].is_numeric(): 
                logger.error(f'La columna {col} no es una columna númerica')
                raise ValueError(f'La columna {col} no es una columna númerica')
        
        lista_standard_data = [
            ((pl.col(col)) - pl.col(col).mean()) / (pl.col(col).std())
            .alias(f'Zscore_{col}')
            for col in self.config_ml.feature_columns
        ]
        
        return lista_standard_data

class JoinData: 
    def __init__(self, df: pl.LazyFrame, model: Type[BaseModel]):
        self.config = model.join_data
        self.df_lazy = df
    
    def post_join(self): 
        schema = self.df_lazy.collect_schema()
        for col in self.config.filter_post_join: 
            if col not in schema: 
                logger.error(f'La columna {col} no se encuentra en el DataFrame')
                raise ValueError(f'La columna {col} no se encuentra en el DataFrame')
            
            if not schema[col].is_numeric(): 
                logger.error(f'La columna {col} no es una columna númerica')
                raise ValueError(f'La columna {col} no es una columna númerica')
        
        lista_post_join = [
            pl.when(getattr(pl.col(col), expresion)).then(self.config.valor_then).otherwise(self.config.valor_otherwise)
            .alias(f'Categoria_{col}')
            for col, expresion in self.config.filter_post_join.items()
        ]
        
        return lista_post_join
    
    def join_data(self): 
        #Aqui join
        pass

