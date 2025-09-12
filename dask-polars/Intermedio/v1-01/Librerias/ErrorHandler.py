#Importacion de librerías importantes
import logging
import polars as pl
from typing import List

#Configuracion del logging
logging.basicConfig(level='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class EdgeCases: 
    @classmethod
    def parametro_lista(self, lista: pl.LazyFrame) -> None: 
        if isinstance(lista, list): 
            logger.error('No se puede pasar una lista de DataFrames')
            raise ValueError('No se puede pasar una lista de DataFrames')
    
    @classmethod
    def longitud_lista_para_join(self, lista: List[pl.LazyFrame]) -> None: 
        not self.parametro_lista(lista=lista)
        if len(lista) != 2: 
            logger.error('La lista debe de tener dos lazyframes')
            raise ValueError('La lista debe de tener dos lazyframes')
    
    @staticmethod
    def columna_no_existente_lista(columnas: List[str], schema: pl.Schema) -> None: 
        for columna in columnas: 
            if columna not in schema: 
                logger.error(f'La columna {columna} no se encuentra en el DataFrame')
                raise ValueError(f'La columna {columna} no se encuentra en el DataFrame')
    
    @staticmethod
    def longitud_de_lista_diferente(columnas: List[str], window: List[int], lag: List[int]) -> None: 
        if len(columnas) != len(window) != len(lag): 
            logger.error('Las longitudes de las listas son diferentes, necesitan ser iguales')
            raise ValueError('Las longitudes de las listas son diferentes, necesitan ser iguales')
    
    @staticmethod
    def columna_no_existente_schema(col: str, schema: pl.Schema) -> None: 
        if col not in schema: 
            logger.error(f'La columna {col} no se encuuentra en el DataFrame')
            raise ValueError(f'La columna {col} no se encuuentra en el DataFrame')
    
    @staticmethod
    def columna_no_numerica(col: str, schema: pl.Schema) -> None: 
        if not schema[col].is_numeric(): 
            logger.error(f'La columna {col} no es una columna númerica')
            raise ValueError(f'La columna {col} no es una columna númerica')

