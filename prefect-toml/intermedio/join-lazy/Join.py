#Importacion de librerías necesarias 
import polars as pl 
import logging
import tomli
from typing import Dict, Any
from pathlib import Path 
from prefect import flow, task
from dataclasses import dataclass

#Configuracion del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TOMLAttributes: 
    df_1: str
    df_2: str
    archivo_salida: str
    join_columna: str

class ReadTOML: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
    
    def read_toml(self) -> pl.LazyFrame: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        
        if not self.archivo.suffix == '.toml': 
            logger.error(f'El archivo {self.archivo.name} no es un archivo .toml')
            raise ValueError(f'El archivo {self.archivo.name} no es un archivo .toml')
        
        try: 
            with open(self.archivo, 'rb') as file: 
                read = file.read()
                
                if not read.strip(): 
                    logger.error(f'El archivo {self.archivo.name} está vacío')
                    raise ValueError(f'El archivo {self.archivo.name} está vacío')
                
                lectura_toml = tomli.loads(read.decode('utf-8'))
            logger.info(f'Se leyó correctamente el archivo {self.archivo.name}')
            return lectura_toml
        except tomli.TOMLDecodeError: 
            logger.error(f'El archivo {self.archivo.name} está corrupto')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')
            raise

class DataProcessor: 
    def __init__(self, diccionario: Dict[str, Any]):
        self.diccionario = diccionario
        try: 
            self.atributos = TOMLAttributes(
                        df_1=self.diccionario['data']['input_clientes'],
                        df_2=self.diccionario['data']['input_pedidos'],
                        archivo_salida=self.diccionario['data']['output_path'],
                        join_columna=self.diccionario['data']['join_column']
                        )
            logger.info('Atributos del TOML cargados correctamente')
            raise
        except KeyError as e: 
            logger.error(f'Ocurrio un error al llamar las llaves: {e}')
            raise
    
    def obtencion_lazy(self) -> tuple:
        if not self.diccionario: 
            logger.error('El diccionario está vacío')
            raise ValueError('El diccionario está vacío')
        
        df_lazy_1 = pl.scan_csv(self.atributos.df_1)
        df_lazy_2 = pl.scan_csv(self.atributos.df_2)
        
        logger.info('Se obtuvieron los DataFrames')
        return df_lazy_1, df_lazy_2
    
    def join_lazy(self, tupla: tuple) -> pl.LazyFrame: 
        if not tupla: 
            logger.error('La tupla está vacía')
            raise ValueError('La tupla está vacía')
        
        df_lazy = tupla[0].join(tupla[1], on=self.atributos.join_columna, how='inner')
        logger.info(f'Se juntaron ambos DataFrames en la columna {self.atributos.join_columna}')
        return df_lazy
    
    def guardar(self, df: pl.lazyframe): 
        df_lazy = df.collect(streaming=True)
        df_lazy.write_parquet(self.atributos.archivo_salida)
        logger.info(f'La coleccion del DataFrame se guardo correctamente en el archivo {self.atributos.archivo_salida}')

class Pipeline: 
    def __init__(self, archivo: str):
        self.config =  ReadTOML(archivo=archivo)
        self.read = self.config.read_toml()
        self.procesador = DataProcessor(diccionario=self.read)
    
    @task(retries=3, retry_delay_seconds=5)
    def extract(self) -> tuple: 
        return self.procesador.obtencion_lazy()
    
    @task
    def join(self, tupla: tuple) -> pl.LazyFrame: 
        return self.procesador.join_lazy(tupla=tupla)
    
    @task
    def load(self, df: pl.LazyFrame): 
        return self.procesador.guardar(df=df)
    
    @flow(name='Pipeline para Join LazyFrame')
    def pipeline(self): 
        data = self.extract()
        df = self.join(tupla=data)
        self.load(df=df)
