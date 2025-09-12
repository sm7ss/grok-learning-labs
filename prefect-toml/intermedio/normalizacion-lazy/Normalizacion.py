#Importacion de librerías necesarias 
import tomli
import logging
import polars as pl  
from prefect import flow, task 
from pathlib import Path
from typing import Dict, Any, Union

#configuarcion del logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-(levelname)s-(message)s')
logger = logging.getLogger(__name__)

class OpenToml: 
    def __init__(self, archivo: str) -> None:
        self.archivo = Path(archivo)
    
    def read_toml(self) -> Dict[str, Any]: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        
        try: 
            with open(self.archivo, 'rb') as file: 
                read = file.read()
                
                if not read.strip(): 
                    logger.error(f'El archivo {self.archivo.name} está vacío')
                    raise ValueError(f'El archivo {self.archivo.name} está vacío')
                
                lectura_toml = tomli.loads(read.decode('utf-8'))
            logger.info(f'El archivo {self.archivo} se leyó correctamente')
            return lectura_toml
        except tomli.TOMLDecodeError as e: 
            logger.error(f'El archivo {self.archivo.name} esta corrupto')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')
            raise

class DataProcessor: 
    def __init__(self, diccionario: Dict[str, Any]) -> None:
        self.diccionario = diccionario['data']
        self.streaming = self.diccionario['streaming']
    
    def extract_lazyframe(self) -> Union[pl.DataFrame, pl.LazyFrame]: 
        if not self.diccionario: 
            logger.error('El diccionario está vacío')
            raise ValueError('El diccionario está vacío')
        
        archivo = Path(self.diccionario['input_path'])
        
        if archivo.suffix == '.csv': 
            if self.streaming == 'true': 
                df = pl.scan_csv(archivo)
                logger.info(f'Se leyó correctamente el archivo {archivo.name} en tipo streaming')
            else: 
                df = pl.read_csv(archivo)
                logger.info(f'Se leyó correctamente el archivo {archivo.name}')
        return df
    
    def normalize_min_max(self, df: Union[pl.LazyFrame, pl.DataFrame]) -> Union[pl.LazyFrame, pl.DataFrame]: 
        col = pl.col(self.diccionario['normalize_column'])
        df = df.with_columns(
            ((col - col.min()) / (col.max() - col.min())).alias(f'{col}_normalize')
        )
        logger.info(f"Se normalizó la columna {self.diccionario['normalize_column']}")
        return df
    
    def load_data(self, df: Union[pl.DataFrame, pl.LazyFrame]):
        archivo_salida = self.diccionario['output_path']
        if self.streaming == 'true':
            df_lazy = df.collect(streaming=True)
            df_lazy.write_parquet(archivo_salida)
            logger.info(f'Se colecto y guardo el Dataframe en el archivo {archivo_salida}')
        else: 
            df.write_parquet(archivo_salida)
            logger.info(f'Se guardo el DataFrame en el archivo {archivo_salida}')

class Pipeline: 
    def __init__(self, archivo: str):
        self.config = OpenToml(archivo=archivo)
        self.toml = self.config.read_toml()
        self.procesador = DataProcessor(diccionario=self.toml)
    
    @task(retries=3, retry_delay_seconds=5)
    def extract(self) -> Union[pl.DataFrame, pl.LazyFrame]: 
        return self.procesador.extract_lazyframe()
    
    @task
    def normalize(self, df: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]: 
        return self.procesador.normalize_min_max(df=df)
    
    @task
    def load(self, df: Union[pl.DataFrame, pl.LazyFrame]): 
        return self.procesador.load_data(df=df)
    
    @flow(name='Pipeline MinMax')
    def pipeline(self): 
        data = self.extract()
        data_norm = self.normalize(df=data)
        self.load(df=data_norm)
