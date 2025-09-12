#Importacion de librerías necesarias
import polars as pl
import logging 
import tomli
from pathlib import Path
from typing import Type, Dict
from pydantic import BaseModel
from prefect import flow, task

#Configuracion logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class ReadToml: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
    
    def read_toml(self, validador_model: Type[BaseModel]) -> BaseModel:
        nombre_archivo = self.archivo.name
        if not self.archivo.exists(): 
            logger.error(f'El archivo {nombre_archivo} no existe')
            raise FileNotFoundError(f'El archivo {nombre_archivo} no existe')
        
        if self.archivo.suffix != '.toml': 
            logger.error(f'El archivo {nombre_archivo} tiene una reminacion {self.archivo.suffix} cuando debería de ser .toml')
            raise TypeError(f'El archivo {nombre_archivo} tiene una reminacion {self.archivo.suffix} cuando debería de ser .toml')
        
        try: 
            with open(self.archivo, 'rb') as f: 
                read = tomli.load(f)
            logger.info(f'Se leyó el archivo {nombre_archivo} exitosamente')
            validador = validador_model(**read)
            logger.info(f'Se valido correctamente el archivo {nombre_archivo}')
            return validador
        except tomli.TOMLDecodeError as e: 
            logger.error(f'el archivo {nombre_archivo} está corrupto: {e}')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio una excepcion al querer leer el archivo {nombre_archivo}: {e}')
            raise

class ExtractLazyFrame: 
    def __init__(self, model: BaseModel):
        self.rolling_data = model.rolling_data
    
    def extract_data(self) -> pl.LazyFrame: 
        nombre_archivo = Path(self.rolling_data.input_path)
        
        if not nombre_archivo.exists(): 
            logger.error(f'El archivo {nombre_archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {nombre_archivo.name} no existe')
        
        df_lazy = pl.scan_csv(nombre_archivo)
        logger.info(f'Se leyó correctamente el archivo {nombre_archivo.name} y se creó su LazyFrame')
        return df_lazy

class PipelineExtractData: 
    def __init__(self, archivo: str, model_validator: Type[BaseModel]):
        self.archivo = archivo
        self.model = model_validator
    
    @task(retries=3, retry_delay_seconds=5)
    def read(self) -> BaseModel: 
        return ReadToml(archivo=self.archivo).read_toml(validador_model=self.model)
    
    @task
    def extract(self, model: BaseModel) -> Dict[pl.LazyFrame, BaseModel]: 
        return ExtractLazyFrame(model=model).extract_data()
    
    @flow(name='Pipeline de Extracción de Datos')
    def pipeline_extract_data(self) -> pl.LazyFrame: 
        model = self.read()
        df_lazy = self.extract(model=model)
        return df_lazy, model
