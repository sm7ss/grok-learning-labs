#Importacion de librerias 
import polars as pl
import logging
import tomli 
from prefect import task, flow 
from pydantic import BaseModel, Field
from typing import List, Literal, Dict, Any
from pathlib import Path

#Configuracion de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class ConfigJoin(BaseModel): 
    archivos: List[str] = Field(..., min_length=2)
    output_file: str
    on: str
    how: Literal['inner', 'left', 'outer']

class ReadTOML: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
    
    def read_toml(self) -> Dict[str, Any]: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        
        try: 
            with open(self.archivo, 'rb') as file: 
                read = tomli.load(file)
            logger.info(f'El archivo {self.archivo.name} se leyó correctamente')
            validador = ConfigJoin(**read['Config_Join'])
            logger.info(f'Se valido correctamente el archivo {self.archivo.name}')
            return validador
        except tomli.TOMLDecodeError as e: 
            logger.error(f'El archivo {self.archivo.name} esta corrupto: {e}')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error al querrer leer el archivo {self.archivo.name}: {e}')
            raise

class Joins: 
    def __init__(self, dicionario: Dict[str, Any]):
        self.config = dicionario
    
    def obtencion_lazyframe(self) -> List[pl.LazyFrame]: 
        lista = [pl.scan_csv(archivo) for archivo in self.config.archivos if Path(archivo).exists()]
        logger.info(f'Se obtuvieron los archivos y se convirtieron a LazyFrames')
        return lista
    
    def join_df(self, lista: List[pl.LazyFrame]) -> pl.LazyFrame: 
        if not (len(lista) == 2): 
            logger.error('La lista no tiene 2 elementos')
            raise ValueError('La lista no tiene 2 elementos')
        
        df_1, df_2 = lista
        schema_df_1, schema_df_2 = df_1.collect_schema(), df_2.collect_schema()
        if self.config.on not in schema_df_1 or self.config.on not in schema_df_2: 
            logger.error(f'La columna {self.config.on} no se encunetra en alguno de los DataFrames')
            raise ValueError(f'La columna {self.config.on} no se encunetra en alguno de los DataFrames')
        df_lazy = df_1.join(df_2, on=self.config.on, how=self.config.how)
        logger.info('Se unieron los lazyframes')
        return df_lazy
    
    def guardar_lazyframe(self, df: pl.LazyFrame): 
        df_lazy = df.collect()
        df_lazy.write_parquet(self.config.output_file)
        logger.info(f'Se guardo la consulta correctamente en el archivo {self.config.output_file}')

class Pipeline: 
    def __init__(self, archivo: str):
        self.config = ReadTOML(archivo=archivo)
        self.config_dic = self.config.read_toml()
        self.join = Joins(dicionario=self.config_dic)
    
    @task(retries=3, retry_delay_seconds=5)
    def obtencion(self) -> List[pl.LazyFrame]: 
        return self.join.obtencion_lazyframe()
    
    @task
    def join_dfs(self, lista: List[pl.LazyFrame]) -> pl.LazyFrame: 
        return self.join.join_df(lista=lista)
    
    @task
    def guadar(self, df: pl.LazyFrame): 
        return self.join.guardar_lazyframe(df=df)
    
    @flow(name='Pipeline Join')
    def pipeline(self): 
        data = self.obtencion()
        df = self.join_dfs(lista=data)
        self.guadar(df=df)


