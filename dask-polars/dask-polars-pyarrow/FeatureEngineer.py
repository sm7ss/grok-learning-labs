#Importacion de librerías importantes 
import polars as pl 
from dask.distributed import Client
import dask.dataframe as dd
import logging
import tomli
from pydantic import BaseModel, Field, model_validator
from typing import List, Literal, Type
from enum import Enum
from pathlib import Path
from prefect import task, flow

#Configuracion del logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class EstrategiasRolling(str, Enum): 
    rolling_mean = 'rolling_mean'
    rolling_sum = 'rolling_sum'
    rolling_max = 'rolling_max'

class ValidadorCompute(BaseModel): 
    use_dask: bool
    streaming: Literal['streaming', 'deafult']
    chunk_size: int = Field(gt=1000)
    memory_limit: str = Field(pattern=f'^\d+[MG]B$')

class ValidadorPath(BaseModel): 
    input_path_data: str
    output_path_data: str

class ValidadorEtl(BaseModel): 
    estrategia_rolling: EstrategiasRolling
    feature_columns: List[str] = Field(min_length=1)
    window_size: List[int] = Field(min_length=1)
    lags: List[int] = Field(min_length=1)
    
    @model_validator(mode='after')
    def validador_longitud(self): 
        if len({len(self.feature_columns), len(self.window_size), len(self.lags)}) > 1: 
            logger.error('Las listas deben de tener las mismas longitudes')
            raise ValueError('Las listas deben de tener las mismas longitudes')
        
        if len(set(self.feature_columns)) != len(self.feature_columns): 
            logger.error('Las columnas deben de ser unicas')
            raise ValueError('Las columnas deben de ser unicas')
        
        return self

class Validador(BaseModel): 
    compute : ValidadorCompute
    path: ValidadorPath
    etl : ValidadorEtl

class ReadToml: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
        
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        
        if self.archivo.suffix != ".toml": 
            logger.error(f'El archivo {self.archivo.name} deber ser un archivo toml')
            raise ValueError(f'El archivo {self.archivo.name} deber ser un archivo toml')
    
    def read_toml(self) -> BaseModel: 
        try: 
            with open(self.archivo, 'rb') as f: 
                read = tomli.load(f)
            logger.info(f'Se leyó correctamente el archivo {self.archivo.name}')
            validador = Validador(**read)
            logger.info(f'Se valido correctamene el archivo {self.archivo.name}')
            return validador
        except tomli.TOMLDecodeError as e: 
            logger.error(f'El archivo {self.archivo.name} está corrupto: {e}')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error al querrer leer el archivo {self.archivo.name} tipo: {e}')
            raise

class GetLazyFrame: 
    def __init__(self, model: BaseModel):
        self.model = model
        self.path = self.model.path.input_path_data
        self.archivo = Path(self.path)
        
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        
        if self.archivo.suffix != '.csv': 
            logger.error(f'El archivo {self.archivo.name} debe ser un archivo csv')
            raise ValueError(f'El archivo {self.archivo.name} debe ser un archivo csv')
    
    def get_lazyframe(self) -> pl.LazyFrame: 
        df_lazy = pl.scan_csv(self.archivo)
        
        if df_lazy.collect_schema()[self.model.etl.feature_columns[0]] == 0:
            logger.error(f'El lazyframe del archivo {self.archivo.name} está vacio')
            raise ValueError(f'El lazyframe del archivo {self.archivo.name} está vacio')
        
        logger.info(f'Se creo el lazyframe del archivo {self.archivo.name}')
        return df_lazy

class FeatureEngineer: 
    def __init__(self, df_lazy: pl.LazyFrame, model: Type[BaseModel]):
        self.df_lazy = df_lazy 
        self.etl = model.etl
        self.schema = self.df_lazy.collect_schema()
        
        for col in self.etl.feature_columns: 
            if col not in self.schema: 
                logger.error(f'La columna {col} no se encuentra en el LazyFrame')
                raise ValueError(f'La columna {col} no se encuentra en el LazyFrame')
            
            if not self.schema[col].is_numeric(): 
                logger.error(f'La columna {col} debe ser una columna númerica')
                raise ValueError(f'La columna {col} debe ser una columna númerica')
            
        if 'user_id' not in self.schema:
            logger.error('La columna "user_id" debe de existir en el LazyFrame')
            raise ValueError('La columna "user_id" debe de existir en el LazyFrame')
        
        self.columnas = self.etl.feature_columns
        self.window = self.etl.window_size
        self.lag = self.etl.lags
    
    def rolling_data(self) -> pl.Expr: 
        return [
            getattr(pl.col(self.columnas[rango]), self.etl.estrategia_rolling)
            (window_size=self.window[rango])
            .shift(self.lag[rango])
            .cum_sum()
            .over('user_id')
            .alias(f'{self.etl.estrategia_rolling}_{self.columnas[rango]}')
            for rango in range(len(self.columnas))
        ]

class PipelineFeatureEngineer: 
    def __init__(self, archivo: str):
        self.model = ReadToml(archivo=archivo).read_toml()
        self.df_lazy = GetLazyFrame(model=self.model).get_lazyframe()
        self.fe = FeatureEngineer(df_lazy=self.df_lazy, model=self.model)
        
        self.compute_config = self.model.compute
        self.path_config = self.model.path
        
        if not self.path_config.output_path_data.endswith('.parquet'): 
            logger.error(f'El archivo {self.path_config.output_path_data} no es un archivo para salida parquet')
            raise ValueError(f'El archivo {self.path_config.output_path_data} no es un archivo para salida parquet')
    
    @task
    def feature_enfineer(self) -> List[pl.Expr]: 
        return self.fe.rolling_data()
    
    @flow(name='Pipeline Chafa')
    def pipeline_fe(self): 
        features = self.feature_enfineer()
        df_lazy = self.df_lazy.with_columns(features)
        if self.compute_config.use_dask: 
            with Client(n_workers=4, threads_per_worker=2, memory_limit=self.compute_config.memory_limit): 
                df_pandas = df_lazy.collect(engine=self.compute_config.streaming).to_pandas()
                dask_frame = dd.from_pandas(df_pandas, npartitions=10)
                
                dask_frame.to_parquet(
                    self.path_config.output_path_data, 
                    compression='zstd', 
                )
        else: 
            df_lazy.sink_parquet(
                self.path_config.output_path_data, 
                compression='zstd', 
                row_group_size=self.compute_config.chunk_size
            )
