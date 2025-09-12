#Importacion de librerías importantes 
import polars as pl 
import pyarrow as pa
import yaml
import tomli
import ray 
import logging
from typing import List, Type, Union, Optional
from enum import Enum
from pathlib import Path
from pydantic import BaseModel, field_validator, Field, model_validator

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class EstrategiaJoin(str, Enum): 
    INNER = 'inner'
    LEFT = 'left'
    RIGHT = 'right'
    OUTER = 'outer'

class ValidadorCompute(BaseModel): 
    use_ray: bool
    chunk_size: int = Field(gt=1000)
    streaming: bool 
    memory_limit: str = Field(pattern=f'^\d+[MG]B$')
    partition_by: List[str] = Field(max_length=1)

class EstrategiaGetFrame(str, Enum): 
    PARQUET = '.parquet'
    CSV = '.csv'

class ValidadorEtl(BaseModel): 
    input_paths: List[str] = Field(max_length=2)
    output_path: str
    join_on: str
    join_type: EstrategiaJoin
    column_filter: str
    
    @field_validator('input_paths')
    def tipo_archivo_entrada(cls, v): 
        for indice, archivo in enumerate(v): 
            v[indice] = Path(archivo)
            if not v[indice].exists(): 
                        logger.error(f'El archivo {v[indice].name} no existe')
                        raise FileNotFoundError(f'El archivo {v[indice].name} no existe')
            
            if v[indice].suffix not in [x.value for x in EstrategiaGetFrame]: 
                logger.error('Formato de archivo incorrecto')
                raise ValueError('Formato de archivo incorrecto')
        return v
    
    @field_validator('output_path')
    def tipo_archivo_salida(cls, v): 
        if not v.endswith('.parquet'): 
            logger.error(f'El archivo {v} debe ser un archivo parquet')
            raise ValueError(f'El archivo {v} debe ser un archivo parquet')
        
        if Path(v).exists(): 
            logger.error('El archivo de salida no debe de existir')
            raise FileExistsError('El archivo de salida no debe de existir')
        return v

class Validador(BaseModel): 
    compute: ValidadorCompute
    etl: ValidadorEtl
    
    @model_validator(mode='after')
    def columna_existente(self): 
        tipo_archivo = [] 
        for archivo in self.etl.input_paths: 
            if archivo.suffix == '.csv': 
                tipo_archivo.append(pl.scan_csv(archivo).collect_schema())
            else: 
                tipo_archivo.append(pl.scan_parquet(archivo).collect_schema())
        df_1, df_2 = tipo_archivo
        if self.etl.join_on not in df_1 or self.etl.join_on not in df_2: 
            logger.error(f'La columna {self.etl.join_on} no se encunetra en el DataFrame')
            raise ValueError(f'La columna {self.etl.join_on} no se encunetra en el DataFrame')
        
        if self.compute.partition_by[0] not in df_1 or self.compute.partition_by[0] not in df_2: 
            logger.error(f'La columna {self.compute.partition_by[0]} no se encuentra en el DataFrame')
            raise ValueError(f'La columna {self.compute.partition_by[0]} no se encuentra en el DataFrame')
        
        if self.etl.column_filter not in df_1:
            logger.error(f'La columna {self.etl.column_filter} no se encuentra en el primer DataFrame')
            raise ValueError(f'La columna {self.etl.column_filter} no se encuentra en el primer DataFrame')

        if not df_1[self.etl.column_filter].is_numeric():
            logger.error(f'La columna {self.etl.column_filter} en el primer DataFrame no es numérica')
            raise ValueError(f'La columna {self.etl.column_filter} en el primer DataFrame no es numérica')
        return self

class EstretegiaConfig(str, Enum): 
    YAML = '.yaml'
    TOML = '.toml'

class ReadFile: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        if self.archivo.suffix not in [x.value for x in EstretegiaConfig]:
            logger.error('El archivo de entrada debe de ser un YAML o un TOML')
            raise ValueError('El archivo de entrada debe de ser un YAML o un TOML')
    
    def read_file(self) -> BaseModel: 
        estrategia_archivo = 'r' if self.archivo.suffix == EstretegiaConfig.YAML else 'rb'
        try: 
            with open(self.archivo, estrategia_archivo) as file: 
                lectura_archivo = yaml.safe_load(file) if EstretegiaConfig.YAML else tomli.load(file)
                read = lectura_archivo
            logger.info('Se leyó correctamente el archivo')
            validacion = Validador(**read)
            logger.info('Se valido correctamente el archivo')
            return validacion
        except yaml.YAMLError if EstretegiaConfig.YAML else tomli.TOMLDecodeError: 
            logger.error('El archivo está corrupto')
            raise 
        except UnicodeDecodeError: 
            logger.error('No se pudo leer el archivo')
            raise 
        except Exception as e: 
            logger.error(f'Ocurio un error durante la lectura: {e}')
            raise

class GetFrame: 
    def __init__(self, model: Type[BaseModel]):
        self.config_etl = model.etl
        
        self.archivo_1 = self.config_etl.input_paths[0]
        self.archivo_2 = self.config_etl.input_paths[1]
        
        self.lista_tipo_archivo = [x.value for x in EstrategiaGetFrame]
    
    def get_polars_frame(self) -> pl.LazyFrame: 
        if self.archivo_1.suffix in self.lista_tipo_archivo and self.archivo_2.suffix in self.lista_tipo_archivo: 
            if self.archivo_1.suffix == '.csv': 
                df_lazy_1, df_lazy_2 = pl.scan_csv(self.archivo_1), pl.scan_csv(self.archivo_2)
            else: 
                df_lazy_1, df_lazy_2 = pl.scan_parquet(self.archivo_1), pl.scan_parquet(self.archivo_2)
        return df_lazy_1, df_lazy_2
    
    def get_ray_frame(self) -> ray.data.Dataset: 
        if self.archivo_1.suffix in self.lista_tipo_archivo and self.archivo_2.suffix in self.lista_tipo_archivo: 
            if self.archivo_1.suffix == '.csv': 
                ray_frame_1, ray_frame_2 = ray.data.read_csv(self.archivo_1), ray.data.read_csv(self.archivo_2)
            else: 
                ray_frame_1, ray_frame_2 = ray.data.read_parquet(self.archivo_1), ray.data.read_parquet(self.archivo_2)
        return ray_frame_1, ray_frame_2

class JoinFilterData: 
    def __init__(self, model: Type[BaseModel]):
        self.config_etl = model.etl
        self.config_compute = model.compute
    
    def filtrar_data(self) -> pl.Expr: 
        return pl.when(pl.col(self.config_etl.column_filter) < 50).then(pl.lit('Bajo')).otherwise(pl.lit('Alto')).alias(f'{self.config_etl.column_filter}_diferiencia')
    
    def join_data(self, frame_1: Union[pl.LazyFrame, ray.data.Dataset], frame_2: Union[pl.LazyFrame, ray.data.Dataset])-> Union[pl.LazyFrame, ray.data.Dataset]: 
        if self.config_compute.use_ray: 
            joined_data = frame_1.join(frame_2, on=[self.config_etl.join_on], num_partitions=4, join_type=self.config_etl.join_type)
            return joined_data
        else: 
            joined_data = frame_1.join(frame_2, on=self.config_etl.join_on, how=self.config_etl.join_type)
            return joined_data

class Pipeline: 
    def __init__(self, archivo: str):
        self.model = ReadFile(archivo=archivo).read_file()
        
        self.config_compute = self.model.compute
        self.config_etl = self.model.etl
        
        self.frame = GetFrame(model=self.model)
        self.join_transform = JoinFilterData(model=self.model)
    
    def get_data(self, data: Union[pl.LazyFrame, pa.Table]) -> Union[pl.Expr, pa.Table]: 
        filter_data = self.join_transform.filtrar_data()
        
        if self.config_compute.use_ray: 
            polars_frame = pl.from_arrow(data)
            if self.config_etl.column_filter in polars_frame.collect_schema():
                return polars_frame.lazy().with_columns(filter_data).collect(engine='streaming').to_arrow()
            else:
                return polars_frame.to_arrow()
        else: 
            if self.config_etl.column_filter in data.collect_schema():
                return data.with_columns(filter_data)
            else:
                return data
    
    def pipeline(self): 
        if self.config_compute.use_ray: 
            if ray.is_initialized(): 
                ray.shutdown()
            ray.init(num_cpus=4, object_store_memory=int(self.config_compute.memory_limit.replace("GB", ""))*1e9)
            
            ds_1, ds_2 = self.frame.get_ray_frame()
            ds_1_filter = ds_1.map_batches(
                lambda partition: self.get_data(data=partition), 
                batch_size= self.config_compute.chunk_size,
                batch_format= "pyarrow"
            )
            ds_2_filter = ds_2.map_batches(
                lambda partition: self.get_data(data=partition), 
                batch_size= self.config_compute.chunk_size,
                batch_format= "pyarrow"
            )
            
            ds_joined = self.join_transform.join_data(frame_1=ds_1_filter, frame_2=ds_2_filter)
            ds_joined.write_parquet(self.config_etl.output_path.replace('.parquet', ''))
        else: 
            df_lazy_1, df_lazy_2 = self.frame.get_polars_frame()
            df_1_filtered= self.get_data(data=df_lazy_1)
            df_2_filtered= self.get_data(data=df_lazy_2)
            
            df_joined = self.join_transform.join_data(frame_1=df_1_filtered, frame_2=df_2_filtered)
            df_joined.sink_parquet(
                self.config_etl.output_path, 
                compression='zstd', 
                row_group_size=self.config_compute.chunk_size
            )