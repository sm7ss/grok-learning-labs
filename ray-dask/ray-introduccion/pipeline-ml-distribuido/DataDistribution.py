#Importacion de librerías
import yaml
import polars as pl 
import ray
import logging 
import pyarrow as pa
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum
from typing import List, Type
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class EstrategiasEscalado(str, Enum): 
    SCALER = 'scaler'
    MINMAX = 'minmax'

class EstrategiasAgregaciones(str, Enum): 
    SUM = 'sum'
    MIN = 'min'
    MEAN = 'mean'

class EstrategiaLectura(str, Enum): 
    PARQUET = '.parquet'
    CSV = '.csv'

class ValidacionData(BaseModel): 
    input_path: str
    output_path: str
    
    @field_validator('input_path')
    def archivo_existente(cls, v): 
        archivo = Path(v)
        if not archivo.exists(): 
            logger.error(f'El archivo {archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {archivo.name} no existe')
        
        if not (archivo.suffix != EstrategiaLectura): 
            logger.error(f'El archivo {archivo.name} debe ser un archivo csv o parquet')
            raise ValueError(f'El archivo {archivo.name} debe ser un archivo csv o parquet')
        return archivo

class ValidacionCompute(BaseModel): 
    use_ray: bool
    chunk_size: int
    streaming: bool
    partition_by: List[str] = Field(max_length=1)

class ValidacionAgregaciones(BaseModel): 
    age: EstrategiasAgregaciones

class ValidacionMl(BaseModel): 
    learning_rate: float = Field(gt=0)
    scaler: EstrategiasEscalado
    feature_columns: List[str] = Field(min_length=1)
    aggregations: ValidacionAgregaciones

class Validacion(BaseModel): 
    data: ValidacionData
    compute: ValidacionCompute
    ml: ValidacionMl
    
    @model_validator(mode='after')
    def columnas_existentes(self): 
        schema = pl.scan_csv(self.data.input_path).collect_schema() if self.data.input_path.suffix == '.csv' else pl.scan_parquet(self.data.input_path)
        particiones = self.compute.partition_by[0]
        if particiones not in schema: 
            logger.error(f'La columna {particiones} no se encuentra en el DataFrame')
            raise ValueError(f'La columna {particiones} no se encuentra en el DataFrame')
        
        for col in self.ml.feature_columns: 
            if col not in schema: 
                logger.error(f'La columna {col} no se encunetra en el DataFrame')
                raise ValueError(f'La columna {col} no se encunetra en el DataFrame')
            
            if not schema[col].is_numeric(): 
                logger.error(f'La columna {col} debe ser númerica')
                raise ValueError(f'La columna {col} debe ser númerica')
        return self

class ReadFile: 
    def __init__(self, archivo: str):
        self.archivo = archivo
    
    def read_file(self) -> BaseModel: 
        try: 
            with open(self.archivo, 'r') as file: 
                read = yaml.safe_load(file)
            logger.info(f'Se leyó correctamente el archivo {self.archivo}')
            validador = Validacion(**read)
            logger.info(f'El archivo {self.archivo} se valido correctamente')
            return validador
        except yaml.YAMLError: 
            logger.error(f'El archivo {self.archivo} está corrputo')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer leer el archivo {self.archivo}: {e}')
            raise

class GetLazyFrame: 
    def __init__(self, model: Type[BaseModel]):
        self.config_data = model.data
    
    def get_lazyframe(self) -> pl.LazyFrame: 
        if self.config_data.input_path.suffix == '.csv': 
            df_lazy = pl.scan_csv(self.config_data.input_path)
        else: 
            df_lazy = pl.scan_parquet(self.config_data.input_path)
        logger.info(f'Se obtuvo el LazyFrame')
        return df_lazy

class EscalerData: 
    def __init__(self, model: Type[BaseModel]):
        self.model = model.ml
    
    def scaler_data(self, col: str) -> pl.Expr: 
        std_valor = pl.col(col).std()
        return pl.when(std_valor == 0).then(pl.lit(0)).otherwise(((pl.col(col) - pl.col(col).mean()) / std_valor)).alias(f'scaler_{col}')
    
    def minmax_data(self, col: str) -> pl.Expr: 
        diferiencia = (pl.col(col).max() - pl.col(col).min())
        return pl.when(diferiencia == 0).then(pl.lit(0)).otherwise(((pl.col(col) - pl.col(col).min()) / diferiencia)).alias(f'minmax_{col}')

class Agregaciones: 
    def __init__(self, model: Type[BaseModel]):
        self.model = model.ml.aggregations.age
    
    def agregaciones_data(self, col: str) -> pl.Expr: 
        return getattr(pl.col(col), self.model.value)().alias(f'{self.model.value}_{col}')

class Pipeline: 
    def __init__(self, archivo: str):
        self.model = ReadFile(archivo=archivo).read_file()
        
        self.config_data = self.model.data
        self.config_compute = self.model.compute
        self.config_ml = self.model.ml
        
        self.scalar_data = EscalerData(model=self.model)
        self.agregaciones = Agregaciones(model=self.model)
    
    def escalado(self, col: str, estrategia: EstrategiasEscalado) -> pl.Expr: 
        match estrategia: 
            case EstrategiasEscalado.SCALER: 
                escalado = self.scalar_data.scaler_data(col=col)
            case EstrategiasEscalado.MINMAX: 
                escalado = self.scalar_data.minmax_data(col= col)
            case None: 
                logger.error('No se selecciono una estrategia')
                raise ValueError('No se selecciono una estrategia')
            case _: 
                logger.error(f'Estrategia {estrategia} no valida')
                raise ValueError(f'Estrategia {estrategia} no valida')
        return escalado
    
    def get_agregation(self): 
        lista_agregacion = []
        
        for col in self.config_ml.feature_columns: 
            lista_agregacion.append(self.agregaciones.agregaciones_data(col=col))
        return lista_agregacion
    
    def get_table(self, table: pa.Table) -> pa.Table:
        lista_expresion = []
        estrategia = self.config_ml.scaler
        
        for col in self.config_ml.feature_columns: 
            lista_expresion.append(self.escalado(col=col, estrategia=estrategia))
        
        df_polars = pl.from_arrow(table)
        df_transformer = df_polars.lazy().with_columns(lista_expresion).collect(engine='streaming')
        return df_transformer.to_arrow()
    
    def pipeline(self): 
        if self.config_compute.use_ray: 
            if self.config_compute.use_ray: 
                if ray.is_initialized(): 
                    ray.shutdown()
                ray.init(num_cpus=4, object_store_memory=1*1e9)
            
            if self.config_data.input_path.suffix == '.csv': 
                ds = ray.data.read_csv(self.config_data.input_path)
            else: 
                ds = ray.data.read_parquet(self.config_data.input_path)
            ray_transform = ds.map_batches(
                lambda partition: self.get_table(table=partition), 
                batch_format='pyarrow'
            )
            ray_transform.write_parquet(self.config_data.output_path)
        else: 
            df_lazy = GetLazyFrame(model=self.model).get_lazyframe()
            
            df_lazy_final = df_lazy.with_columns(self.get_agregation())
            df_lazy_final.sink_parquet(
                f'{self.config_data.output_path.strip()}.parquet', 
                compression='zstd', 
                row_group_size=self.config_compute.chunk_size
            )

