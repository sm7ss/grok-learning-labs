#Importacion de librerias necesrias 
import yaml
import ray 
import polars as pl
import logging 
import pyarrow as pa
from pathlib import Path
from pydantic import Field, BaseModel, model_validator, field_validator
from enum import Enum
from typing import List, Type

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class EstrategiaRank(str, Enum): 
    AVERAGE = 'average'
    DENSE = 'dense'
    MIN = 'min'

class ValidadorCompute(BaseModel): 
    use_ray: bool
    chunk_size: int = Field(gt=1000)
    streaming: bool
    memory_limit: str = Field(pattern=f'^\d+[MG]B$')
    partition_by: List[str] = Field(min_length=1)

class ValidadorEtl(BaseModel): 
    input_path: str
    output_path: str
    feature_columns: List[str] = Field(min_length=1)
    window_sizes: List[int] = Field(min_length=1)
    lags: List[int] = Field(min_length=1)
    rank_method: EstrategiaRank
    
    @field_validator('input_path')
    def csv_existente(cls, v): 
        archivo = Path(v)
        if archivo.suffix != '.csv': 
            logger.error(f'El archivo {v} debe de ser un archivo csv')
            raise ValueError(f'El archivo {v} debe de ser un archivo csv')
        
        if not archivo.exists(): 
            logger.error(f'El archivo {v} no eciste')
            raise FileNotFoundError(f'El archivo {v} no eciste')
        return archivo
    
    @field_validator('feature_columns')
    def valores_unicos(cls, v): 
        if len(v) != len(set(v)): 
            logger.error('Las columnas deben de ser unicas')
            raise ValueError('Las columnas deben de ser unicas')
        return v
    
    @model_validator(mode='after')
    def paths_y_listas(self): 
        if not (len(self.feature_columns) == len(self.window_sizes) == len(self.lags)):
            logger.error('Las listas deben de tener la misma longitud')
            raise ValueError('Las listas deben de tener la misma longitud')
        
        for i in range(len(self.feature_columns)):
            if self.window_sizes[i] < 1:
                logger.error(f'El valor en window_sizes en el índice {i} con el valor {self.window_sizes[i]} debe ser mayor a 0')
                raise ValueError(f'El valor en window_sizes en el índice {i} con el valor {self.window_sizes[i]} debe ser mayor a 0')
            
            if self.lags[i] < 0:
                logger.error(f'El valor en lags en el índice {i} con el valor {self.lags[i]} debe ser mayor o igual a 0')
                raise ValueError(f'El valor en lags en el índice {i} con el valor {self.lags[i]} debe ser mayor o igual a 0')
        return self
    
    @model_validator(mode='after')
    def validacion_columnas(self): 
        schema = pl.scan_csv(self.input_path).collect_schema()
        for col in self.feature_columns: 
            if col not in schema: 
                logger.error(f'La columna {col} no existe')
                raise ValueError(f'La columna {col} no existe')
            
            if not schema[col].is_numeric():
                logger.error(f'La columna {col} debe ser numerica')
                raise ValueError(f'La columna {col} debe ser numerica')
        return self

class Validador(BaseModel): 
    compute: ValidadorCompute
    etl: ValidadorEtl

class ReadToml: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
        
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
    
    def read_toml(self) -> BaseModel:
        try: 
            with open(self.archivo, 'r') as file: 
                read = yaml.safe_load(file)
            logger.info(f'Se leyó correctamente el archivo {self.archivo.name}')
            validador = Validador(**read)
            logger.info(f'Se valido el archivo {self.archivo.name}')
            return validador
        except UnicodeDecodeError as e: 
            logger.error(f'La decodificacion del archivo {self.archivo.name} no es posible: {e}')
            raise 
        except yaml.YAMLError as e: 
            logger.error(f'El archivo {self.archivo.name} está corrupto')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer leer el archivo {self.archivo.name}: {e}')
            raise ValueError(f'Ocurrio un error al querer leer el archivo {self.archivo.name}: {e}')

class FeatureEngineer: 
    def __init__(self, model: Type[BaseModel]):
        self.config_etl = model.etl
    
    def rolling_data(self, col: str, window: int, lag: int) -> pl.Expr: 
        return pl.col(col).rolling_sum(window).shift(lag).alias(f'Rolling_{col}')
    
    def cumsum_data(self, col: str, over: str) -> pl.Expr:
        if over != 'user_id': 
            logger.error('La columna over debe de ser "user_id"')
        return pl.col(col).cum_sum().over(over).alias(f'Acumulacion_{col}')
    
    def rank_data(self, col: str, metodo: str) -> pl.Expr: 
        return pl.col(col).rank(method=metodo).alias(f'Rank_{col}')

class Pipeline:
    def __init__(self, archivo: str):
        self.model = ReadToml(archivo=archivo).read_toml()
        self.config_compute = self.model.compute
        self.config_etl = self.model.etl
        self.fe = FeatureEngineer(model=self.model)
        
        if self.config_compute.use_ray:
            if ray.is_initialized():
                ray.shutdown()
            ray.init(num_cpus=4, object_store_memory=int(self.config_compute.memory_limit.replace("GB", "")) * 1e9)
    
    def get_table(self, df_arrow: pa.Table) -> pa.Table:
        try:
            lista_expresiones = []
            for rango in range(len(self.config_etl.feature_columns)):
                col = self.config_etl.feature_columns[rango]
                window = self.config_etl.window_sizes[rango]
                lag = self.config_etl.lags[rango]
                lista_expresiones.append(self.fe.rolling_data(col=col, window=window, lag=lag))
                lista_expresiones.append(self.fe.cumsum_data(col=col, over='user_id'))
                lista_expresiones.append(self.fe.rank_data(col=col, metodo=self.config_etl.rank_method))
            
            df_polars = pl.from_arrow(df_arrow)
            df_transformer = df_polars.lazy().with_columns(lista_expresiones).collect(engine='streaming')
            return df_transformer.to_arrow()
        except Exception as e:
            logger.error(f"Error en get_table: {str(e)}")
            raise
    
    def pipeline(self):
        ds = ray.data.read_csv(self.config_etl.input_path)
        transform_data = ds.map_batches(
            lambda batch: self.get_table(df_arrow=batch),
            batch_size=self.config_compute.chunk_size,
            batch_format="pyarrow"
        )
        transform_data.write_parquet(self.config_etl.output_path)

