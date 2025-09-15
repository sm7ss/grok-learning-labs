import logging
import polars as pl
from pydantic import BaseModel, Field, model_validator, field_validator

from Strategy import EstrategiaAgg, EstrategiaWindow, EstrategiaClean, EstrategiaJoin
from typing import Union, List, Optional
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class DataConfig(BaseModel): 
    input_path: Union[str, List[str]]
    output_path: str
    
    @field_validator('input_path')
    def input_path(cls, v): 
        if isinstance(v, list): 
            if not v:
                logger.error(f'La lista de archivos debe al menos tener un parametro')
                raise ValueError(f'La lista de archivos debe al menos tener un parametro')
            
            ruta_archivo=[]
            for archivo in v: 
                ruta = Path(archivo)
                if not ruta.exists(): 
                    logger.error(f'El archivo {ruta.name} no existe')
                    raise FileNotFoundError(f'El archivo {ruta.name} no existe')
                
                if ruta.suffix not in ['.parquet', '.csv']: 
                    logger.error(f'El archivo {ruta.name} debe ser un archivo tipo parquet o csv')
                    raise ValueError(f'El archivo {ruta.name} debe ser un archivo tipo parquet o csv')
                ruta_archivo.append(ruta)
            return ruta_archivo
        
        ruta = Path(v)
        if not ruta.exists(): 
            logger.error(f'El archivo {ruta.name} no existe')
            raise FileNotFoundError(f'El archivo {ruta.name} no existe')
        if ruta.suffix not in ['.parquet', '.csv']: 
            logger.error(f'El archivo {ruta.name} debe ser un archivo tipo parquet o csv')
            raise ValueError(f'El archivo {ruta.name} debe ser un archivo tipo parquet o csv')
        return ruta
    
    @field_validator('output_path')
    def output_path(cls, v): 
        ruta = Path(v)
        if ruta.suffix != '.parquet': 
            logger.error(f'El archivo de salida {ruta.name} debe ser un tipo parquet')
            raise ValueError(f'El archivo de salida {ruta.name} debe ser un tipo parquet')
        if ruta.exists(): 
            logger.error(f'El archivo {ruta.name} no debe de existir')
            raise FileExistsError(f'El archivo {ruta.name} no debe de existir')
        return v

class CleanDataConfig(BaseModel): 
    operation: EstrategiaClean
    col: str

class WindowDataConfig(BaseModel): 
    operation: EstrategiaWindow
    col: str
    window: int = Field(ge=0)
    over: str

class AggDataConfig(BaseModel): 
    operation: EstrategiaAgg
    col: str 
    group_by: str

class PostFilterConfig(BaseModel): 
    imbalance_handle: bool
    col: str 
    threshold: int = Field(gt=0)

class JoinDataConfig(BaseModel): 
    join_on: str 
    join_type: EstrategiaJoin 
    post_filter: Optional[PostFilterConfig]

class etl(BaseModel): 
    data: DataConfig
    clean_data: Optional[CleanDataConfig]
    window_data: Optional[WindowDataConfig]
    agg_data: Optional[AggDataConfig]
    join_data: Optional[JoinDataConfig]
    
    @model_validator(mode='after')
    def columnas_existentes_join(self): 
        input_data = self.data.input_path
        if isinstance(input_data, list): 
            if self.join_data is None:
                logger.error('Se necesita información para poder hacer join entre archivos')
                raise ValueError('Se necesita información para poder hacer join entre archivos')
            
            lista = []
            for archivo in input_data: 
                if archivo.suffix == '.csv': 
                    schema = pl.scan_csv(archivo).collect_schema()
                    lista.append(schema)
                else: 
                    schema = pl.scan_parquet(archivo).collect_schema()
                    lista.append(schema)
            
            schema_1, schema_2 = lista
            
            if self.join_data.join_on not in schema_1 or self.join_data.join_on not in schema_2: 
                logger.error(f'La columna {self.join_data.join_on} debe estar en ambos schemas')
                raise ValueError(f'La columna {self.join_data.join_on} debe estar en ambos schemas')
            
            post_filter = self.join_data.post_filter
            if post_filter is not None: 
                if post_filter.col not in schema_1 or post_filter.col not in schema_2:
                    logger.error(f'La columna {post_filter.col} no sé encuentra en schema')
                    raise ValueError(f'La columna {post_filter.col} no sé encuentra en schema')
                
                if not schema_1[post_filter.col].is_numeric() or not schema_2[post_filter.col].is_numeric(): 
                    logger.error(f'La columna {post_filter.col} debe ser una columna numerica')
                    raise ValueError(f'La columna {post_filter.col} debe ser una columna numerica')
        
        if self.data.suffix == '.csv': 
            schema = pl.scan_csv(self.data.input_path).collect_schema()
        else: 
            schema= pl.scan_parquet(self.data.input_path).collect_schema()
        
        if self.clean_data is not None: 
            if self.clean_data.col not in schema: 
                logger.error(f'La columna {self.clean_data.col} no existe en el Schema')
                raise ValueError(f'La columna {self.clean_data.col} no existe en el Schema')
        
        if self.window_data is not None: 
            if self.window_data.col not in schema: 
                logger.error(f'La columna {self.window_data.col} no existe en el Schema')
                raise ValueError(f'La columna {self.window_data.col} no existe en el Schema')
            if not schema[self.window_data.col].is_numeric(): 
                logger.error(f'La columna {self.window_data.col} debe ser una columna numerica')
                raise ValueError(f'La columna {self.window_data.col} debe ser una columna numerica')
            if self.window_data.over not in schema: 
                logger.error(f'La columna {self.window_data.over} no existe en el Schema')
                raise ValueError(f'La columna {self.window_data.over} no existe en el Schema')
        
        if self.agg_data is not None: 
            if self.agg_data.col not in schema: 
                logger.error(f'La columna {self.agg_data.col} no existe en el Schema')
                raise ValueError(f'La columna {self.agg_data.col} no existe en el Schema')
            if not schema[self.agg_data.col].is_numeric(): 
                logger.error(f'La columna {self.agg_data.col} debe ser una columna numerica')
                raise ValueError(f'La columna {self.agg_data.col} debe ser una columna numerica')
            if self.agg_data.group_by not in schema: 
                logger.error(f'La columna {self.agg_data.group_by} no existe en el Schema')
                raise ValueError(f'La columna {self.agg_data.group_by} no existe en el Schema')
        
        return self
