#Importacion de librerias importantes 
import logging
import polars as pl
from pydantic import BaseModel, Field, model_validator, field_validator
from Strategy import EstrategiaAgg, EstrategiaWindow, EstrategiaClean
from typing import Optional
from pathlib import Path

#Config logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class CleanData(BaseModel): 
    operation: EstrategiaClean
    col: str

class WindowData(BaseModel): 
    operation: EstrategiaWindow
    col: str
    window: int = Field(ge=0)
    over: str

class AggData(BaseModel): 
    operation: EstrategiaAgg
    col: str 
    group_by: str

class etl(BaseModel): 
    input_path: str
    output_path: str
    clean_data: Optional[CleanData]
    window_data: Optional[WindowData]
    agg_data: Optional[AggData]
    
    @field_validator('input_path')
    def archivo_existente(cls, v): 
        path = Path(v)
        if not path.exists():
            logger.error(f'El archivo {path.name} no existe')
            raise FileNotFoundError(f'El archivo {path.name} no existe')
        if path.suffix not in ['.csv','.parquet']: 
            logger.error(f'El archivo {path.name} debe ser un archivo csv o parquet')
            raise ValueError(f'El archivo {path.name} debe ser un archivo csv o parquet')
        return path
    
    @field_validator('output_path')
    def archivo_salida_validador(cls, v): 
        path = Path(v)
        if path.exists(): 
            logger.error(f'El archivo {path.name} no debe de existir')
            raise FileExistsError(f'El archivo {path.name} no debe de existir')
        if path.suffix != '.parquet': 
            logger.error(f'El archivo {path} debe ser un tipo parquet')
            raise ValueError(f'El archivo {path} debe ser un tipo parquet')
        return v
    
    @model_validator(mode='after')
    def columnas_existentes(self): 
        if self.input_path.suffix == '.csv': 
            schema = pl.scan_csv(self.input_path).collect_schema()
        else: 
            schema = pl.scan_parquet(self.input_path).collect_schema()
        
        clean = self.clean_data
        window = self.window_data
        agg = self.agg_data
        
        if clean is not None: 
            if clean.col not in schema: 
                logger.error(f'La columna {clean.col} no existe en el schema')
                raise ValueError(f'La columna {clean.col} no existe en el schema')
        
        if window is not None: 
            if window.col not in schema: 
                logger.error(f'La columna {window.col} no existe en el schema')
                raise ValueError(f'La columna {window.col} no existe en el schema')
            if not schema[window.col].is_numeric(): 
                logger.error(f'La columna {window.col} debe ser una columna numerica')
                raise ValueError(f'La columna {window.col} debe ser una columna numerica')
            if window.over not in schema: 
                logger.error(f'La columna {window.over} no existe en el schema')
                raise ValueError(f'La columna {window.over} no existe en el schema')
        
        if agg is not None:
            if agg.col not in schema: 
                logger.error(f'La columna {agg.col} no existe en el schema')
                raise ValueError(f'La columna {agg.col} no existe en el schema')
            if not schema[agg.col].is_numeric(): 
                logger.error(f'La columna {agg.col} debe ser una columna numerica')
                raise ValueError(f'La columna {agg.col} debe ser una columna numerica')
            if agg.group_by not in schema: 
                logger.error(f'La columna {agg.group_by} no existe en el schema')
                raise ValueError(f'La columna {agg.group_by} no existe en el schema')
        return self
