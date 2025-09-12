#Importamos las librerías necesarias 
import logging
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Union, Literal
from Estrategy import EstrategiaRolling, EstretegiaEscalado, EstretegiaAgregaciones, EstrategiaJoin, EstrategiaManage

#Configuramos loggings 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class Compute(BaseModel): 
    use_dask: bool
    chunk_size: int = Field(..., gt=1000)
    streaming: Literal['streaming', 'default']
    memory_limit: str = Field(..., pattern=f'^\d+[MG]B$')
    partition_by: List[str] = Field(..., max_length=1)

class ManageEstrategy(BaseModel): 
    estrategy_function: EstrategiaManage

class DataInputData(BaseModel): 
    input_path: Union[str, List[str]] = Field(..., min_length=1)
    output_path: str

class Rolling_data(BaseModel): 
    estrategia: EstrategiaRolling
    feature_columns: List[str] = Field(..., min_length=1) 
    window_sizes: List[int] = Field(..., min_length=1)
    lags: List[int] = Field(..., min_length=1)
    
    @field_validator('feature_columns')
    def columnas_unicas(cls, v): 
        if len(v) != len(set(v)): 
            logger.error('Las columnas deben de ser unicas')
            raise ValueError('Las columnas deben de ser unicas')
        return v
    
    @field_validator('window_sizes')
    def tamaño_window(cls, v): 
        for valor in v: 
            if valor <= 0: 
                logger.error('El valor de la venatana tiene que ser mayor a 0')
                raise ValueError('El valor de la venatana tiene que ser mayor a 0')
        return v
    
    @field_validator('lags')
    def lags(cls, v): 
        for valor in v: 
            if valor < 0: 
                logger.error('El valor de la venatana tiene que ser un número positivo')
                raise ValueError('El valor de la venatana tiene que ser un número positivo')
        return v

class Ml(BaseModel): 
    learning_rate: float = Field(..., ge=0)
    scaler: EstretegiaEscalado
    feature_columns: List[str] = Field(..., min_length=1)
    aggregations: Dict[str, EstretegiaAgregaciones]
    
    @field_validator('feature_columns')
    def columnas_validas(cls, v): 
        if len(v) != len(set(v)): 
            logger.error('Las columnas deben de ser unicas')
            raise ValueError('Las columnas deben de ser unicas')
        return v

class Join(BaseModel): 
    join_filter: bool
    join_on: str
    join_type: EstrategiaJoin
    valor_then: str
    valor_otherwise: str
    filter_post_join: List[str] = Field(..., min_length=1)

class PipelineValidator(BaseModel): 
    compute : Compute
    manage_estrategy: ManageEstrategy
    data_input_output: DataInputData
    rolling_data : Rolling_data
    ml : Ml
    join_data : Join
