#Importar las librerías importantes 
import polars as pl 
import time
import ray 
import pyarrow as pa 
import logging 
import psutil
import yaml
from pydantic import BaseModel, field_validator, Field
from pathlib import Path
from dataclasses import dataclass
from typing import List
from enum import Enum

#Configurar el logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class EstrategiaData(str, Enum): 
    CLEAN = 'clean'
    WINDOW = 'window'
    AGG = 'agg'
    GROUP = 'group'

class EstrategiaClean(str, Enum): 
    DROP_NULLS = 'drop_nulls'

class EstrategiaWindow(str, Enum): 
    ROLLING_MEAN = 'rolling_mean'
    ROLLING_SUM = 'rolling_sum'

class EstrategiaAgg(str, Enum): 
    MEAN = 'mean'
    SUM = 'sum'
    MIN = 'min'
    MAX = 'max'

@dataclass
class EstarategiaGeneral: 
    data = [x.value for x in EstrategiaData]
    clean = [x.value for x in EstrategiaClean]
    window = [x.value for x in EstrategiaWindow]
    agg = [x.value for x in EstrategiaAgg]

class Etl(BaseModel): 
    input_path: str
    output_paht: str
    stages: List[dict] = Field(min_length=1)
    
    @field_validator('stages')
    def validar_lista(cls, v): 
        pass

