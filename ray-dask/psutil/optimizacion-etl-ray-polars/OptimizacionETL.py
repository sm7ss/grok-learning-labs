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
    GROUP_BY = 'group_by'

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

def validacion_data(lista): 
    for indice, diccionario in enumerate(lista): 
        name = diccionario.get('name', lista[indice])
        if name not in [x.value for x in EstrategiaData]: 
            logger.error(f'La estrategia {name} no es valida')

def estrategia_valida(lista): 
    for diccionario in lista: 
        ops = diccionario.get('ops', None)
        for estrategia in ops: 
            estrategia_data = estrategia.split(': ')[0].strip()
            if estrategia_data in [x.value for x in EstrategiaWindow]: 
                continue
            elif estrategia_data in [x.value for x in EstrategiaClean]: 
                continue
            elif estrategia_data in [x.value for x in EstrategiaData]: 
                continue
            else: 
                logger.error(f'La estrategia {estrategia_data} no está disponible')
                raise ValueError(f'La estrategia {estrategia_data} no está disponible')

def validacion_diccionario(df: pl.LazyFrame, lista): 
    group = []
    window = []
    clean = []
    schema = df.collect_schema()
    for li in lista: 
        ops = li.get('ops', None)
        for elemento in ops: 
            if '{' in str(elemento): 
                if 'group_by' in str(elemento): 
                    lit = elemento.split(', ')
                    for li in lit: 
                        nueva_lista = li.split(': ')
                        lis = [x.strip('{').strip('}') for x in nueva_lista]
                        
                        for objeto in lis: 
                            if objeto in [x.value for x in EstrategiaData]: 
                                group.append(objeto)
                            elif objeto in [x.value for x in EstrategiaAgg]: 
                                group.append(objeto)
                            elif objeto in schema: 
                                group.append(objeto)
                            else: 
                                logger.error(f'{objeto} no se encunetra en el DataFrame o no es valido')
                                raise ValueError(f'{objeto} no se encunetra en el DataFrame o no es valido')
                elif 'rolling' in str(elemento): 
                    elemento = elemento.strip('}').split('{')
                    estrategia = elemento[0].strip(': ')
                    window.append(estrategia)
                    validacion = elemento[1].split(', ')
                    for val in validacion: 
                        validacion = val.split(': ')
                        if validacion[1] in schema: 
                            if schema[validacion[1]].is_numeric(): 
                                window.append(validacion)
                        elif int(validacion[1]): 
                            window.append(validacion)
                        else: 
                            logger.error(f'{validacion[1]} no es valida, pues no puede existir la columna o es una columna no numerica')
            else: 
                elementos = elemento.split(': ')
                for objeto in elementos: 
                    if objeto in schema: 
                        clean.append(objeto)
                    elif objeto in [x.value for x in EstrategiaClean]: 
                        clean.append(objeto)
                    else: 
                        logger.error(f'{objeto} no es valido o no se encuentra en el DataFrame')
                        raise ValueError(f'{objeto} no es valido o no se encuentra en el DataFrame')



