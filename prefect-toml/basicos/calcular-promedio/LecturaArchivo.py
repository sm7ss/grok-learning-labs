#Importacion de librerías necesarias
import tomli
import tomlkit
import logging
from pathlib import Path
from prefect import task, flow
from typing import Union, Dict

#Configuracion del logging
logger = logging.getLogger(__name__)

@task
def lectura_toml(archivo_entrada: Path): 
    if not archivo_entrada.exists(): 
        logger.error(f'El archivo {archivo_entrada} no existe')
        raise FileNotFoundError(f'El archivo {archivo_entrada} no existe')
    
    try:
        with open(archivo_entrada, 'rb') as file: 
            toml = file.read()
            
            if not toml.strip(): 
                logger.error(f'El archivo {archivo_entrada} está vacío')
                raise ValueError(f'El archivo {archivo_entrada} está vacío')
            
            load_toml = tomli.loads(toml.decode('utf-8'))
    except tomli.TOMLDecodeError as e: 
        logger.error(f'Ocurrio un error al querrer leer el archivo TOML: {e}')
    except Exception as e: 
        logger.error(f'Ocurrio un error: {e}')
    
    logger.info(f'Se leyo el archivo {archivo_entrada}')
    return load_toml

@task
def promedio_numeros(datos: list[int]) -> Union[int, float]: 
    if not datos: 
        logger.error('El diccionario está vacío')
        raise ValueError('El diccionario está vacío')
    
    suma = 0 
    for numero in datos:
        suma += numero 
    logger.info('Se calculo del promedio')
    return suma/len(datos)

@task
def guardar_toml(archivo_salida: Path, resultado: Dict[str, Union[int, float]]):
    imputacion = {'resultado': resultado} 
    try:
        archivo_salida.parent.mkdir(parents=True, exist_ok=True)
        with open(archivo_salida, 'xb') as file: 
            toml = tomlkit.dumps(imputacion)
            file.write(toml.encode('utf-8'))
        logger.info(f'Se guardaron los cambios en el archivo {archivo_salida}')
    except FileExistsError: 
        logger.error(f'El archivo {archivo_salida} existe')
    except Exception as e: 
        logger.error(f'Ocurrio un error: {e}')

@flow(name='Promedio de una lista')
def Pipeline(archivo_entrada: Path, archivo_salida: Path): 
    config = lectura_toml(archivo_entrada=archivo_entrada)
    nombre = config['datos']
    
    promedio = promedio_numeros(datos=nombre['numeros'])
    
    guardar_toml(archivo_salida=archivo_salida, resultado=promedio)
