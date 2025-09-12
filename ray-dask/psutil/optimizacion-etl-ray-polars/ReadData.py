#Importacion de librerías importante
import logging
import yaml 
import tomli 
from pathlib import Path
from pydantic import BaseModel
from Validator import etl

#Configuracion del logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(messsage)s')
logger = logging.getLogger(__name__)

class ReadToml: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
    
    def read_toml(self) -> BaseModel:
        nombre_archivo = self.archivo.name 
        try: 
            with open(self.archivo, 'rb') as file: 
                read = tomli.load(file)
            logger.info(f'Se leyó correctamente el archivo {nombre_archivo}')
            validador = etl(**read['etl'])
            logger.info(f'Se validó correctamente el archivo {nombre_archivo}')
            return validador
        except tomli.TOMLDecodeError: 
            logger.error(f'El archivo {nombre_archivo} está corrupto')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error al querrer leer el archivo {nombre_archivo}: {str(e)}')
            raise

class ReadYaml: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
    
    def read_yaml(self) -> BaseModel: 
        nombre_archivo = self.archivo.name
        try: 
            with open(self.archivo, 'r') as file: 
                read = yaml.safe_load(file)
            logger.info(f'Se leyó correctamente el archivo {nombre_archivo}')
            validador = etl(**read['etl'])
            logger.info(f'Se validó correctamente el archivo {nombre_archivo}')
            return validador
        except yaml.YAMLError: 
            logger.error(f'El archivo {nombre_archivo} está corrupto')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error al querrer leer el archivo {nombre_archivo}: {str(e)}')
            raise

class ReadFile: 
    def __init__(self, archivo: str):
        self.archivo = archivo
        self.toml = ReadToml(archivo=self.archivo)
        self.yaml = ReadYaml(archivo=self.archivo)
    
    def read_file(self) -> BaseModel: 
        terminacion = Path(self.archivo).suffix
        if terminacion == '.toml': 
            model = self.toml.read_toml()
        elif terminacion in ['.yaml', '.yml']: 
            model = self.yaml.read_yaml()
        else: 
            logger.error(f'El archivo {self.archivo} no tiene una terminacion valida. Debe ser un archivo tipo toml o yaml')
            raise ValueError(f'El archivo {self.archivo} no tiene una terminacion valida. Debe ser un archivo tipo toml o yaml')
        return model
