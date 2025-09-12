# Importacin de librerias necesarias 
from pydantic import BaseModel, Field
import tomli
import logging
from typing import Dict, Any, List
from pathlib import Path

#Configuracion logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class Models(BaseModel): 
    lr: float 
    hidden_layers: List[int] = Field(..., min_length=1)

class ReadTOML: 
    def __init__(self, archivo: str):
        self.archivo = Path(archivo)
    
    def read_toml(self) -> Dict[str, Any]: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        
        try: 
            with open(self.archivo, 'rb') as file: 
                leer = tomli.load(file)
            logger.info(f'Se leyó correctamente el archivo {self.archivo.name}')
            validador = Models(**leer['models'])
            logger.info(f'Se valido correctamente el archivo {self.archivo.name}')
            return validador
        except tomli.TOMLDecodeError as e: 
            logger.error(f'El archivo {self.archivo.name} está corrupto: {e}')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error al querrer leer y validar el archivo {self.archivo.name}: {e}')
            raise


