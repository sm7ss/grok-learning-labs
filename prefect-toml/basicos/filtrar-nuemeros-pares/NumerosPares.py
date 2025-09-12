#importamos las librerías necesarias 
import tomli
import tomlkit
from prefect import task, flow 
import logging
from pathlib import Path
from typing import Dict, Any

#Logging básico
logger = logging.getLogger(__name__)

class FiltrarPares: 
    def __init__(self, archivo_entrada: str):
        self.archivo = Path(archivo_entrada)
    
    @task
    def lectura_toml(self) -> Dict[str, Any]: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo} no existe')
        
        try: 
            with open(self.archivo, 'rb') as file: 
                lectura = file.read()
                
                if not lectura.strip(): 
                    logger.error(f'El archivo {self.archivo} está vacío')
                    raise ValueError(f'El archivo {self.archivo} está vacío')
                
                lectura_toml = tomli.loads(lectura.decode('utf-8'))
            logger.info('Se leyó el archivo toml correctamente')
            return lectura_toml
        except tomli.TOMLDecodeError as e: 
            logger.error(f'Ocurrio un error al querer leer el archivo {self.archivo} tipo {e}')
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')
    
    @task
    def filtrar_pares(self, datos: Dict[str, Any], ordenar: bool=False) -> Dict: 
        if not datos: 
            logger.error('No hay datos disponibles')
            raise ValueError('No hay datos disponibles')
        dic = {}
        for data, diccionario in datos.items(): 
            for val, lista in diccionario.items(): 
                pares = [x for x in lista if x%2==0]
                if pares and ordenar: 
                    dic[val] = sorted(pares)
                else: 
                    dic[val] = pares
        if not dic: 
            logger.error(f'El diccionario está vacío. No se encontraron números pares')
            raise ValueError(f'El diccionario está vacío. No se encontraron números pares')
        
        diccionario = {}
        diccionario['numeros_pares'] = dic
        return diccionario
    
    @task
    def guardar_toml(self, datos: Dict[str, Any], archivo_salida: str): 
        if not datos: 
            logger.error('No hay datos disponible')
            raise ValueError('No hay datos disponible')
        archivo = Path(Path(archivo_salida))
        try: 
            archivo.parent.mkdir(parents=True, exist_ok=True)
            with open(archivo, 'xb') as file: 
                guardar = tomlkit.dumps(datos)
                file.write(guardar.encode('utf-8'))
            logger.info('Se guardo exitosamente el Toml')
        except FileExistsError: 
            logger.error(f'El archivo {archivo_salida} existe por lo que no se puede sobreescribir')
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')
    
    @flow(name='Pipeline para Números Pares')
    def Pipeline(self, archivo_salida: str, ordenar: bool=False): 
        datos = self.lectura_toml()
        datos_salida = self.filtrar_pares(datos=datos, ordenar=ordenar)
        self.guardar_toml(datos=datos_salida, archivo_salida=archivo_salida)
