#Importacion de librerías 
import tomli
import tomlkit
from prefect import task, flow
import logging
from typing import Dict, Any, Union
from pathlib import Path

#Configuracion loggings
logger = logging.getLogger(__name__)

class LoadWriteToml: 
    def __init__(self, archivo_entrada: str):
        self.archivo = Path(archivo_entrada)
    
    @task
    def leer_toml(self) -> Dict[str, Any]: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo} no existe')
        try: 
            with open(self.archivo, 'rb') as file: 
                cargar = file.read()
                
                if not cargar.strip(): 
                    logger.error('El archivo está vacío')
                    raise ValueError('El archivo está vacío')
                
                datos = tomli.loads(cargar.decode('utf-8'))
            logger.info('Se leyó bien el archivo Toml')
            return datos
        except tomli.TOMLDecodeError: 
            logger.error('El archivo Toml está corrupto')
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')
    
    @task
    def guardar_toml(self, archivo_salida: str, datos: Dict[str, Any]): 
        if not datos: 
            logger.error('No hay datos para guardar')
            raise ValueError('No hay datos para guardar')
        archivo = Path(archivo_salida)
        try: 
            archivo.parent.mkdir(parents=True, exist_ok=True)
            with open(archivo, 'xb') as file: 
                escritura = tomlkit.dumps(datos)
                file.write(escritura.encode('utf-8'))
            logger.info(f'Se guardo correctamente el archivo {archivo_salida}')
        except FileExistsError: 
            logger.error(f'El archivo {archivo_salida} existe. No se puede sobreescribir en archivos existentes')
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')

class Operaciones: 
    def __init__(self, datos: Dict[str, Any]):
        if not datos: 
            logger.error('El diccionario está vacío')
            raise ValueError('El diccionario está vacío')
        self.datos = datos
        self.num_multi = datos.get('multiplicacion', None).get('mul', 0)
    
    @task(retries=3, retry_delay_seconds=5)
    def suma(self) -> Union[int, float]: 
        suma = 0
        nom_num = self.datos.get('numeros', None)
        for var, num in nom_num.items(): 
            suma += num
        if suma > 50: 
            logger.error(f'La suma no puede ser mayor a 50. Resultado suma: {suma}')
            raise ValueError(f'La suma no puede ser mayor a 50. Resultado suma: {suma}')
        logger.info('Se sumaron los números del archivo toml')
        return suma
    
    @task
    def multiplicacion(self, numero: Union[int, float]) -> Dict[str, Union[int, float]]: 
        multiplicacion = numero * self.num_multi
        return {'resultado' : multiplicacion}

class Pipeline: 
    @flow(name='Pipeline de Suma y Multiplicación de Datos')
    def pipeline(archivo_entrada: str, archivo_salida: str): 
        lectura_guardar = LoadWriteToml(archivo_entrada=archivo_entrada)
        data = lectura_guardar.leer_toml()
        operaciones = Operaciones(datos=data)
        suma = operaciones.suma()
        multiplicacion = operaciones.multiplicacion(numero=suma)
        lectura_guardar.guardar_toml(archivo_salida=archivo_salida, datos=multiplicacion)
