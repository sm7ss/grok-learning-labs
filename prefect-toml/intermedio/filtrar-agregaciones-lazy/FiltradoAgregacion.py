#Importacion de librerías 
import tomli 
import logging
from prefect import flow, task
import polars as pl
from pathlib import Path
from typing import Dict, Any

#Configuracion del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class LoadTOML: 
    def __init__(self, archivo: str) -> None:
        self.path = Path(archivo)
    
    def load(self) -> Dict[str, Any]: 
        if not self.path.exists(): 
            logger.error(f'El archivo {self.path} no existe')
            raise FileNotFoundError(f'El archivo {self.path} no existe')
        
        try:     
            with open(self.path, 'rb') as file: 
                leer = file.read()
                
                if not leer.strip(): 
                    logger.error('El archivo esya vacio')
                    raise ValueError('El archivo esya vacio')
                
                cargar = tomli.loads(leer.decode('utf-8'))
            logger.info('Se cargo exitosamente el archivo')
            return cargar
        except tomli.TOMLDecodeError as e: 
            logger.error(f'Ocurrio un error al cargar el archivo Toml: {e}')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error: {e}')
            raise

class DataProcessor: 
    def __init__(self, diccionario: Dict[str, Any]):
        self.diccionario = diccionario['data']
    
    def read_csv(self) -> pl.LazyFrame: 
        if not self.diccionario: 
            logger.error('El diccionario está vacío')
            raise ValueError('El diccionario está vacío')
        archivo = Path(self.diccionario['input_path'])
        if not archivo.exists(): 
            logger.error(f'El archivo {archivo} no existe')
            raise ValueError(f'El archivo {archivo} no existe')
        df_lazy = pl.scan_csv(archivo)
        logger.info(f'Se leyó correctamente el archivo {archivo.name}')
        return df_lazy
    
    def data_filter(self, df: pl.LazyFrame) -> pl.LazyFrame: 
        valor_filtrado = self.diccionario['filter_value']
        columna_filtrada = self.diccionario['filter_column']
        agg_funcion = self.diccionario['agg_function']
        agrupacion = self.diccionario['group_by']
        
        lista= []
        if 'sum' in agg_funcion: 
            columna_a_agregar = agg_funcion.split('(')[1].split(')')[0]
            lista.append(pl.sum(columna_a_agregar).alias('Total_Edad'))
        else: 
            logger.error(f'La funcion de agregacion {columna_a_agregar} no está disponible')
            raise ValueError(f'La funcion de agregacion {columna_a_agregar} no está disponible')
        
        df_lazy = (
            df
            .filter(pl.col(columna_filtrada) == valor_filtrado)
            .group_by(agrupacion)
            .agg(
                lista
            )
        )
        logger.info(f"Datos filtrados por {columna_filtrada}={valor_filtrado} y agrupados por {agrupacion}")
        return df_lazy
    
    def load(self, df: pl.LazyFrame): 
        df = df.collect()
        df.write_parquet(self.diccionario['output_path'])
        logger.info('Se colectaron los datos del DataFrame y se guardaron correctamente')

class ETLPipeline: 
    def __init__(self, archivo: str):
        self.config = LoadTOML(archivo=archivo)
        self.config_dic = self.config.load()
        self.procesador = DataProcessor(diccionario=self.config_dic)
    
    @task(retries=3, retry_delay_seconds=5)
    def lectura_csv(self) -> pl.LazyFrame: 
        return self.procesador.read_csv()
    
    @task
    def ETL(self, df: pl.LazyFrame) -> pl.LazyFrame: 
        return self.procesador.data_filter(df=df)
    
    @task
    def guardar(self, df: pl.LazyFrame): 
        return self.procesador.load(df=df)
    
    @flow(name='Pipeline de Filtrado y agrupacion de Datos')
    def Pipeline(self): 
        df_lazy = self.lectura_csv()
        etl_procesador = self.ETL(df=df_lazy)
        self.guardar(df=etl_procesador)

