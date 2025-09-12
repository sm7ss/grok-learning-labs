#Importacion de librerias
import tomli
import polars as pl
import logging
from prefect import task, flow
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from typing import Literal

#Configuracion logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class Config(BaseModel): 
    file_csv: str
    output_file: str
    streaming: Literal['default', 'streaming']

class RollingColumns(BaseModel):
    rolling_columns: List[str] = Field(..., min_length=1)
    window_size: int = Field(..., gt=0)
    window_shift: int

Models_Map = {
    'Config' : Config, 
    'Rolling_Columns' : RollingColumns
}

class ReadTOML: 
    def __init__(self, archivo:str) -> None:
        self.archivo = Path(archivo)
    
    def read_toml(self) -> Dict[str, Any]: 
        if not self.archivo.exists(): 
            logger.error(f'El archivo {self.archivo.name} no existe')
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        try: 
            with open(self.archivo, 'rb') as file: 
                read = tomli.load(file)
            logger.info(f'Se leyó correctamente el archivo {self.archivo.name}')
            
            validador = {k: v(**read[k]) for k, v in Models_Map.items() if k in read}
            
            logger.info(f'Se valido correctamente el archivo {self.archivo.name}')
            return validador
        except tomli.TOMLDecodeError as e: 
            logger.error(f'El archivo {self.archivo.name} está corrupto. Error: {e}')
            raise
        except Exception as e: 
            logger.error(f'Ocurrio un error en la ejecucion del archivo {self.archivo.name}: {e}')
            raise

class ETL: 
    def __init__(self, configuracion: Dict[str, Any]):
        self.config_general = configuracion['Config']
        self.rolling = configuracion['Rolling_Columns']
    
    def obtencion_lazyframe(self) -> pl.LazyFrame: 
        df_lazy = pl.scan_csv(self.config_general.file_csv)
        logger.info('Se creo correctamente el lazyframe del archivo')
        return df_lazy
    
    def window_shift(self, df: pl.LazyFrame) -> pl.LazyFrame: 
        df_schema = df.collect_schema()
        for col in self.rolling.rolling_columns: 
            if col not in df_schema: 
                logger.error(f'La columna {col} no existe en el DataFrame')
                raise ValueError(f'La columna {col} no existe en el DataFrame')
            
            if not df_schema[col].is_numeric(): 
                logger.error(f'La columna {col} no es una columna tipo numerica')
                raise ValueError(f'La columna {col} no es una columna tipo numerica')
        
        lista = []
        for col in self.rolling.rolling_columns: 
            lista.append(pl.col(col).rolling_mean(window_size=self.rolling.window_size).shift(self.rolling.window_shift).alias(f'Rolling_{col}'))
        
        df_lazy = df.with_columns(lista)
        
        logger.info(f'Se aplicaron ventanas deslizantes en las columnas {self.rolling.rolling_columns}')
        return df_lazy
    
    def guardar_toml(self, df: pl.LazyFrame): 
        df_lazy = df.collect(engine=self.config_general.streaming)
        df_lazy.write_parquet(self.config_general.output_file)
        logger.info(f'Se guardo el lazyframe en {self.config_general.output_file}')

class Pipeline: 
    def __init__(self, archivo: str):
        self.config = ReadTOML(archivo=archivo)
        self.read = self.config.read_toml()
        self.etl = ETL(configuracion=self.read)
    
    @task(retries=3, retry_delay_seconds=5)
    def obtencion(self) -> pl.LazyFrame: 
        return self.etl.obtencion_lazyframe()
    
    @task
    def window(self, df: pl.LazyFrame) -> pl.LazyFrame: 
        return self.etl.window_shift(df=df)
    
    @task
    def guardar(self, df: pl.LazyFrame): 
        return self.etl.guardar_toml(df=df)
    
    @flow(name='Pipeline para Window Deslizante')
    def pipeline(self): 
        df_lazy = self.obtencion()
        consulta_lazy = self.window(df=df_lazy)
        self.guardar(df=consulta_lazy)

