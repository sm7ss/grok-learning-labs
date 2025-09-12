#Importacion de librerías importantes 
import polars as pl
import dask
from dask.distributed import Client
import logging
from prefect import task, flow
from typing import Type, Union, Tuple, List
from pydantic import BaseModel

from ExtractData import ReadToml, ExtractLazyFrame
from FeautureEngineer import RollingDataExpresion, ScalerData, JoinData
from Pydantic import PipelineValidator
from Estrategy import EstrategiaManage, EstretegiaEscalado

#Configuracion del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class PipelineData: 
    def __init__(self, archivo: str) -> None:
        self.archivo = archivo
    
    @task(retries=3, retry_delay_seconds=5)
    def get_model(self, validador_model: Type[BaseModel]) -> Type[BaseModel]: 
        return ReadToml(archivo=self.archivo).read_toml(validador_model=validador_model)
    
    @task
    def get_lazyframe(self, model: Type[BaseModel]) -> Union[str, list]: 
        return ExtractLazyFrame(model=model).extract_data()
    
    @flow(name='Pipeline De Extracción de Datos')
    def pipeline(self) -> Tuple[BaseModel, pl.LazyFrame]: 
        model = self.get_model(validador_model=PipelineValidator)
        df_lazy = self.get_lazyframe(model=model)
        return model, df_lazy

class PipelineFeatureEngineer: 
    def __init__(self, list_features: Union[pl.LazyFrame, Type[BaseModel]]) -> None:
        self.model, self.df_lazy = list_features
    
    @task
    def rolling_data(self) -> List[pl.Expr]: 
        return RollingDataExpresion(df=self.df_lazy, model=self.model)
    
    @task
    def scaler_data(self) -> List[pl.Expr]: 
        return ScalerData(df=self.df_lazy, model=self.model)
    
    @task
    def join_data(self) -> List[pl.Expr]: 
        return JoinData(df=self.df_lazy, model=self.model)
    
    @flow(name='Piepeline de estrategias para Features Engineer')
    def pipeline(self, estrategia: EstrategiaManage) -> List[pl.Expr]: 
        match estrategia: 
            case EstrategiaManage.rolling_data: 
                df = self.rolling_data().expresiones_rolling()
            case EstrategiaManage.ml: 
                if self.model.ml.scaler == EstretegiaEscalado.min_max: 
                    df = self.scaler_data().minmax_data()
                elif self.model.ml.scaler == EstretegiaEscalado.standard: 
                    df = self.scaler_data().standard_data()
                else: 
                    df = self.scaler_data().agregaciones_data()
            case EstrategiaManage.join_data: 
                df = self.join_data().join_data()
        return df

class PipelineOrquestador: 
    def __init__(self, archivo: str) -> None:
        self.archivo = archivo 
    
    @task
    def lista(self) -> Tuple[Union[str, list], Type[BaseModel]]: 
        return PipelineData(archivo=self.archivo).pipeline()
    
    @flow(name='Orquestador')
    def pipeline_general(self, estrategia: EstrategiaManage) -> None: 
        model, df_lazy = self.lista()
        
        client = Client(memory_limit=model.compute.memory_limit if model.compute.use_dask else None)
        
        features_data = PipelineFeatureEngineer(list_features=self.lista()).pipeline(estrategia=estrategia)
        
        if estrategia == 'join_data': 
            df = features_data
        else: 
            df = df_lazy.with_columns(features_data)
        
        df_lazy = df.collect(engine=model.compute.streaming)
        
        df_lazy.write_parquet(
            model.data_input_output.output_path, 
            compression='zstd', 
            partition_by=model.compute.partition_by if model.compute.use_dask else None
        )

