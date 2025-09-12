#Importamos librerías
import logging
import polars as pl
from pydantic import BaseModel
from typing import List, Type
from ErrorHandler import EdgeCases

#Configuracion logging
logging.basicConfig(level=logging.basicConfig, format='%(asctime)s-%(levelname)s-(message)s')
logger = logging.getLogger(__name__)

class RollingDataExpresion: 
    def __init__(self, df: pl.LazyFrame, model: Type[BaseModel]) -> None:
        self.df_lazy = df
        self.rolling_data = model.rolling_data
        self.estrategia = self.rolling_data.estrategia
        self.columnas = self.rolling_data.feature_columns
        self.window = self.rolling_data.window_sizes
        self.lag = self.rolling_data.lags
        self.edge_cases = EdgeCases()
        
        self.edge_cases.parametro_lista(lista=self.df_lazy)
        self.edge_cases.columna_no_existente_lista(columnas=self.columnas, schema=self.df_lazy.collect_schema())
        self.edge_cases.longitud_de_lista_diferente(columnas=self.columnas, window=self.window, lag=self.lag)
    
    def expresiones_rolling(self) -> List[pl.Expr]: 
        try: 
            expresiones = [
            getattr(pl.col(self.columnas[rango]), self.estrategia)
            (window_size=self.window[rango])
            .shift(self.lag[rango])
            .over(self.columnas[rango])
            .alias(f'{self.estrategia}_{self.columnas[rango]}')
            for rango in range(len(self.columnas))
            ]
        except AttributeError: 
            logger.error(f'La estrategia {self.estrategia} no es valida')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error durante la ejecución: {e}')
            raise
        
        return expresiones

class ScalerData: 
    def __init__(self, df: pl.LazyFrame, model: Type[BaseModel]) -> None:
        self.df_lazy = df
        self.config_ml = model.ml
        self.edge_cases = EdgeCases()
    
    def minmax_data(self) -> List[pl.Expr]:
        schema = self.df_lazy.collect_schema()
        for col in self.config_ml.feature_columns: 
            self.edge_cases.columna_no_existente_schema(col=col, schema=schema)
            self.edge_cases.columna_no_numerica(col=col, schema=schema)
        
        minmax_lista = [
            (pl.col(col) - pl.col(col).min()) / (pl.col(col).max() - pl.col(col).min())
            .alias(f'MinMax_{col}')
            for col in self.config_ml.feature_columns
        ]
        return minmax_lista
    
    def agregaciones_data(self) -> List[pl.Expr]: 
        schema = self.df_lazy.collect_schema()
        for col in self.config_ml.aggregations.keys(): 
            self.edge_cases.columna_no_existente_schema(col=col, schema=schema)
            self.edge_cases.columna_no_numerica(col=col, schema=schema)
        
        lista_agregaciones = [
            getattr(pl.col(col), agregacion.value)()
            .alias(f'{agregacion}_{col}')
            for col, agregacion in self.config_ml.aggregations.items()
        ]
        
        return lista_agregaciones
    
    def standard_data(self) -> List[pl.Expr]: 
        schema = self.df_lazy.collect_schema()
        for col in self.config_ml.feature_columns: 
            self.edge_cases.columna_no_existente_schema(col=col, schema=schema)
            self.edge_cases.columna_no_numerica(col=col, schema=schema)
        
        lista_standard_data = [
            ((pl.col(col)) - pl.col(col).mean()) / (pl.col(col).std())
            .alias(f'Zscore_{col}')
            for col in self.config_ml.feature_columns
        ]
        
        return lista_standard_data

class JoinData: 
    def __init__(self, df: List[str], model: Type[BaseModel]) -> None:
        self.edge_cases = EdgeCases()
        
        self.edge_cases.longitud_lista_para_join(lista=df)
        
        self.config = model.join_data
        self.df_lazy_1, self.df_lazy_2 = df
        
        self.schema_df_1 = self.df_lazy_1.collect_schema()
        self.schema_df_2 = self.df_lazy_2.collect_schema()
    
    def post_join(self) -> List[pl.Expr]:
        
        lista_post_join = []
        
        try: 
            for expresion in self.config.filter_post_join: 
                columna, comparacion, valor = expresion.split()
                
                self.edge_cases.columna_no_existente_schema(col=columna, schema=self.schema_df_1)
                self.edge_cases.columna_no_existente_schema(col=columna, schema=self.schema_df_2)
                self.edge_cases.columna_no_numerica(col=columna, schema=self.schema_df_1)
                self.edge_cases.columna_no_numerica(col=columna, schema=self.schema_df_2)
                
                lista_post_join.append(
                    pl.when(pl.sql_expr(expresion))
                    .then(pl.lit(self.config.valor_then))
                    .otherwise(pl.lit(self.config.valor_otherwise))
                    .alias(f'Categoria_{columna}')
                )
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer filtrar los datos: {e}')
            raise
        
        return lista_post_join
    
    def join_data(self) -> pl.LazyFrame: 
        
        self.edge_cases.columna_no_numerica(col=self.config.join_on, schema=self.schema_df_1)
        self.edge_cases.columna_no_numerica(col=self.config.join_on, schema=self.schema_df_2)
        
        if self.config.join_filter:
            df_1_filtrado = self.df_lazy_1.with_columns(self.post_join())
            df = df_1_filtrado.join(self.df_lazy_2, on=self.config.join_on, how=self.config.join_type)
            logger.info(f'Se unieron ambos LazyFrames filtrados en {self.config.join_on}')
        else: 
            df = self.df_lazy_1.join(self.df_lazy_2, on=self.config.join_on, how=self.config.join_type)
            logger.info(f'Se unieron ambos LazyFrames en {self.config.join_on}')
        return df

