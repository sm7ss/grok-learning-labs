#Importar las librerías importantes 
import polars as pl 
import ray 
import pyarrow as pa 
import logging 
from pydantic import BaseModel
from ReadData import ReadFile
from Frame import GetFrame

#Configuracion del logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class CleanData: 
    def __init__(self, model: BaseModel):
        self.model_clean = model.clean_data
    
    def drop_nulls(self) -> pl.Expr: 
        return pl.col(self.model_clean.col).is_not_null().alias(f'{self.model_clean.col}_drop_nulls')

class WindowData: 
    def __init__(self, model: BaseModel):
        self.model_window = model.window_data
    
    def window_data(self) -> pl.Expr: 
        return (
            getattr(pl.col(self.model_window.col), self.model_window.operation)
            (self.model_window.window)
            .over(self.model_window.over)
            .alias(f'{self.model_window.operation}_{self.model_window.col}')
        )

class AggData: 
    def __init__(self, model: BaseModel):
        self.model_agg = model.agg_data
    
    def agg_data(self) -> pl.Expr: 
        return (
            getattr(pl.col(self.model_agg.col), self.model_agg.operation)
            ().alias(f'{self.model_agg.operation}_{self.model_agg.col}')
        )

class Pipeline: 
    def __init__(self, archivo: str):
        self.archivo = archivo
        self.model = ReadFile(archivo=self.archivo).read_file()
        self.agg_data = self.model.agg_data
    
    def batches(self, table: pa.Table) -> pa.Table: 
        clean = CleanData(model=self.model)
        window = WindowData(model=self.model)
        
        df_lazy = pl.from_arrow(table).lazy()
        df_lazy_filter = df_lazy.filter(clean.drop_nulls())
        df_lazy_transformed = df_lazy_filter.with_columns(window.window_data())
        
        return df_lazy_transformed.collect(engine='streaming').to_arrow()
    
    def group_batches(self, table: pa.Table) -> pa.Table: 
        agg = AggData(model=self.model)
        agg_expr = agg.agg_data()
        df_polars= pl.from_arrow(table).lazy()
        result= df_polars.group_by(self.agg_data.group_by).agg(agg_expr)
        return result.collect(engine='streaming').to_arrow()
    
    def pipeline_ray(self):
        with ray.init(num_cpus=4, object_store_memory=2*1e9):
            ds = GetFrame(archivo=self.archivo).ray_frame()
            
            ds_transformed = ds.map_batches(
                lambda batch: self.batches(table=batch), 
                batch_size=10000, 
                batch_format='pyarrow'
            )
            grouped = ds_transformed.groupby(self.agg_data.group_by)
            grouped_transformed = grouped.map_groups(
                lambda table: self.group_batches(table=table), 
                batch_format='pyarrow'
            )
            grouped_transformed.write_parquet(self.model.output_path.replace('.parquet', ''))
