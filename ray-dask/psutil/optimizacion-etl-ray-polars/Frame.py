#Importacion de librerias importantes 
import ray
import polars as pl 
from pathlib import Path

class EagerFramStrategy: 
    @staticmethod
    def leer(path: Path) -> pl.DataFrame: 
        if path.suffix == '.csv': 
            return pl.read_csv(path)
        else:
            return pl.read_parquet(path)
    
    @staticmethod
    def leer(path: Path) -> pl.LazyFrame: 
        if path.suffix == '.csv': 
            return pl.scan_csv(path) 
        else: 
            return pl.scan_parquet(path)

class RayFrame: 
    @staticmethod
    def leer(path: Path) -> ray.data.Dataset: 
        if path.suffix == '.csv': 
            return ray.data.read_csv(path)
        else: 
            return ray.data.read_parquet(path)

