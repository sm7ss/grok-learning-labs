import ray
import polars as pl 
import psutil
from pydantic import BaseModel
from pathlib import Path

class PolarsFrame: 
    @staticmethod
    def leer_eager(path: Path) -> pl.DataFrame: 
        if path.suffix == '.csv': 
            return pl.read_csv(path)
        else:
            return pl.read_parquet(path)
    
    @staticmethod
    def leer_lazy(path: Path) -> pl.LazyFrame: 
        if path.suffix == '.csv': 
            return pl.scan_csv(path) 
        else: 
            return pl.scan_parquet(path)

class RayFrame: 
    @staticmethod
    def leer_ray(path: Path) -> ray.data.Dataset: 
        if path.suffix == '.csv': 
            return ray.data.read_csv(path)
        else: 
            return ray.data.read_parquet(path)

class PipelineFrame: 
    def __init__(self, model: BaseModel):
        self.model_data = model.data
    
    def frame_decision(self, tamaño: int, memory: int) -> str: 
        if tamaño > (5*memory): 
            raise ValueError(f'El archivo tiene un tamaño de {tamaño} bytes y es demasiado grande para la RAM disponible {memory} bytes')
        
        umbral_eager = 0.1 * memory
        umbral_lazy = 0.75 * memory 
        
        if tamaño < umbral_eager: 
            return 'eager'
        elif tamaño < umbral_lazy: 
            return 'lazy'
        else:
            return 'ray'
    
    def pipeline_frame(self) -> ray.data.Dataset: 
        if self.model_data.input_path:
            lista_frames = []
            for archivo in self.model_data.input_path:
                tamaño= archivo.stat().st_size
                memory= psutil.virtual_memory().available
                decision = self.frame_decision(tamaño=tamaño, memory=memory)
                
                if decision == 'eager': 
                    lista_frames.append(PolarsFrame.leer_eager(path=archivo))
                elif decision == 'lazy': 
                    lista_frames.append(PolarsFrame.leer_lazy(path=archivo))
                elif decision == 'ray': 
                    lista_frames.append(RayFrame.leer_ray(path=archivo))
            
            return lista_frames
        
        archivo = self.model_data.input_path
        tamaño = archivo.stat().st_size 
        memory = psutil.virtual_memory().available
        decision = self.frame_decision(tamaño=tamaño, memory=memory)
        
        if decision == 'eager': 
            frame = PolarsFrame.leer_eager(path=archivo)
        elif decision == 'lazy': 
            frame = PolarsFrame.leer_lazy(path=archivo)
        elif decision == 'ray': 
            frame = RayFrame.leer_ray(path=archivo)
        return frame 

