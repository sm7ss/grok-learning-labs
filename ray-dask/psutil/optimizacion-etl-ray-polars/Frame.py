#Importamos librerías 
import ray 
from ReadData import ReadFile

class GetFrame: 
    def __init__(self, archivo: str):
        model = ReadFile(archivo=archivo).read_file()
        self.model_file = model.input_path
    
    def ray_frame(self) -> ray.data.Dataset: 
        if self.model_file.suffix == '.csv': 
            return ray.data.read_csv(self.model_file)
        else: 
            return ray.data.read_parquet(self.model_file)
