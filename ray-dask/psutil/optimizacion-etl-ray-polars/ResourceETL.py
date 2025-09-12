import psutil 
import time
from dataclasses import dataclass

@dataclass
class Recursos: 
    cpu_logica: int
    cpu_fisica: int
    ram_disponible_GB: float
    uso_cpu: float 
    
    @classmethod
    def ray_info_available(cls): 
        return cls(
            ram_disponible=psutil.virtual_memory().available / (1024**3), 
            tamaño_de_memoria=min()
        )

class Rescursos: 
    def __init__(self):
        self.cpu_logica = psutil.cpu_count(logical=True)
        self.cpu_fisica = psutil.cpu_count(logical=False)
        self.ram_disponible = psutil.virtual_memory().available / (1024**3)
        self.uso_cpu = psutil.cpu_percent(interval=0.5)
    
    @classmethod
    def ray_info_disponible(self, cls): 
        return cls(
            object_store_memory=min(self.ram_disponible*0.3, self.ram_disponible*(1024**3)*1.5), 
            
            
        )

class MonitorearRecursos: 
    @staticmethod
    def resource(): 
        pass
    
    @staticmethod
    def monitoreo_recursos(): 
        pass

#Wip
