import psutil 
import time

class ResorceConfig: 
    @staticmethod
    def init_ray(tamaño_archivo: int): 
        ram_total = psutil.virtual_memory().available
        
        cpu_fisico= max(1, psutil.cpu_count(logical=False)-1)
        
        object_store_memory = max(ram_total*0.3, tamaño_archivo*1.5)
        
        return {
            "num_cpus": cpu_fisico, 
            'object_store_memory': object_store_memory 
        }

class ResourceMonitoring: 
    @staticmethod
    def monitoreo(funcion, *args, **kwargs): 
        tiempo_inicio = time.time()
        memoria_inicio = psutil.virtual_memory().used
        cpu_inicio = psutil.cpu_percent()
        
        resultado = funcion(*args, **kwargs)
        
        tiempo_final = time.time()
        memoria_final = psutil.virtual_memory().used
        cpu_final = psutil.cpu_percent()
        
        return {
            'resultado':resultado, 
            'timepo_segundos':tiempo_final-tiempo_inicio, 
            'memoria_GB': (memoria_final-memoria_inicio) / (1024**3), 
            'cpu_porcentaje_prom': (cpu_inicio+cpu_final)/2
        }

