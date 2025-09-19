# Importacion de librerías importantes 
import psutil 
import time
from typing import Callable, Dict, Any
import threading
import logging

# Configuracion del logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class ResourceProfiling: 
    def __init__(self, intervalo: float=0.1):
        self.intervalo = intervalo 
        self.metricas = {
            'cpu_per_core' : [], 
            'memory_rss_mb' : [], 
            'io_read_mb' : [], 
            'io_write_mb' : [], 
            'thread_count': [], 
            'swap_used_mb' : [], 
            'net_sent_mb' : [], 
            'net_recv_mb' : [], 
            'cpu_time_user' : [], 
            'cpu_time_system' : []
        }
        self.monitorear = False
        self.procesador = psutil.Process()
        self.inicio_net = psutil.net_io_counters() 
        self.inicio_io = self.procesador.io_counters() if hasattr(self.procesador, 'io_counters') else None
    
    def muestra(self) -> None: 
        try: 
            cpu_por_nucleo = psutil.cpu_percent(interval=None, percpu=True)
            self.metricas['cpu_per_core'].append(cpu_por_nucleo)
            
            mem_info = self.procesador.memory_info()
            self.metricas['memory_rss_mb'].append(mem_info.rss / (1024**2))
            
            if self.inicio_io: 
                io_now = self.procesador.io_counters()
                lectura_mb = (io_now.read_bytes - self.inicio_io.read_bytes) / (1024**2)
                escritura_mb = (io_now.write_bytes - self.inicio_io.write_bytes) / (1024**2)
                self.metricas['io_read_mb'].append(lectura_mb)
                self.metricas['io_write_mb'].append(escritura_mb)
            
            self.metricas['thread_count'].append(self.procesador.num_threads())
            
            sawp = psutil.swap_memory()
            self.metricas['swap_used_mb'].append(sawp.used / (1024**2))
            
            net_ahora = psutil.net_io_counters()
            envio_mb = (net_ahora.bytes_sent - self.inicio_net.bytes_sent) / (1024**2)
            reciv_mb = (net_ahora.bytes_recv - self.inicio_net.bytes_recv) / (1024**2)
            self.metricas['net_sent_mb'].append(envio_mb)
            self.metricas['net_recv_mb'].append(reciv_mb)
            
            cpu_tiempo = self.procesador.cpu_times() 
            self.metricas['cpu_time_user'].append(cpu_tiempo.user)
            self.metricas['cpu_time_system'].append(cpu_tiempo.system)
            
        except (psutil.NoSuchProcess, psutil.AccessDenied): 
            logger.warning('Ocurrio una excepcion para el monitoreo de la muestra, pudo ser por acceso denegado o por no existir uno de los procesos hechos')
            pass
    
    def thread_monitoreo(self) -> None: 
        while self.monitorear: 
            self.muestra()
            time.sleep(self.intervalo)
    
    def inicio(self) -> None: 
        self.monitorear = True
        self.thread = threading.Thread(target=self.thread_monitoreo, daemon=True)
        self.thread.start()
    
    def final(self) -> Dict[str, Any]: 
        self.monitorear = False
        if hasattr(self, 'thread'): 
            self.thread.join(timeout=1.0)
        
        resultados = {}
        for key, value in self.metricas.items(): 
            if len(value) > 0: 
                resultados[f'{key}_mean'] = sum(value) / len(value)
                resultados[f'{key}_max'] = max(value)
                resultados[f'{key}_min'] = min(value)
                resultados[f'{key}_muestras'] = len(value)
            else: 
                continue
        return resultados
    
    def profile(self, func: Callable, *args, **kwargs) -> Dict[str, Any]: 
        self.inicio()
        tiempo_inicio = time.time()
        
        try: 
            resultado = func(*args, **kwargs)
        finally: 
            timepo_final = time.time()
            metricas = self.final()
            metricas['wall_time_s'] = timepo_final - tiempo_inicio
        
        return {
            'resultado' : resultado,
            'metricas' : metricas
        }
