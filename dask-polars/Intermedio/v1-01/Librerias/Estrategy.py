#Importamos librerías necesarias
from enum import Enum

class EstrategiaManage(str, Enum): 
    rolling_data = 'rolling_data'
    ml = 'ml'
    join_data = 'join_data'

class EstrategiaRolling(str, Enum): 
    rolling_mean = 'rolling_mean'
    rolling_sum = 'rolling_sum'
    rolling_min = 'rolling_min'
    rolling_max = 'rolling_max'

class EstretegiaEscalado(str, Enum): 
    min_max = 'minmax'
    standard = 'standard'

class EstretegiaAgregaciones(str, Enum): 
    sum = 'sum'
    mean = 'mean'

class EstrategiaJoin(str, Enum): 
    inner = 'inner'
    left = 'left'
    right = 'right'
    outer = 'outer'