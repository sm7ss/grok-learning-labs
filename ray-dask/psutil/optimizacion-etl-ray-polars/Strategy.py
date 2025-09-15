#Importamos las librerías necesarias 
from enum import Enum

class EstrategiaClean(str, Enum): 
    DROP_NULLS = 'drop_nulls'

class EstrategiaWindow(str, Enum): 
    ROLLING_MEAN = 'rolling_mean'
    ROLLING_SUM = 'rolling_sum'
    ROLLING_MIN = 'rolling_min'
    ROLLING_MAX = 'rolling_max'

class EstrategiaAgg(str, Enum): 
    MEAN = 'mean'
    SUM = 'sum'
    MIN = 'min'
    MAX = 'max'

class EstrategiaJoin(str, Enum): 
    LEFT = 'left'
    RIGHT = 'right'
    INNER = 'inner'
    OUTER = 'outer'
