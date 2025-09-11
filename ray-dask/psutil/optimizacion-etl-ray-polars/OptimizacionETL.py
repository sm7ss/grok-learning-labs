#Importar las librerías importantes 
import polars as pl 
import time
import ray 
import pyarrow as pa 
import logging 
import psutil
import yaml
from pydantic import BaseModel, field_validator, Field
from pathlib import Path
from dataclasses import dataclass
from typing import List
from enum import Enum
#Wip 

