from enum import Enum
from pydantic import BaseModel

class S(str, Enum):
    am='am'
    pm='pm'

class K(BaseModel):
    k:S
    z:str

    class Config:  
        use_enum_values = True  # <--

a = K(k='am', z='rrrr')
print(a.dict())