from pydantic_settings import BaseSettings
from pydantic import BaseModel, HttpUrl
from typing import Optional, List, Dict
from functools import lru_cache
from datetime import date

class Settings(BaseSettings):
    API_KEY: str
    ENV: str
    CC_HOST: str
    CC_USER: str
    CC_PWD: str
    CC_SECURE: bool
    CC_COMPRESSION: bool 
    BATCH_SIZE: int

    class Config:
        env_file = ".env"

class URLItem(BaseModel):
    url: str

class QueryModel(BaseModel):
    AppID: Optional[int] = None
    Name: Optional[str] = None
    Release_date: Optional[date] = None
    Required_age: Optional[int] = None
    Price: Optional[float] = None
    DLC_count: Optional[int] = None
    About_the_game: Optional[str] = None
    Supported_languages: Optional[str] = None
    Windows: Optional[int] = None
    Mac: Optional[int] = None
    Linux: Optional[int] = None
    Positive: Optional[int] = None
    Negative: Optional[int] = None
    Score_rank: Optional[int] = None
    Developers: Optional[str] = None
    Publishers: Optional[str] = None
    Categories: Optional[str] = None
    Genres: Optional[str] = None
    Tags: Optional[str] = None

class AggregateQueryModel(BaseModel):
    field: str
    operation: str

# New decorator for cache
@lru_cache()
def get_settings():
    return Settings()