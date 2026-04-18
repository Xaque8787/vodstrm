from pydantic import BaseModel, EmailStr, field_validator, model_validator
from typing import Literal, Optional
from datetime import datetime


class UserCreate(BaseModel):
    username: str
    password: str
    email: Optional[str] = None


class UserLogin(BaseModel):
    username: str
    password: str


class UserRecord(BaseModel):
    id: int
    username: str
    email: Optional[str]
    is_admin: bool
    created_at: datetime

    class Config:
        from_attributes = True


class TokenData(BaseModel):
    username: str
    user_id: int
    is_admin: bool


class ProviderM3UCreate(BaseModel):
    name: str
    url: str

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Name is required")
        return v

    @field_validator("url")
    @classmethod
    def url_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("URL is required")
        return v


class ProviderXtreamCreate(BaseModel):
    name: str
    username: str
    password: str
    port: Optional[str] = None

    @field_validator("name", "username", "password")
    @classmethod
    def fields_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("This field is required")
        return v

    @field_validator("port")
    @classmethod
    def port_optional_strip(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            return v if v else None
        return None


class ProviderRecord(BaseModel):
    id: int
    name: str
    type: Literal["m3u", "xtream"]
    url: Optional[str]
    username: Optional[str]
    password: Optional[str]
    port: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True
