import datetime
from typing import Optional, ClassVar

from bson import ObjectId
from pydantic import BaseModel, Field


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


def get_timezoneaware_now():
    return datetime.datetime.now(datetime.timezone.utc)


class User(BaseModel):
    collection_name: ClassVar[str] = 'users'
    id: PyObjectId = Field(default_factory=PyObjectId, alias='_id')
    username: str
    user_id: str
    max_research: Optional[int]
    ban_period: Optional[datetime.datetime]
    timestamp: datetime.datetime = Field(default_factory=get_timezoneaware_now)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class BanUser(BaseModel):
    user: str
    period: str


class LimitResearches(BaseModel):
    user: str
    limit: int


class UserAuthTransfer(BaseModel):
    username: str
    user_id: str


class UserAuthTransferReply(UserAuthTransfer):
    authorized: bool
