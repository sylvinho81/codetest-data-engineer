from pydantic import BaseModel, Field


class ClassificationResult(BaseModel):
    label: str = Field(description="the saga", example="got")


class Entity(BaseModel):
    entity_id: str = Field(description="the identifier of the entity", example="0a5ded3808d21fd689a2f09736a1d07492033788")
    name: str = Field(description="the entity", example="Stannis")


class Saga(BaseModel):
    saga_id: str = Field(description="the identifier of the saga", example="0a5ded3808d21fd689a2f09736a1d07492033788")
    name: str = Field(description="the saga", example="got")
