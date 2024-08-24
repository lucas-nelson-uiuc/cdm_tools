from pydantic import BaseModel

from pyspark.sql import types as T


class CommonDataModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def get_fields(cls):
        return cls.__annotations__.items()
    
    @classmethod
    def get_schema(cls):
        return T.StructType([
            T.StructField(field_name, T.StringType())
            for field_name in cls.model_fields.keys()
        ])

    @classmethod
    def get_request_form(cls):
        return {
            field: {"name": field_info.alias or field, "dtype": field_info.annotation}
            for field, field_info in cls.model_fields.items()
        }
