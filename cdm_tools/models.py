from pydantic import BaseModel


class CommonDataModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def get_fields(cls):
        return cls.__annotations__.items()

    @classmethod
    def get_request_form(cls):
        return {
            field: {"name": field_info.alias or field, "dtype": field_info.annotation}
            for field, field_info in cls.model_fields.items()
        }
