import jmespath
from pydantic import BaseModel, Field

class LocationDefault(BaseModel):
    city_name: str = Field(default=None, alias="city")
    country_name: str = Field(default=None, alias="country")
    district_name: str = Field(default=None, alias="district")
    location_name: str = Field(default=None, alias="location_name")
    location_type: str = Field(default=None, alias="location_type")
    province_name: str = Field(default=None, alias="province")
    subdistrict_name: str = Field(default=None, alias="subdistrict")