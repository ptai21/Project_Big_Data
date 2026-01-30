from pydantic import BaseModel
from typing import Optional, List


class CategorySchema(BaseModel):
    food_dining: bool = False
    health_medical: bool = False
    automotive_transport: bool = False
    retail_shopping: bool = False
    beauty_wellness: bool = False
    home_services_construction: bool = False
    education_community: bool = False
    entertainment_travel: bool = False
    industry_manufacturing: bool = False
    financial_legal_services: bool = False

    class Config:
        from_attributes = True


class BusinessCardSchema(BaseModel):
    business_id: str
    name: Optional[str] = None
    address: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    avg_rating: Optional[float] = None
    num_of_reviews: int = 0
    original_category: Optional[str] = None

    class Config:
        from_attributes = True


class BusinessListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: List[BusinessCardSchema]


class BusinessDetailSchema(BaseModel):
    business_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    address: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    avg_rating: Optional[float] = None
    num_of_reviews: int = 0
    url: Optional[str] = None
    is_permanently_closed: bool = False
    hours: Optional[str] = None
    original_category: Optional[str] = None
    new_category: Optional[str] = None

    class Config:
        from_attributes = True