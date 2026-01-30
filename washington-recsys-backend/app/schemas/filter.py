from pydantic import BaseModel
from typing import List, Optional
from enum import Enum


# ============ FIELD OPTIONS (Category Groups) ============
class FieldOption(str, Enum):
    FOOD_DINING = "food_dining"
    HEALTH_MEDICAL = "health_medical"
    AUTOMOTIVE_TRANSPORT = "automotive_transport"
    RETAIL_SHOPPING = "retail_shopping"
    BEAUTY_WELLNESS = "beauty_wellness"
    HOME_SERVICES_CONSTRUCTION = "home_services_construction"
    EDUCATION_COMMUNITY = "education_community"
    ENTERTAINMENT_TRAVEL = "entertainment_travel"
    INDUSTRY_MANUFACTURING = "industry_manufacturing"
    FINANCIAL_LEGAL_SERVICES = "financial_legal_services"


# ============ FILTER OPTIONS RESPONSE ============
class FilterOptionsSchema(BaseModel):
    """Response cho dropdown options"""
    fields: List[str]      # ["food_dining", "health_medical", ...]
    counties: List[str]    # ["King", "Pierce", ...]
    cities: List[str]      # ["Seattle", "Tacoma", ...]
    ratings: List[int]     # [1, 2, 3, 4, 5]


# ============ FILTER REQUEST ============
class BusinessFilterParams(BaseModel):
    """Query params cho filter businesses"""
    field: Optional[str] = None          # Category group
    category: Optional[str] = None       # Original category search
    county: Optional[str] = None
    city: Optional[str] = None
    min_rating: Optional[float] = None
    max_rating: Optional[float] = None
    search: Optional[str] = None         # Search by name
    page: int = 1
    page_size: int = 20
