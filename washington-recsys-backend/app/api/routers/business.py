from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from enum import Enum
from app.db import get_local_db
from app.services import BusinessService
from app.schemas import BusinessListResponse, BusinessDetailSchema


# ============ ENUM cho Swagger Dropdown ============
class FieldEnum(str, Enum):
    food_dining = "food_dining"
    health_medical = "health_medical"
    automotive_transport = "automotive_transport"
    retail_shopping = "retail_shopping"
    beauty_wellness = "beauty_wellness"
    home_services_construction = "home_services_construction"
    education_community = "education_community"
    entertainment_travel = "entertainment_travel"
    industry_manufacturing = "industry_manufacturing"
    financial_legal_services = "financial_legal_services"


router = APIRouter(prefix="/businesses", tags=["Businesses"])


@router.get("", response_model=BusinessListResponse)
async def get_businesses(
    field: Optional[FieldEnum] = Query(None, description="Category field filter"),
    county: Optional[str] = Query(None, description="County filter"),
    city: Optional[str] = Query(None, description="City filter"),
    min_rating: Optional[int] = Query(None, ge=1, le=5, description="Min rating (1-5)"),
    max_rating: Optional[int] = Query(None, ge=1, le=5, description="Max rating (1-5)"),
    search: Optional[str] = Query(None, description="Search by name"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    db: AsyncSession = Depends(get_local_db)
):
    """
    Get list of businesses with filters.
    
    - **field**: Filter by category group (dropdown)
    - **county**: Filter by county
    - **city**: Filter by city
    - **min_rating/max_rating**: Filter by rating range (1-5)
    - **search**: Search by business name
    """
    service = BusinessService(db)
    return await service.get_business_list(
        field=field.value if field else None,
        county=county,
        city=city,
        min_rating=min_rating,
        max_rating=max_rating,
        search=search,
        page=page,
        page_size=page_size
    )


@router.get("/{business_id}", response_model=BusinessDetailSchema)
async def get_business_detail(
    business_id: str,
    db: AsyncSession = Depends(get_local_db)
):
    """Get business detail by ID"""
    service = BusinessService(db)
    return await service.get_business_detail(business_id)