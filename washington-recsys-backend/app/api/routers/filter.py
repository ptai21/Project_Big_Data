from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
from enum import Enum
from app.db import get_local_db
from app.services import FilterService
from app.schemas import FilterOptionsSchema


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


router = APIRouter(prefix="/filters", tags=["Filters"])


@router.get("/options", response_model=FilterOptionsSchema)
async def get_filter_options(
    county: Optional[str] = Query(None, description="County to filter cities"),
    db: AsyncSession = Depends(get_local_db)
):
    service = FilterService(db)
    return await service.get_filter_options(county)


@router.get("/fields", response_model=List[str])
async def get_fields():
    return [field.value for field in FieldEnum]


@router.get("/counties", response_model=List[str])
async def get_counties(
    search: Optional[str] = Query(None, description="Search/autocomplete county name"),
    db: AsyncSession = Depends(get_local_db)
):
    service = FilterService(db)
    counties = await service.get_filter_options()
    counties_list = counties.counties
    
    if search:
        search_lower = search.lower()
        counties_list = [c for c in counties_list if search_lower in c.lower()]
    
    return counties_list


@router.get("/cities", response_model=List[str])
async def get_cities(
    county: Optional[str] = Query(None, description="Filter by county"),
    search: Optional[str] = Query(None, description="Search/autocomplete city name"),
    db: AsyncSession = Depends(get_local_db)
):
    service = FilterService(db)
    
    if county:
        cities_list = await service.get_cities_by_county(county)
    else:
        options = await service.get_filter_options()
        cities_list = options.cities
    
    if search:
        search_lower = search.lower()
        cities_list = [c for c in cities_list if search_lower in c.lower()]
    
    return cities_list


@router.get("/ratings", response_model=List[int])
async def get_ratings():
    return [1, 2, 3, 4, 5]