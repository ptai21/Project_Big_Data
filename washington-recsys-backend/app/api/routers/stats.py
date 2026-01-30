from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.db import get_local_db
from app.services import StatsService
from app.schemas import StatsTotalSchema, StatsYearlyResponse, StatsMonthlyResponse

router = APIRouter(prefix="/businesses/{business_id}/stats", tags=["Statistics"])


@router.get("/total", response_model=StatsTotalSchema)
async def get_total_stats(
    business_id: str,
    db: AsyncSession = Depends(get_local_db)
):
    """
    Get total statistics for a business.
    
    Used for: Total Analysis / Further Analysis charts.
    """
    service = StatsService(db)
    return await service.get_total_stats(business_id)


@router.get("/yearly", response_model=StatsYearlyResponse)
async def get_yearly_stats(
    business_id: str,
    db: AsyncSession = Depends(get_local_db)
):
    """
    Get yearly statistics for a business.
    
    Used for: Yearly Analysis line chart.
    """
    service = StatsService(db)
    return await service.get_yearly_stats(business_id)


@router.get("/monthly", response_model=StatsMonthlyResponse)
async def get_monthly_stats(
    business_id: str,
    year: Optional[int] = Query(None, description="Filter by year"),
    db: AsyncSession = Depends(get_local_db)
):
    """
    Get monthly statistics for a business.
    
    Used for: Monthly Analysis line chart.
    
    - **year**: Optional filter to get data for specific year only.
    """
    service = StatsService(db)
    return await service.get_monthly_stats(business_id, year)
