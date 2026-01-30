from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.repositories import StatsRepository
from app.schemas import (
    StatsTotalSchema,
    StatsYearlyResponse,
    StatsYearlyItem,
    StatsMonthlyResponse,
    StatsMonthlyItem
)
from app.core.exceptions import NotFoundException


class StatsService:
    
    def __init__(self, db: AsyncSession):
        self.repo = StatsRepository(db)

    async def get_total_stats(self, business_id: str) -> StatsTotalSchema:
        """Get total stats for Total Analysis chart"""
        
        stats = await self.repo.get_total_stats(business_id)
        
        if not stats:
            # Return empty stats if not found
            return StatsTotalSchema(business_id=business_id)
        
        return StatsTotalSchema.model_validate(stats)

    async def get_yearly_stats(self, business_id: str) -> StatsYearlyResponse:
        """Get yearly stats for Yearly Analysis line chart"""
        
        stats_list = await self.repo.get_yearly_stats(business_id)
        
        return StatsYearlyResponse(
            business_id=business_id,
            data=[StatsYearlyItem.model_validate(s) for s in stats_list]
        )

    async def get_monthly_stats(
        self, 
        business_id: str,
        year: Optional[int] = None
    ) -> StatsMonthlyResponse:
        """Get monthly stats for Monthly Analysis line chart"""
        
        stats_list = await self.repo.get_monthly_stats(business_id, year)
        
        return StatsMonthlyResponse(
            business_id=business_id,
            year=year,
            data=[StatsMonthlyItem.model_validate(s) for s in stats_list]
        )
