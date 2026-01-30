from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
from app.models import StatsTotal, StatsYearly, StatsMonthly


class StatsRepository:
    
    def __init__(self, db: AsyncSession):
        self.db = db

    # ============ STATS TOTAL ============
    async def get_total_stats(self, business_id: str) -> Optional[StatsTotal]:
        """Get total stats for a business"""
        query = select(StatsTotal).where(StatsTotal.business_id == business_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    # ============ STATS YEARLY ============
    async def get_yearly_stats(self, business_id: str) -> List[StatsYearly]:
        """Get yearly stats for a business (for line chart)"""
        query = (
            select(StatsYearly)
            .where(StatsYearly.business_id == business_id)
            .order_by(StatsYearly.year.asc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    # ============ STATS MONTHLY ============
    async def get_monthly_stats(
        self, 
        business_id: str, 
        year: Optional[int] = None
    ) -> List[StatsMonthly]:
        """Get monthly stats for a business (for line chart)"""
        query = (
            select(StatsMonthly)
            .where(StatsMonthly.business_id == business_id)
        )
        
        if year:
            query = query.where(StatsMonthly.year == year)
        
        query = query.order_by(StatsMonthly.year.asc(), StatsMonthly.month.asc())
        
        result = await self.db.execute(query)
        return list(result.scalars().all())
