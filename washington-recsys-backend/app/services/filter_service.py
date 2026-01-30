from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
from app.repositories import BusinessRepository
from app.schemas import FilterOptionsSchema, FieldOption


class FilterService:
    
    def __init__(self, db: AsyncSession):
        self.repo = BusinessRepository(db)

    async def get_filter_options(
        self, 
        county: Optional[str] = None
    ) -> FilterOptionsSchema:
        """Get all filter dropdown options"""
        
        # Get field options (static)
        fields = [field.value for field in FieldOption]
        
        # Get counties
        counties = await self.repo.get_distinct_counties()
        
        # Get cities (filtered by county if provided)
        cities = await self.repo.get_distinct_cities(county)
        
        # Rating options (static)
        ratings = [1, 2, 3, 4, 5]
        
        return FilterOptionsSchema(
            fields=fields,
            counties=counties,
            cities=cities,
            ratings=ratings
        )

    async def get_cities_by_county(self, county: str) -> List[str]:
        """Get cities for a specific county (for cascading dropdown)"""
        return await self.repo.get_distinct_cities(county)
