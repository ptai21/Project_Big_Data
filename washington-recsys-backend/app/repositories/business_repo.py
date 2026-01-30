from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import joinedload
from typing import Optional, List, Tuple
from app.models import Business, Category


class BusinessRepository:
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_id(self, business_id: str) -> Optional[Business]:
        """Get business by ID with category"""
        query = (
            select(Business)
            .options(joinedload(Business.category))
            .where(Business.business_id == business_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_list(
        self,
        field: Optional[str] = None,
        county: Optional[str] = None,
        city: Optional[str] = None,
        min_rating: Optional[float] = None,
        max_rating: Optional[float] = None,
        search: Optional[str] = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Business], int]:
        """Get filtered list of businesses with pagination"""
        
        # Base query
        query = select(Business).options(joinedload(Business.category))
        count_query = select(func.count(Business.business_id))
        
        # Build conditions
        conditions = []
        
        # Filter by field (category group)
        if field:
            field_condition = self._get_field_condition(field)
            if field_condition is not None:
                query = query.join(Category)
                count_query = count_query.join(Category)
                conditions.append(field_condition)
        
        # Filter by county
        if county:
            conditions.append(Business.county == county)
        
        # Filter by city
        if city:
            conditions.append(Business.city == city)
        
        # Filter by rating
        if min_rating is not None:
            conditions.append(Business.avg_rating >= min_rating)
        if max_rating is not None:
            conditions.append(Business.avg_rating <= max_rating)
        
        # Search by name
        if search:
            conditions.append(
                Business.name.ilike(f"%{search}%")
            )
        
        # Apply conditions
        if conditions:
            query = query.where(and_(*conditions))
            count_query = count_query.where(and_(*conditions))
        
        # Get total count
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        offset = (page - 1) * page_size
        query = query.offset(offset).limit(page_size)
        
        # Order by rating desc
        query = query.order_by(Business.avg_rating.desc().nullslast())
        
        # Execute
        result = await self.db.execute(query)
        businesses = result.scalars().unique().all()
        
        return list(businesses), total

    def _get_field_condition(self, field: str):
        """Map field name to category column condition"""
        field_mapping = {
            "food_dining": Category.food_dining == True,
            "health_medical": Category.health_medical == True,
            "automotive_transport": Category.automotive_transport == True,
            "retail_shopping": Category.retail_shopping == True,
            "beauty_wellness": Category.beauty_wellness == True,
            "home_services_construction": Category.home_services_construction == True,
            "education_community": Category.education_community == True,
            "entertainment_travel": Category.entertainment_travel == True,
            "industry_manufacturing": Category.industry_manufacturing == True,
            "financial_legal_services": Category.financial_legal_services == True,
        }
        return field_mapping.get(field)

    async def get_distinct_counties(self) -> List[str]:
        """Get all distinct counties"""
        query = (
            select(Business.county)
            .where(Business.county.isnot(None))
            .distinct()
            .order_by(Business.county)
        )
        result = await self.db.execute(query)
        return [row[0] for row in result.fetchall()]

    async def get_distinct_cities(self, county: Optional[str] = None) -> List[str]:
        """Get all distinct cities, optionally filtered by county"""
        query = (
            select(Business.city)
            .where(Business.city.isnot(None))
            .distinct()
            .order_by(Business.city)
        )
        if county:
            query = query.where(Business.county == county)
        result = await self.db.execute(query)
        return [row[0] for row in result.fetchall()]
