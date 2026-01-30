from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.repositories import BusinessRepository
from app.schemas import (
    BusinessCardSchema,
    BusinessListResponse,
    BusinessDetailSchema
)
from app.core.exceptions import NotFoundException


class BusinessService:
    
    def __init__(self, db: AsyncSession):
        self.repo = BusinessRepository(db)

    async def get_business_list(
        self,
        field: Optional[str] = None,
        county: Optional[str] = None,
        city: Optional[str] = None,
        min_rating: Optional[float] = None,
        max_rating: Optional[float] = None,
        search: Optional[str] = None,
        page: int = 1,
        page_size: int = 20
    ) -> BusinessListResponse:
        """Get filtered list of businesses"""
        
        businesses, total = await self.repo.get_list(
            field=field,
            county=county,
            city=city,
            min_rating=min_rating,
            max_rating=max_rating,
            search=search,
            page=page,
            page_size=page_size
        )
        
        # Convert to schema
        cards = [
            BusinessCardSchema(
                business_id=b.business_id,
                name=b.name,
                address=b.address,
                county=b.county,
                city=b.city,
                avg_rating=b.avg_rating,
                num_of_reviews=b.num_of_reviews or 0,
                original_category=b.original_category
            )
            for b in businesses
        ]
        
        return BusinessListResponse(
            total=total,
            page=page,
            page_size=page_size,
            data=cards
        )

    async def get_business_detail(self, business_id: str) -> BusinessDetailSchema:
        """Get business detail by ID"""
        
        business = await self.repo.get_by_id(business_id)
        
        if not business:
            raise NotFoundException(f"Business {business_id} not found")
        
        return BusinessDetailSchema.model_validate(business)
