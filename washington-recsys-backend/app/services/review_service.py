from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.repositories import ReviewRepository
from app.schemas import (
    ReviewSchema,
    ReviewListResponse,
    ReviewSummarySchema,
    RatingSummaryItem
)


class ReviewService:
    
    def __init__(self, db: AsyncSession):
        self.repo = ReviewRepository(db)

    async def get_reviews_by_business(
        self,
        business_id: str,
        page: int = 1,
        page_size: int = 20,
        rating: Optional[int] = None
    ) -> ReviewListResponse:
        """Get reviews for a business with optional rating filter"""
        
        reviews, total = await self.repo.get_by_business_id(
            business_id=business_id,
            page=page,
            page_size=page_size,
            rating=rating
        )
        
        return ReviewListResponse(
            total=total,
            page=page,
            page_size=page_size,
            data=[ReviewSchema.model_validate(r) for r in reviews]
        )

    async def get_review_summary(self, business_id: str) -> ReviewSummarySchema:
        """Get review summary (rating distribution) for bar chart"""
        
        distribution = await self.repo.get_rating_distribution(business_id)
        
        total = sum(item["count"] for item in distribution)
        
        return ReviewSummarySchema(
            business_id=business_id,
            total_reviews=total,
            rating_distribution=[
                RatingSummaryItem(rating=item["rating"], count=item["count"])
                for item in distribution
            ]
        )