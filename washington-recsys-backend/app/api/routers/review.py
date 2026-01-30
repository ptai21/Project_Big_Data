from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.db import get_local_db
from app.services import ReviewService
from app.schemas import ReviewListResponse, ReviewSummarySchema

router = APIRouter(prefix="/businesses/{business_id}/reviews", tags=["Reviews"])


@router.get("", response_model=ReviewListResponse)
async def get_reviews(
    business_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    rating: Optional[int] = Query(None, ge=1, le=5),
    db: AsyncSession = Depends(get_local_db)
):
    """Get reviews for a specific business with optional rating filter"""
    service = ReviewService(db)
    return await service.get_reviews_by_business(
        business_id=business_id,
        page=page,
        page_size=page_size,
        rating=rating
    )


@router.get("/summary", response_model=ReviewSummarySchema)
async def get_review_summary(
    business_id: str,
    db: AsyncSession = Depends(get_local_db)
):
    """
    Get review summary for bar chart.
    
    Returns rating distribution (count per rating 1-5).
    """
    service = ReviewService(db)
    return await service.get_review_summary(business_id)