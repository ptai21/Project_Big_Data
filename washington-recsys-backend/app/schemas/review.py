from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal
from datetime import datetime, date


class ReviewSchema(BaseModel):
    review_id: str
    business_id: str
    customer_id: str
    time: Optional[datetime] = None
    rating: Optional[int] = None
    text: Optional[str] = None
    sentiment_score: Optional[Decimal] = None
    sentiment_label: Optional[str] = None
    has_response: bool = False
    response_latency_hrs: Optional[Decimal] = None

    class Config:
        from_attributes = True


# ============ REVIEW SUMMARY (Bar Chart) ============
class RatingSummaryItem(BaseModel):
    """Số lượng review theo rating"""
    rating: int  # 1-5
    count: int


class ReviewSummarySchema(BaseModel):
    """Response cho Review Summary bar chart"""
    business_id: str
    total_reviews: int
    rating_distribution: List[RatingSummaryItem]  # 5 items cho rating 1-5


# ============ REVIEW LIST ============
class ReviewListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: List[ReviewSchema]
