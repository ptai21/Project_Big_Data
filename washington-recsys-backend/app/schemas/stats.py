from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal
from datetime import date


# ============ STATS TOTAL (Total Analysis) ============
class StatsTotalSchema(BaseModel):
    business_id: str
    total_reviews: int = 0
    positive_count: int = 0
    neutral_count: int = 0
    negative_count: int = 0
    positive_pct: Optional[Decimal] = None
    neutral_pct: Optional[Decimal] = None
    negative_pct: Optional[Decimal] = None
    avg_sentiment: Optional[Decimal] = None
    first_review_date: Optional[date] = None
    last_review_date: Optional[date] = None

    class Config:
        from_attributes = True


# ============ STATS YEARLY (Yearly Analysis Line Chart) ============
class StatsYearlyItem(BaseModel):
    year: int
    total_reviews: int = 0
    positive_count: int = 0
    neutral_count: int = 0
    negative_count: int = 0
    avg_sentiment: Optional[Decimal] = None

    class Config:
        from_attributes = True


class StatsYearlyResponse(BaseModel):
    business_id: str
    data: List[StatsYearlyItem]


# ============ STATS MONTHLY (Monthly Analysis Line Chart) ============
class StatsMonthlyItem(BaseModel):
    year: int
    month: int
    total_reviews: int = 0
    positive_count: int = 0
    neutral_count: int = 0
    negative_count: int = 0
    avg_sentiment: Optional[Decimal] = None

    class Config:
        from_attributes = True


class StatsMonthlyResponse(BaseModel):
    business_id: str
    year: Optional[int] = None  # Filter by year nếu cần
    data: List[StatsMonthlyItem]
