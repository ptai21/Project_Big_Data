from pydantic import BaseModel
from typing import List, Optional, Any
from datetime import date

# --- Shared Schemas ---
class BusinessSummary(BaseModel):
    business_id: str
    name: str
    address: str | None
    county: str | None
    city: str | None
    avg_rating: float | None
    num_of_reviews: int
    new_category: str | None
    
    class Config:
        from_attributes = True

# --- Endpoint 1 Response ---
class SearchResponse(BaseModel):
    count: int
    data: List[BusinessSummary]

# --- Endpoint 2 Response (Deep Analysis) ---
class StatsMonthlySchema(BaseModel):
    year: int
    month: int
    total_reviews: int
    avg_sentiment: float | None

class ReviewSchema(BaseModel):
    date: date | None
    rating: int
    text: str | None
    sentiment_label: str | None

class BusinessDetailResponse(BaseModel):
    info: BusinessSummary
    stats_total: dict | None # Chứa positive_pct, avg_sentiment, etc.
    monthly_trends: List[StatsMonthlySchema]
    recent_reviews: List[ReviewSchema]