from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from typing import Optional, List, Tuple
from app.models import Review


class ReviewRepository:
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_business_id(
        self,
        business_id: str,
        page: int = 1,
        page_size: int = 20,
        rating: Optional[int] = None
    ) -> Tuple[List[Review], int]:
        """Get reviews for a business with pagination and optional rating filter"""
        
        # Build conditions
        conditions = [Review.business_id == business_id]
        
        if rating is not None:
            conditions.append(Review.rating == rating)
        
        # Count query
        count_query = (
            select(func.count(Review.review_id))
            .where(and_(*conditions))
        )
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Data query
        offset = (page - 1) * page_size
        query = (
            select(Review)
            .where(and_(*conditions))
            .order_by(Review.time.desc())
            .offset(offset)
            .limit(page_size)
        )
        result = await self.db.execute(query)
        reviews = result.scalars().all()
        
        return list(reviews), total

    async def get_rating_distribution(self, business_id: str) -> List[dict]:
        """Get rating distribution for Review Summary bar chart"""
        query = (
            select(
                Review.rating,
                func.count(Review.review_id).label("count")
            )
            .where(Review.business_id == business_id)
            .group_by(Review.rating)
            .order_by(Review.rating.desc())
        )
        result = await self.db.execute(query)
        rows = result.fetchall()
        
        # Đảm bảo có đủ 5 rating levels
        distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for row in rows:
            if row.rating:
                distribution[row.rating] = row.count
        
        return [
            {"rating": rating, "count": count}
            for rating, count in sorted(distribution.items(), reverse=True)
        ]