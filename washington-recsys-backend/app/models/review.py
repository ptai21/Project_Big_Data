from sqlalchemy import Column, String, Text, Integer, Boolean, DECIMAL, ForeignKey, TIMESTAMP, Date, Computed
from sqlalchemy.orm import relationship
from app.db.local_db import Base


class Review(Base):
    __tablename__ = "review"

    review_id = Column(String, primary_key=True)
    business_id = Column(String, ForeignKey("business.business_id"), nullable=False)
    customer_id = Column(String, ForeignKey("customer.customer_id"), nullable=False)
    
    # Time
    time = Column(TIMESTAMP)
    date = Column(Date, Computed("time::date", persisted=True))
    month = Column(Integer, Computed("EXTRACT(MONTH FROM time)", persisted=True))
    year = Column(Integer, Computed("EXTRACT(YEAR FROM time)", persisted=True))
    
    # Content
    rating = Column(Integer)
    text = Column(Text)
    sentiment_score = Column(DECIMAL(5, 4))
    sentiment_label = Column(String(50))
    has_response = Column(Boolean, default=False)
    response_latency_hrs = Column(DECIMAL(10, 2))

    # Relationships
    business = relationship("Business", back_populates="reviews")
    customer = relationship("Customer", back_populates="reviews")
