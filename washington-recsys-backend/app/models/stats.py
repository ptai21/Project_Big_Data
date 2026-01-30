from sqlalchemy import Column, String, Integer, DECIMAL, Date, ForeignKey
from sqlalchemy.orm import relationship
from app.db.local_db import Base


class StatsMonthly(Base):
    __tablename__ = "stats_monthly"

    business_id = Column(String, ForeignKey("business.business_id", ondelete="CASCADE"), primary_key=True)
    year = Column(Integer, primary_key=True)
    month = Column(Integer, primary_key=True)
    
    total_reviews = Column(Integer, default=0)
    positive_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)
    negative_count = Column(Integer, default=0)
    
    positive_pct = Column(DECIMAL(5, 2))
    neutral_pct = Column(DECIMAL(5, 2))
    negative_pct = Column(DECIMAL(5, 2))
    
    avg_sentiment = Column(DECIMAL(5, 4))

    # Relationship
    business = relationship("Business", back_populates="stats_monthly")


class StatsYearly(Base):
    __tablename__ = "stats_yearly"

    business_id = Column(String, ForeignKey("business.business_id", ondelete="CASCADE"), primary_key=True)
    year = Column(Integer, primary_key=True)
    
    total_reviews = Column(Integer, default=0)
    positive_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)
    negative_count = Column(Integer, default=0)
    
    positive_pct = Column(DECIMAL(5, 2))
    neutral_pct = Column(DECIMAL(5, 2))
    negative_pct = Column(DECIMAL(5, 2))
    
    avg_sentiment = Column(DECIMAL(5, 4))

    # Relationship
    business = relationship("Business", back_populates="stats_yearly")


class StatsTotal(Base):
    __tablename__ = "stats_total"

    business_id = Column(String, ForeignKey("business.business_id", ondelete="CASCADE"), primary_key=True)
    
    total_reviews = Column(Integer, default=0)
    positive_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)
    negative_count = Column(Integer, default=0)
    
    positive_pct = Column(DECIMAL(5, 2))
    neutral_pct = Column(DECIMAL(5, 2))
    negative_pct = Column(DECIMAL(5, 2))
    
    avg_sentiment = Column(DECIMAL(5, 4))
    
    first_review_date = Column(Date)
    last_review_date = Column(Date)

    # Relationship
    business = relationship("Business", back_populates="stats_total")
