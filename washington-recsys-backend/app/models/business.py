from sqlalchemy import Column, String, Text, Boolean, Integer, DECIMAL, ForeignKey
from sqlalchemy.orm import relationship
from app.db.local_db import Base


class Business(Base):
    __tablename__ = "business"

    business_id = Column(String, primary_key=True)
    name = Column(Text)
    description = Column(Text)
    
    # Location
    address = Column(Text)
    county = Column(Text)
    city = Column(String(100))
    latitude = Column(DECIMAL(10, 7))
    longitude = Column(DECIMAL(10, 7))
    
    # Metrics
    avg_rating = Column(DECIMAL(3, 2))
    num_of_reviews = Column(Integer, default=0)
    
    # Details
    url = Column(Text)
    is_permanently_closed = Column(Boolean, default=False)
    hours = Column(Text)
    
    # Category
    original_category = Column(Text)
    new_category = Column(Text)

    # Relationships
    category = relationship("Category", back_populates="business", uselist=False)
    reviews = relationship("Review", back_populates="business")
    stats_total = relationship("StatsTotal", back_populates="business", uselist=False)
    stats_yearly = relationship("StatsYearly", back_populates="business")
    stats_monthly = relationship("StatsMonthly", back_populates="business")


class Category(Base):
    __tablename__ = "category"

    business_id = Column(String, ForeignKey("business.business_id", ondelete="CASCADE"), primary_key=True)
    
    food_dining = Column(Boolean, default=False)
    health_medical = Column(Boolean, default=False)
    automotive_transport = Column(Boolean, default=False)
    retail_shopping = Column(Boolean, default=False)
    beauty_wellness = Column(Boolean, default=False)
    home_services_construction = Column(Boolean, default=False)
    education_community = Column(Boolean, default=False)
    entertainment_travel = Column(Boolean, default=False)
    industry_manufacturing = Column(Boolean, default=False)
    financial_legal_services = Column(Boolean, default=False)

    # Relationship
    business = relationship("Business", back_populates="category")
