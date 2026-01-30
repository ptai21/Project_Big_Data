from sqlalchemy import Column, String, Text
from sqlalchemy.orm import relationship
from app.db.local_db import Base


class Customer(Base):
    __tablename__ = "customer"

    customer_id = Column(String, primary_key=True)
    name = Column(Text)

    # Relationships
    reviews = relationship("Review", back_populates="customer")
