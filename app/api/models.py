from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Customer(Base):
    __tablename__ = "customers"
    
    customer_id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    phone = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    purchases = relationship("Purchase", back_populates="customer")

class Seller(Base):
    __tablename__ = "sellers"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    email = Column(String, unique=True, index=True)
    phone = Column(String)
    address = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    products = relationship("Product", back_populates="seller")

class Product(Base):
    __tablename__ = "products"
    
    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String)
    category = Column(String, index=True)
    price = Column(Float)
    stock = Column(Integer)
    image = Column(String, nullable=True)
    image_url = Column(String, nullable=True)
    seller_id = Column(Integer, ForeignKey("sellers.id"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    seller = relationship("Seller", back_populates="products")
    purchases = relationship("Purchase", back_populates="product")

class Purchase(Base):
    __tablename__ = "purchases"
    
    id = Column(String, primary_key=True, index=True)
    product_id = Column(String, ForeignKey("products.id"))
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), index=True)
    seller_id = Column(Integer, ForeignKey("sellers.id"), index=True)
    quantity = Column(Integer)
    total_price = Column(Float)
    price_at_time = Column(Float, nullable=True)
    purchase_ts = Column(DateTime(timezone=True), server_default=func.now())
    purchased_at = Column(DateTime(timezone=True), nullable=True)
    
    product = relationship("Product", back_populates="purchases") 
    customer = relationship("Customer", back_populates="purchases") 
    seller = relationship("Seller", backref="purchases") 