from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime

class SellerCreate(BaseModel):
    name: str
    email: EmailStr
    phone: str
    address: str

class SellerUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    address: Optional[str] = None

class SellerOut(SellerCreate):
    id: int
    created_at: datetime

class ProductCreate(BaseModel):
    name: str
    description: str
    category: str
    price: float = Field(gt=0)
    stock: int = Field(ge=0)
    image: Optional[str] = None

class ProductOut(ProductCreate):
    id: str
    created_at: datetime
    image_url: Optional[str] = None

class PurchaseCreate(BaseModel):
    product_id: str
    customer_id: int
    seller_id: int
    quantity: int = Field(gt=0)
    price_at_time: Optional[float] = None
    purchased_at: Optional[datetime] = None

class PurchaseOut(PurchaseCreate):
    id: str
    purchase_ts: datetime
    total_price: float

class CustomerBase(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    phone: Optional[str] = None

class CustomerCreate(CustomerBase):
    pass

class CustomerUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None

class CustomerOut(CustomerBase):
    customer_id: int
    created_at: datetime

    class Config:
        orm_mode = True

# Bulk insertion schemas
class CustomerBulkCreate(BaseModel):
    customers: List[CustomerCreate]

class ProductBulkCreate(BaseModel):
    products: List[ProductCreate]

class PurchaseBulkCreate(BaseModel):
    purchases: List[PurchaseCreate]

class BulkResponse(BaseModel):
    success_count: int
    error_count: int
    errors: Optional[List[str]] = None