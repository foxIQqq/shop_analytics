from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from typing import List, Optional
from uuid import uuid4
from api.database import get_db
from api import schemas, models, auth, producers
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["Products"])

@router.post("/", response_model=schemas.ProductOut)
async def create_product(
    product: schemas.ProductCreate, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    # Валидация данных
    if product.price <= 0:
        raise HTTPException(status_code=400, detail="Price must be positive")
    
    # Создаем запись в БД
    db_product = models.Product(
        id=str(uuid4()),
        name=product.name,
        description=product.description,
        category=product.category,
        price=product.price,
        stock=product.stock,
        image=product.image
    )
    
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    
    # Отправка в Kafka для дальнейшей обработки
    try:
        await producers.send_product(product.dict())
        logger.info(f"Product {db_product.id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send product to Kafka: {str(e)}")
    
    return db_product

@router.get("/{product_id}", response_model=schemas.ProductOut)
async def get_product(product_id: str, db: Session = Depends(get_db)):
    db_product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product

@router.get("/", response_model=List[schemas.ProductOut])
async def list_products(
    category: Optional[str] = None, 
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db)
):
    query = db.query(models.Product)
    
    if category:
        query = query.filter(models.Product.category == category)
    
    products = query.offset(skip).limit(limit).all()
    return products

@router.put("/{product_id}", response_model=schemas.ProductOut)
async def update_product(
    product_id: str, 
    product_data: schemas.ProductCreate, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Обновление данных
    for key, value in product_data.dict().items():
        setattr(db_product, key, value)
    
    db.commit()
    db.refresh(db_product)
    
    # Отправка в Kafka для обновления в других системах
    try:
        product_dict = product_data.dict()
        product_dict["id"] = product_id
        await producers.send_product(product_dict)
        logger.info(f"Updated product {product_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send updated product to Kafka: {str(e)}")
    
    return db_product

@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    product_id: str, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    
    db.delete(db_product)
    db.commit()
    
    # Отправка уведомления в Kafka о удалении
    try:
        await producers.send_product({"id": product_id, "deleted": True})
        logger.info(f"Product deletion {product_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send product deletion to Kafka: {str(e)}")
    
    return None