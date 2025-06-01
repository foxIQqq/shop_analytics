from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from api.database import get_db
from api import schemas, models, auth
import uuid

router = APIRouter(prefix="/sellers", tags=["Sellers"])

@router.post("/", response_model=schemas.SellerOut)
async def create_seller(
    seller: schemas.SellerCreate, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_seller = models.Seller(
        name=seller.name,
        email=seller.email,
        phone=seller.phone,
        address=seller.address
    )
    db.add(db_seller)
    db.commit()
    db.refresh(db_seller)
    
    # Отправка в Kafka можно оставить для асинхронной обработки
    from api.producers import send_seller
    await send_seller(seller.dict())
    
    return db_seller

@router.get("/{seller_id}", response_model=schemas.SellerOut)
async def get_seller(
    seller_id: int, 
    db: Session = Depends(get_db)
):
    seller = db.query(models.Seller).filter(models.Seller.id == seller_id).first()
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    return seller

@router.get("/", response_model=List[schemas.SellerOut])
async def list_sellers(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db)
):
    sellers = db.query(models.Seller).offset(skip).limit(limit).all()
    return sellers

@router.put("/{seller_id}", response_model=schemas.SellerOut)
async def update_seller(
    seller_id: int, 
    seller_data: schemas.SellerUpdate, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_seller = db.query(models.Seller).filter(models.Seller.id == seller_id).first()
    if not db_seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    
    # Обновление данных
    for key, value in seller_data.dict(exclude_unset=True).items():
        setattr(db_seller, key, value)
    
    db.commit()
    db.refresh(db_seller)
    return db_seller

@router.delete("/{seller_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_seller(
    seller_id: int, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_seller = db.query(models.Seller).filter(models.Seller.id == seller_id).first()
    if not db_seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    
    db.delete(db_seller)
    db.commit()
    return None