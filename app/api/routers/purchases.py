from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from typing import List, Optional
from uuid import uuid4
from datetime import datetime
from api.database import get_db
from api import schemas, models, auth, producers
from api.producers import send_purchase, bulk_send_to_kafka
from api.schemas import BulkResponse, PurchaseBulkCreate
from api.config import settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/purchases")

@router.post("/", response_model=schemas.PurchaseOut)
async def create_purchase(
    purchase: schemas.PurchaseCreate, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    # Проверка существования товара
    product = db.query(models.Product).filter(models.Product.id == purchase.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Проверка наличия товара на складе
    if product.stock < purchase.quantity:
        raise HTTPException(status_code=400, detail="Not enough stock available")
    
    # Расчет общей суммы покупки
    price_at_time = purchase.price_at_time or product.price
    total_price = price_at_time * purchase.quantity
    purchased_at = purchase.purchased_at or datetime.utcnow()
    
    # Создание записи о покупке
    db_purchase = models.Purchase(
        id=str(uuid4()),
        product_id=purchase.product_id,
        seller_id=purchase.seller_id,
        customer_id=purchase.customer_id,
        quantity=purchase.quantity,
        price_at_time=price_at_time,
        total_price=total_price,
        purchase_ts=datetime.utcnow(),
        purchased_at=purchased_at
    )
    
    # Обновление остатка товара
    product.stock -= purchase.quantity
    
    db.add(db_purchase)
    db.commit()
    db.refresh(db_purchase)
    
    # Отправка в Kafka для дальнейшей обработки
    try:
        purchase_data = purchase.dict()
        purchase_data["id"] = db_purchase.id
        purchase_data["total_price"] = total_price
        purchase_data["price_at_time"] = price_at_time
        purchase_data["purchase_ts"] = db_purchase.purchase_ts.isoformat()
        purchase_data["purchased_at"] = purchased_at.isoformat()
        await producers.send_purchase(purchase_data)
        logger.info(f"Purchase {db_purchase.id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send purchase to Kafka: {str(e)}")
    
    return db_purchase

@router.get("/{purchase_id}", response_model=schemas.PurchaseOut)
async def get_purchase(
    purchase_id: str, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_purchase = db.query(models.Purchase).filter(models.Purchase.id == purchase_id).first()
    if db_purchase is None:
        raise HTTPException(status_code=404, detail="Purchase not found")
    return db_purchase

@router.get("/", response_model=List[schemas.PurchaseOut])
async def list_purchases(
    customer_id: Optional[int] = None,
    product_id: Optional[str] = None,
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    query = db.query(models.Purchase)
    
    if customer_id:
        query = query.filter(models.Purchase.customer_id == customer_id)
    
    if product_id:
        query = query.filter(models.Purchase.product_id == product_id)
    
    purchases = query.offset(skip).limit(limit).all()
    return purchases

@router.delete("/{purchase_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_purchase(
    purchase_id: str, 
    db: Session = Depends(get_db),
    current_user: dict = Depends(auth.get_current_user)
):
    db_purchase = db.query(models.Purchase).filter(models.Purchase.id == purchase_id).first()
    if db_purchase is None:
        raise HTTPException(status_code=404, detail="Purchase not found")
    
    # Возвращаем товар на склад
    product = db.query(models.Product).filter(models.Product.id == db_purchase.product_id).first()
    if product:
        product.stock += db_purchase.quantity
    
    db.delete(db_purchase)
    db.commit()
    
    # Отправка уведомления в Kafka о удалении
    try:
        await producers.send_purchase({"id": purchase_id, "deleted": True})
        logger.info(f"Purchase deletion {purchase_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send purchase deletion to Kafka: {str(e)}")
    
    return None

@router.post("/bulk-kafka", response_model=BulkResponse)
async def bulk_send_purchases_to_kafka(
    purchases_data: PurchaseBulkCreate,
    current_user: dict = Depends(auth.get_current_user)
):
    """
    Массовая отправка покупок напрямую в Kafka, минуя PostgreSQL.
    Принимает массив объектов для создания покупок и отправляет их в топик purchases.
    Это аналог загрузки данных из kafka_purchases.json в скрипте запуска.
    """
    # Подготовка данных для отправки в Kafka
    purchases_for_kafka = [
        {
            "customer_id": purchase.customer_id,
            "product_id": purchase.product_id,
            "seller_id": purchase.seller_id,
            "quantity": purchase.quantity,
            "price_at_time": purchase.price_at_time or 0.0,
            "purchased_at": purchase.purchased_at.isoformat() if purchase.purchased_at else datetime.utcnow().isoformat()
        }
        for purchase in purchases_data.purchases
    ]
    
    # Отправка данных в Kafka
    success_count, errors = await bulk_send_to_kafka(
        settings.KAFKA_PURCHASES_TOPIC, 
        purchases_for_kafka
    )
    
    # Формируем ответ
    return {
        "success_count": success_count,
        "error_count": len(errors),
        "errors": errors if errors else None
    }