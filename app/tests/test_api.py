import pytest
from fastapi.testclient import TestClient
from api.main import app
from api.database import Base, engine, get_db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import uuid

SQLALCHEMY_TEST_DATABASE_URL = "sqlite:///./test.db"
engine_test = create_engine(SQLALCHEMY_TEST_DATABASE_URL)
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_test)

Base.metadata.create_all(bind=engine_test)

def override_get_db():
    try:
        db = TestSessionLocal()
        yield db
    finally:
        db.close()

from api.auth import get_current_user

async def override_get_current_user():
    return {"username": "testuser", "is_active": True}

app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[get_current_user] = override_get_current_user

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert "database" in response.json()
    assert "kafka" in response.json()

def test_create_seller():
    response = client.post(
        "/api/v1/sellers/",
        json={"name": "Test Seller", "email": "test@example.com", "phone": "1234567890", "address": "Test Address"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Test Seller"
    assert "id" in data

def test_get_seller():
    create_response = client.post(
        "/api/v1/sellers/",
        json={"name": "Get Test", "email": "get@example.com", "phone": "0987654321", "address": "Get Address"},
    )
    seller_id = create_response.json()["id"]
    
    response = client.get(f"/api/v1/sellers/{seller_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Get Test"

def test_create_product():
    product_data = {
        "name": "Laptop",
        "description": "High performance laptop",
        "category": "Electronics",
        "price": 999.99,
        "stock": 10
    }
    response = client.post("/api/v1/products/", json=product_data)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Laptop"
    assert data["price"] == 999.99
    assert "id" in data

def test_list_products():
    product1 = {
        "name": "Phone",
        "description": "Smartphone",
        "category": "Electronics",
        "price": 499.99,
        "stock": 20
    }
    product2 = {
        "name": "T-shirt",
        "description": "Cotton t-shirt",
        "category": "Clothing",
        "price": 19.99,
        "stock": 100
    }
    
    client.post("/api/v1/products/", json=product1)
    client.post("/api/v1/products/", json=product2)
    
    response = client.get("/api/v1/products/")
    assert response.status_code == 200
    products = response.json()
    assert len(products) >= 2
    
    response = client.get("/api/v1/products/?category=Electronics")
    assert response.status_code == 200
    electronics = response.json()
    assert all(p["category"] == "Electronics" for p in electronics)

def test_create_purchase():
    product_data = {
        "name": "Test Product",
        "description": "Product for purchase test",
        "category": "Test",
        "price": 50.0,
        "stock": 10
    }
    product_response = client.post("/api/v1/products/", json=product_data)
    product_id = product_response.json()["id"]
    
    purchase_data = {
        "product_id": product_id,
        "customer_id": 1,
        "quantity": 2
    }
    
    response = client.post("/api/v1/purchases/", json=purchase_data)
    assert response.status_code == 200
    data = response.json()
    assert data["product_id"] == product_id
    assert data["quantity"] == 2
    assert "total_price" in data
    
    product_after = client.get(f"/api/v1/products/{product_id}")
    assert product_after.json()["stock"] == 8