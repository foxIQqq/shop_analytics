from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from sqlalchemy.orm import Session

# Настраиваем логгер
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from api.routers import products, sellers, purchases, customers
    from api.database import engine, Base, get_db
    from api import auth, models
    from api.config import settings
except ImportError as e:
    logger.error(f"Failed to import modules: {str(e)}")
    raise

# Create FastAPI app
app = FastAPI(
    title="Shop Analytics API",
    description="API for shop analytics and data processing",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Настройка rate limiting
limiter = auth.setup_limiter(app)

# Create database tables
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Failed to create database tables: {str(e)}")
    logger.warning("API will continue to run, but database operations may fail")

# Include routers
app.include_router(products.router, prefix=settings.API_V1_PREFIX, tags=["products"])
app.include_router(sellers.router, prefix=settings.API_V1_PREFIX, tags=["sellers"])
app.include_router(purchases.router, prefix=settings.API_V1_PREFIX, tags=["purchases"])
app.include_router(customers.router, prefix=settings.API_V1_PREFIX, tags=["customers"])

@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "Shop Analytics API is running",
        "version": "1.0.0"
    }

@app.get("/health")
def health_check():
    db_status = "connected"
    kafka_status = "connected"
    
    try:
        # Здесь можно добавить проверку соединения с БД
        pass
    except Exception:
        db_status = "disconnected"
        
    try:
        # Здесь можно добавить проверку соединения с Kafka
        pass
    except Exception:
        kafka_status = "disconnected"
    
    return {
        "status": "healthy",
        "database": db_status,
        "kafka": kafka_status
    }

@app.post(f"{settings.API_V1_PREFIX}/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    # Здесь должна быть проверка пользователя в БД
    # Для примера используем заглушку
    if form_data.username != "admin" or form_data.password != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )