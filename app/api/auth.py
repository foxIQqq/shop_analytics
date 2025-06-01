from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Optional
import logging

from api.database import get_db
from api import schemas
from api.config import settings

logger = logging.getLogger(__name__)

# Условный импорт slowapi
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address
    USE_RATE_LIMITING = True
    logger.info("Rate limiting is enabled")
except ImportError:
    logger.warning("slowapi не установлен, ограничение запросов отключено")
    USE_RATE_LIMITING = False

# Константы для JWT берем из настроек
SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

# Хеширование паролей
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_PREFIX}/token")

# Функции аутентификации
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        
        # Здесь должен быть запрос к базе данных для получения пользователя
        # Например: user = db.query(models.User).filter(models.User.username == username).first()
        # Заглушка для текущей реализации
        user = {"username": username, "is_active": True}
        
        if user is None:
            raise credentials_exception
        if not user.get("is_active", False):
            raise HTTPException(status_code=400, detail="Inactive user")
    except JWTError:
        raise credentials_exception
    
    return user

# Добавить в роутеры:
# @router.post("/", dependencies=[Depends(get_current_user)])

# Функция для добавления rate limiting, если slowapi доступен
def setup_limiter(app):
    if USE_RATE_LIMITING:
        limiter = Limiter(key_func=get_remote_address)
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        logger.info("Rate limiting настроен")
        return limiter
    else:
        logger.warning("Rate limiting не настроен из-за отсутствия slowapi")
        return None

# @app.get("/api/v1/products/")
# @limiter.limit("5/minute")
# async def get_products(request: Request):
#     # ...