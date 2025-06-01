# Shop Analytics API

REST API для работы с данными магазина и аналитической платформы.

## Аутентификация

Все защищенные эндпоинты требуют JWT-токен в заголовке Authorization.

### Получение токена

```
POST /api/v1/token
```

Параметры формы:
- username: логин пользователя
- password: пароль пользователя

Ответ:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

## Продавцы (Sellers)

### Создание продавца

```
POST /api/v1/sellers/
```

Тело запроса:
```json
{
  "name": "Магазин электроники",
  "email": "contact@electronics.ru",
  "phone": "+7 (999) 123-45-67",
  "address": "г. Москва, ул. Ленина, 42"
}
```

### Получение списка продавцов

```
GET /api/v1/sellers/
```

### Получение продавца по ID

```
GET /api/v1/sellers/{seller_id}
```

### Обновление продавца

```
PUT /api/v1/sellers/{seller_id}
```

### Удаление продавца

```
DELETE /api/v1/sellers/{seller_id}
```

## Товары (Products)

### Создание товара

```
POST /api/v1/products/
```

Тело запроса:
```json
{
  "name": "Смартфон Ultra X",
  "description": "Современный смартфон с 6.5' экраном",
  "category": "Электроника",
  "price": 49999.0,
  "stock": 10
}
```

### Получение списка товаров

```
GET /api/v1/products/
```

Параметры запроса:
- category: фильтрация по категории
- skip: пропустить N записей
- limit: ограничить количество записей

### Получение товара по ID

```
GET /api/v1/products/{product_id}
```

### Обновление товара

```
PUT /api/v1/products/{product_id}
```

### Удаление товара

```
DELETE /api/v1/products/{product_id}
```

## Покупки (Purchases)

### Создание покупки

```
POST /api/v1/purchases/
```

Тело запроса:
```json
{
  "product_id": "abc123",
  "customer_id": 42,
  "quantity": 2
}
```

### Получение списка покупок

```
GET /api/v1/purchases/
```

Параметры запроса:
- customer_id: фильтрация по ID покупателя
- product_id: фильтрация по ID товара
- skip: пропустить N записей
- limit: ограничить количество записей

### Получение покупки по ID

```
GET /api/v1/purchases/{purchase_id}
```

### Удаление покупки

```
DELETE /api/v1/purchases/{purchase_id}
```

## Клиенты (Customers)

### Создание клиента

```
POST /api/v1/customers/
```

Тело запроса:
```json
{
  "first_name": "Иван",
  "last_name": "Петров",
  "email": "ivan.petrov@example.com",
  "phone": "+7 (999) 123-45-67"
}
```

### Получение списка клиентов

```
GET /api/v1/customers/
```

Параметры запроса:
- skip: пропустить N записей
- limit: ограничить количество записей

### Получение клиента по ID

```
GET /api/v1/customers/{customer_id}
```

### Обновление клиента

```
PUT /api/v1/customers/{customer_id}
```

### Удаление клиента

```
DELETE /api/v1/customers/{customer_id}
```

## Мониторинг

### Проверка состояния API

```
GET /health
```

Ответ:
```json
{
  "status": "healthy",
  "database": "connected",
  "kafka": "connected"
}
``` 