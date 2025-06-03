import json
import random
import datetime
from datetime import timedelta

# Список возможных товаров (product_id)
products = [f"prod-{i:03}" for i in range(1, 51)]

# Диапазон customer_id (из ранее созданного файла customers.json)
min_customer_id = 1
max_customer_id = 200

# Диапазон seller_id
min_seller_id = 1
max_seller_id = 50

# Диапазон цен
min_price = 5.0
max_price = 500.0

# Диапазон количества товаров
min_quantity = 1
max_quantity = 10

# Генерация случайной даты в пределах последних 5 лет
def random_date():
    start_date = datetime.datetime.now() - timedelta(days=365*5)
    end_date = datetime.datetime.now()
    time_delta = end_date - start_date
    random_seconds = random.randint(0, int(time_delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)

# Генерация случайной покупки
def generate_purchase():
    product_id = random.choice(products)
    customer_id = random.randint(min_customer_id, max_customer_id)
    seller_id = random.randint(min_seller_id, max_seller_id)
    quantity = random.randint(min_quantity, max_quantity)
    price_at_time = round(random.uniform(min_price, max_price), 2)
    purchased_at = random_date().strftime("%Y-%m-%dT%H:%M:%S")
    
    return {
        "product_id": product_id,
        "customer_id": customer_id,
        "seller_id": seller_id,
        "quantity": quantity,
        "price_at_time": price_at_time,
        "purchased_at": purchased_at
    }

# Генерация указанного количества покупок
def generate_purchases(count):
    return [generate_purchase() for _ in range(count)]

# Количество покупок для генерации
num_purchases = 500

# Генерация покупок
purchases = generate_purchases(num_purchases)

# Создание итогового JSON объекта
purchases_json = {
    "purchases": purchases
}

# Запись JSON в файл
output_path = 'app/input/purchases.json'
with open(output_path, 'w') as f:
    json.dump(purchases_json, f, indent=2)

print(f"JSON файл с покупками успешно создан: {output_path}")
print(f"Всего покупок: {len(purchases)}") 