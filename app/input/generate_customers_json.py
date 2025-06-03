import re
import json
import os

def sql_to_json(sql_content):
    # Регулярное выражение для извлечения данных из SQL INSERT
    pattern = r"INSERT INTO customers \(customer_id, first_name, last_name, email, created_at\) VALUES \((\d+), '([^']+)', '([^']+)', '([^']+)', '([^']+)'\);"
    
    customers = []
    
    # Находим все совпадения
    matches = re.findall(pattern, sql_content)
    
    for match in matches:
        customer_id, first_name, last_name, email, created_at = match
        
        customers.append({
            "customer_id": int(customer_id),
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "created_at": created_at
        })
    
    # Создаем итоговый JSON объект
    json_data = {
        "customers": customers
    }
    
    return json_data

# Путь к файлу с SQL-запросами
sql_file_path = 'app/input/customers_sql.txt'

# Читаем SQL-запросы из файла
with open(sql_file_path, 'r') as f:
    sql_content = f.read()

# Конвертируем SQL в JSON
json_data = sql_to_json(sql_content)

# Записываем JSON в файл
output_path = 'app/input/customers.json'
with open(output_path, 'w') as f:
    json.dump(json_data, f, indent=2)

print(f"JSON файл успешно создан: {output_path}")
print(f"Всего клиентов: {len(json_data['customers'])}") 