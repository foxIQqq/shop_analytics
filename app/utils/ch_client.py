# ----------------------------------------------------------------------
# utils/ch_client.py
#
# Вспомогательный модуль для работы с ClickHouse через clickhouse-driver.
# Содержит функцию получения клиента и настройки соединения.
# ----------------------------------------------------------------------

import time
import logging
from typing import Dict, Any, Optional
from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, ServerException

logger = logging.getLogger(__name__)

# Настройки повторных попыток
MAX_RETRIES = 3
RETRY_DELAY = 2  # секунды


def get_clickhouse_client(host: str = "localhost",
                          port: int = 9000,
                          user: str = "default",
                          password: str = "",
                          database: str = "default") -> Client:
    """
    Возвращает экземпляр clickhouse_driver.Client, уже
    подключённый к указанному хосту/порту.
    """
    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        send_receive_timeout=300,     # Чтобы не падало по таймауту для больших вставок
        connect_timeout=10,
        settings={
            "use_numpy": True,        # Позволяет передавать numpy-типизованные данные, но не обязательно.
            "max_insert_block_size": 100_000,  # Максимальный размер блоков вставки
            "max_threads": 8,         # Максимальное количество потоков для выполнения запроса
            "max_memory_usage": 10_000_000_000,  # Максимальное использование памяти
            "max_execution_time": 60,  # Максимальное время выполнения запроса в секундах
            "compress": True          # Сжатие данных при передаче
        },
        compression=True  # Включаем сжатие для уменьшения объема передаваемых данных
    )
    return client


def execute_with_retry(client: Client, query: str, params: Optional[Dict[str, Any]] = None, 
                       data=None, retry_count: int = MAX_RETRIES) -> Any:
    """
    Выполняет запрос к ClickHouse с автоматическими повторными попытками при ошибках сети.
    
    Args:
        client: Клиент ClickHouse
        query: SQL-запрос
        params: Параметры запроса (для параметризованных запросов)
        data: Данные для вставки (для INSERT запросов)
        retry_count: Количество повторных попыток
        
    Returns:
        Результат выполнения запроса
    """
    attempt = 0
    last_exception = None
    
    while attempt < retry_count:
        try:
            if data is not None:
                return client.execute(query, data)
            elif params is not None:
                return client.execute(query, params)
            else:
                return client.execute(query)
        except (NetworkError, ServerException) as e:
            last_exception = e
            attempt += 1
            if attempt < retry_count:
                sleep_time = RETRY_DELAY * (2 ** (attempt - 1))  # Экспоненциальная задержка
                logger.warning(f"ClickHouse query failed (attempt {attempt}/{retry_count}): {str(e)}. "
                               f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"ClickHouse query failed after {retry_count} attempts: {str(e)}")
                raise
    
    # Если мы здесь, значит все попытки не удались
    if last_exception:
        raise last_exception
    return None


def bulk_insert(client: Client, table: str, columns: list, data: list, 
                batch_size: int = 10000) -> int:
    """
    Выполняет массовую вставку данных в ClickHouse с разбивкой на батчи.
    
    Args:
        client: Клиент ClickHouse
        table: Имя таблицы
        columns: Список колонок
        data: Список кортежей с данными
        batch_size: Размер батча для вставки
        
    Returns:
        Количество успешно вставленных строк
    """
    total_rows = len(data)
    inserted_rows = 0
    
    if total_rows == 0:
        return 0
    
    # Формируем SQL запрос для вставки
    columns_str = ", ".join(columns)
    query = f"INSERT INTO {table} ({columns_str}) VALUES"
    
    # Разбиваем данные на батчи
    for i in range(0, total_rows, batch_size):
        batch = data[i:i + batch_size]
        try:
            execute_with_retry(client, query, data=batch)
            inserted_rows += len(batch)
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
        except Exception as e:
            logger.error(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
            # Пробуем вставить по одной строке
            for row in batch:
                try:
                    execute_with_retry(client, query, data=[row])
                    inserted_rows += 1
                except Exception as row_error:
                    logger.error(f"Error inserting row: {str(row_error)}")
    
    return inserted_rows