import time
import logging
from typing import Dict, Any, Optional
from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, ServerException

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 2


def get_clickhouse_client(host: str = "localhost",
                          port: int = 9000,
                          user: str = "default",
                          password: str = "",
                          database: str = "default") -> Client:

    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        send_receive_timeout=300,
        connect_timeout=10,
        settings={
            "use_numpy": True,
            "max_insert_block_size": 100_000,
            "max_threads": 8,
            "max_memory_usage": 10_000_000_000,
            "max_execution_time": 60,
            "compress": True
        },
        compression=True
    )
    return client


def execute_with_retry(client: Client, query: str, params: Optional[Dict[str, Any]] = None, 
                       data=None, retry_count: int = MAX_RETRIES) -> Any:

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
                sleep_time = RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning(f"ClickHouse query failed (attempt {attempt}/{retry_count}): {str(e)}. "
                               f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"ClickHouse query failed after {retry_count} attempts: {str(e)}")
                raise
    
    if last_exception:
        raise last_exception
    return None


def bulk_insert(client: Client, table: str, columns: list, data: list, 
                batch_size: int = 10000) -> int:

    total_rows = len(data)
    inserted_rows = 0
    
    if total_rows == 0:
        return 0
    
    columns_str = ", ".join(columns)
    query = f"INSERT INTO {table} ({columns_str}) VALUES"
    
    for i in range(0, total_rows, batch_size):
        batch = data[i:i + batch_size]
        try:
            execute_with_retry(client, query, data=batch)
            inserted_rows += len(batch)
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
        except Exception as e:
            logger.error(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
            for row in batch:
                try:
                    execute_with_retry(client, query, data=[row])
                    inserted_rows += 1
                except Exception as row_error:
                    logger.error(f"Error inserting row: {str(row_error)}")
    
    return inserted_rows