#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_products_data():
    """Create sample products data and save as Parquet file"""
    
    try:
        data = {
            "product_id": np.arange(1, 101),
            "product_name": [f"Product {i}" for i in range(1, 101)],
            "category": np.random.choice(["Electronics", "Clothing", "Books", "Home", "Sports"], 100),
            "price": np.random.uniform(10, 500, 100).round(2),
            "stock_quantity": np.random.randint(0, 100, 100)
        }
        
        df = pd.DataFrame(data)
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        os.makedirs(current_dir, exist_ok=True)
        
        parquet_path = os.path.join(current_dir, "products.parquet")
        df.to_parquet(parquet_path, index=False)
        
        logger.info(f"Created sample products data: {parquet_path}")
        return parquet_path
    except Exception as e:
        logger.error(f"Error creating sample data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parquet_path = os.path.join(current_dir, "products.parquet")
        
        if os.path.exists(parquet_path):
            logger.info(f"Sample products data already exists at: {parquet_path}")
        else:
            create_products_data()
    except Exception as e:
        logger.error(f"Failed to create sample data: {str(e)}")
        exit(1) 