#!/usr/bin/env python3
"""
Commerce7 Data Ingestion Script - Production Version

This script is optimized for running as a CRON job on Render.
It includes proper error handling, logging, and exit codes for monitoring.
"""

import os
import sys
import logging
import json
import time
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import QueuePool
import numpy as np

# Load environment variables
load_dotenv()

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def retry_on_db_error(max_retries: int = 3, base_delay: float = 1.0):
    """Decorator to retry database operations with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, SQLAlchemyError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Database operation failed after {max_retries} attempts: {str(e)}")
                        raise last_exception
            return None
        return wrapper
    return decorator

def get_database_path() -> str:
    """Get the correct database connection string for PostgreSQL."""
    # Check for PostgreSQL connection string first
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        logger.info("Using PostgreSQL connection from DATABASE_URL")
        # Add SSL and timeout parameters for Render PostgreSQL
        if 'postgresql' in database_url and 'render.com' in database_url:
            separator = '&' if '?' in database_url else '?'
            database_url += f"{separator}sslmode=require&connect_timeout=30&application_name=dashdon_ingest"
        return database_url
    
    # Check for individual PostgreSQL environment variables
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    if all([db_host, db_name, db_user, db_password]):
        logger.info("Using PostgreSQL connection from individual environment variables")
        ssl_params = "?sslmode=require&connect_timeout=30&application_name=dashdon_ingest" if 'render.com' in db_host else ""
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}{ssl_params}"
    
    raise ValueError("No PostgreSQL configuration found. Please set DATABASE_URL or individual DB_* variables.")

def create_database_engine():
    """Create a database engine with proper connection pooling and timeout settings."""
    database_url = get_database_path()
    
    if 'postgresql' in database_url:
        # PostgreSQL-specific engine configuration
        engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                'connect_timeout': 30,
                'application_name': 'dashdon_ingest'
            }
        )
        logger.debug("Created PostgreSQL engine with connection pooling")
    else:
        raise ValueError("Only PostgreSQL is supported in production")
    
    return engine

class Commerce7Client:
    """Client for interacting with Commerce7 API."""
    
    def __init__(self):
        self.auth_token = os.getenv('C7_AUTH_TOKEN')
        self.tenant = os.getenv('C7_TENANT')
        
        if not self.auth_token or not self.tenant:
            raise ValueError("C7_AUTH_TOKEN and C7_TENANT environment variables are required")
        
        self.base_url = f"https://api.commerce7.com/v1"
        self.headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }
        self.engine = create_database_engine()
    
    @retry_on_db_error(max_retries=3, base_delay=2.0)
    def get_watermark(self, table: str) -> Optional[datetime]:
        """Get the watermark for a table."""
        with self.engine.connect() as conn:
            # Create watermark table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS watermark (
                id VARCHAR(255) PRIMARY KEY,
                last_processed_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            
            # Get the watermark
            result = conn.execute(
                text("SELECT last_processed_at FROM watermark WHERE id = :table"),
                {"table": table}
            ).fetchone()
            
            if result and result[0]:
                return result[0]
            return None
    
    def fetch_data(self, endpoint: str, watermark: Optional[datetime] = None) -> List[Dict]:
        """Fetch data from Commerce7 API with pagination."""
        all_data = []
        cursor = None
        total_records = 0
        
        logger.info(f"Starting data fetch for endpoint: {endpoint}")
        
        while True:
            url = f"{self.base_url}/{endpoint}"
            params = {}
            
            if cursor:
                params['cursor'] = cursor
            
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                records = data.get('data', [])
                
                if not records:
                    logger.info("No more records to fetch")
                    break
                
                # Filter by watermark if provided
                if watermark:
                    filtered_records = []
                    for record in records:
                        updated_at = record.get('updatedAt')
                        if updated_at:
                            record_datetime = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                            if record_datetime > watermark:
                                filtered_records.append(record)
                    records = filtered_records
                
                all_data.extend(records)
                total_records += len(records)
                
                logger.info(f"Fetched {len(records)} records (total: {total_records})")
                
                # Get next cursor
                cursor = data.get('pagination', {}).get('cursor')
                if not cursor:
                    logger.info("No more pages to fetch")
                    break
                
                # Small delay to be respectful to the API
                time.sleep(0.1)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {e}")
                raise
        
        logger.info(f"Completed data fetch. Total records: {total_records}")
        return all_data
    
    @retry_on_db_error(max_retries=3, base_delay=2.0)
    def upsert_data(self, table: str, data: List[Dict]):
        """Upsert data into the database."""
        if not data:
            logger.info(f"No data to upsert for table: {table}")
            return
        
        with self.engine.connect() as conn:
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id VARCHAR(255) PRIMARY KEY,
                data JSONB NOT NULL,
                last_processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(text(create_table_sql))
            
            # Upsert data
            upsert_sql = f"""
            INSERT INTO {table} (id, data, last_processed_at)
            VALUES (:id, :data, :last_processed_at)
            ON CONFLICT (id) DO UPDATE SET
                data = EXCLUDED.data,
                last_processed_at = EXCLUDED.last_processed_at,
                updated_at = CURRENT_TIMESTAMP
            """
            
            for record in data:
                record_id = record.get('id')
                if record_id:
                    conn.execute(text(upsert_sql), {
                        "id": record_id,
                        "data": json.dumps(record),
                        "last_processed_at": datetime.now(timezone.utc)
                    })
            
            conn.commit()
            logger.info(f"Upserted {len(data)} records into {table}")
    
    def process_endpoint(self, endpoint: str) -> bool:
        """Process a single endpoint with error handling."""
        try:
            logger.info(f"Processing endpoint: {endpoint}")
            
            # Get watermark
            watermark = self.get_watermark(endpoint)
            if watermark:
                logger.info(f"Using watermark: {watermark}")
            
            # Fetch data
            data = self.fetch_data(endpoint, watermark)
            
            if data:
                # Upsert data
                self.upsert_data(f"raw_{endpoint}", data)
                
                # Update watermark
                with self.engine.connect() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO watermark (id, last_processed_at)
                            VALUES (:table, :timestamp)
                            ON CONFLICT (id) DO UPDATE SET
                                last_processed_at = EXCLUDED.last_processed_at,
                                updated_at = CURRENT_TIMESTAMP
                        """),
                        {
                            "table": endpoint,
                            "timestamp": datetime.now(timezone.utc)
                        }
                    )
                    conn.commit()
                
                logger.info(f"Successfully processed {len(data)} records for {endpoint}")
                return True
            else:
                logger.info(f"No new data for {endpoint}")
                return True
                
        except Exception as e:
            logger.error(f"Error processing {endpoint}: {e}")
            return False

def main():
    """Main function for production ingestion."""
    start_time = datetime.now()
    logger.info("Starting Commerce7 data ingestion")
    
    # Validate environment
    required_vars = ['C7_AUTH_TOKEN', 'C7_TENANT']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Define endpoints to process
    endpoints = [
        'customer',
        'order', 
        'product',
        'club-membership'
    ]
    
    client = Commerce7Client()
    success_count = 0
    total_endpoints = len(endpoints)
    
    for endpoint in endpoints:
        try:
            if client.process_endpoint(endpoint):
                success_count += 1
            else:
                logger.error(f"Failed to process {endpoint}")
        except Exception as e:
            logger.error(f"Unexpected error processing {endpoint}: {e}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"Ingestion completed in {duration}")
    logger.info(f"Successfully processed {success_count}/{total_endpoints} endpoints")
    
    # Exit with appropriate code
    if success_count == total_endpoints:
        logger.info("All endpoints processed successfully")
        
        # Run dbt transformations if ingestion was successful
        logger.info("Starting dbt transformations...")
        try:
            dbt_result = subprocess.run(
                ["python", "run_dbt.py"],
                capture_output=True,
                text=True,
                check=False
            )
            
            if dbt_result.returncode == 0:
                logger.info("✅ dbt transformations completed successfully")
                sys.exit(0)
            else:
                logger.error(f"❌ dbt transformations failed with exit code {dbt_result.returncode}")
                if dbt_result.stdout:
                    logger.error(f"dbt stdout: {dbt_result.stdout}")
                if dbt_result.stderr:
                    logger.error(f"dbt stderr: {dbt_result.stderr}")
                sys.exit(1)
                
        except Exception as e:
            logger.error(f"❌ Failed to run dbt transformations: {e}")
            sys.exit(1)
    else:
        logger.error(f"Some endpoints failed ({success_count}/{total_endpoints})")
        logger.info("Skipping dbt transformations due to ingestion failures")
        sys.exit(1)

if __name__ == "__main__":
    main() 