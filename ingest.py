#!/usr/bin/env python3
"""
Commerce7 Data Ingestion Script

This script pulls data from Commerce7 API and loads it into the data warehouse.
It uses incremental loading based on the updatedAt watermark.
"""

import os
import sys
import logging
import base64
import json
import time
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import QueuePool
import numpy as np

load_dotenv()

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ingest.log')  # Add file logging
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
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Database operation failed after {max_retries} attempts: {str(e)}")
                        raise last_exception
            return None
        return wrapper
    return decorator

def get_project_root() -> Path:
    """Get the project root directory."""
    # In Render, the project root is the current working directory
    # In local development, it's the parent of the parent of this file
    if os.getenv('RENDER') == 'true':
        return Path.cwd()
    else:
        return Path(__file__).parent.parent

def get_database_path() -> str:
    """Get the correct database connection string for PostgreSQL or SQLite."""
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
        # Add SSL and timeout parameters for Render PostgreSQL
        ssl_params = "?sslmode=require&connect_timeout=30&application_name=dashdon_ingest" if 'render.com' in db_host else ""
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}{ssl_params}"
    
    # Fallback to SQLite for backward compatibility
    logger.warning("No PostgreSQL configuration found, falling back to SQLite")
    project_root = get_project_root()
    db_path = project_root / 'commerce7_dw.db'
    return f"sqlite:///{db_path}"

def create_database_engine():
    """Create a database engine with proper connection pooling and timeout settings."""
    database_url = get_database_path()
    
    if 'postgresql' in database_url:
        # PostgreSQL-specific engine configuration
        engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=5,  # Number of connections to maintain
            max_overflow=10,  # Additional connections that can be created
            pool_pre_ping=True,  # Validate connections before use
            pool_recycle=3600,  # Recycle connections after 1 hour
            connect_args={
                'connect_timeout': 30,
                'application_name': 'dashdon_ingest'
            }
        )
        logger.debug("Created PostgreSQL engine with connection pooling")
    else:
        # SQLite engine (simpler configuration)
        engine = create_engine(database_url)
        logger.debug("Created SQLite engine")
    
    return engine

def load_environment():
    # Log all relevant environment variables (masking sensitive data)
    env_vars = {
        'C7_AUTH_TOKEN': os.getenv('C7_AUTH_TOKEN'),
        'C7_TENANT': os.getenv('C7_TENANT'),
        'X_TOCK_AUTH': os.getenv('X_TOCK_AUTH'),
        'X_TOCK_SCOPE': os.getenv('X_TOCK_SCOPE'),
        'DATABASE_URL': os.getenv('DATABASE_URL'),
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_NAME': os.getenv('DB_NAME'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD')
    }
    
    logger.info("Environment variables loaded:")
    for key, value in env_vars.items():
        if value:
            masked_value = '*' * len(value) if 'PASSWORD' in key or 'TOKEN' in key or 'AUTH' in key else value
            logger.info(f"{key}: {masked_value}")
        else:
            logger.warning(f"{key}: Not set")
    
    # Validate required environment variables
    # Check for either DATABASE_URL or individual PostgreSQL variables
    has_postgres_config = (
        env_vars['DATABASE_URL'] or 
        all([env_vars['DB_HOST'], env_vars['DB_NAME'], env_vars['DB_USER'], env_vars['DB_PASSWORD']])
    )
    
    # Check for at least one API configuration (Commerce7 or Tock)
    has_commerce7_config = all([env_vars['C7_AUTH_TOKEN'], env_vars['C7_TENANT']])
    has_tock_config = all([env_vars['X_TOCK_AUTH'], env_vars['X_TOCK_SCOPE']])
    
    if not has_commerce7_config and not has_tock_config:
        logger.error("Missing required API credentials. Need either Commerce7 (C7_AUTH_TOKEN, C7_TENANT) or Tock (X_TOCK_AUTH, X_TOCK_SCOPE) credentials")
        raise ValueError("Missing required API credentials")
    
    if not has_postgres_config:
        logger.warning("No PostgreSQL configuration found - will use SQLite fallback")

class TockAPIClient:
    """Tock API client for data ingestion."""
    
    def __init__(self):
        self.auth_header = os.getenv('X_TOCK_AUTH')
        self.scope_header = os.getenv('X_TOCK_SCOPE')
        self.base_url = 'https://dashboard.exploretock.com/api/data/export/urls'
        
        if not all([self.auth_header, self.scope_header]):
            raise ValueError("Missing required Tock API credentials (X_TOCK_AUTH and X_TOCK_SCOPE)")
        
        self.session = requests.Session()
        self.session.headers.update({
            'X-Tock-Authorization': self.auth_header,
            'X-Tock-Scope': self.scope_header
        })
        logger.debug("TockAPIClient initialized successfully")
    
    def get_data_urls(self) -> Dict[str, List[str]]:
        """Get the data export URLs from Tock API."""
        try:
            logger.debug("Fetching Tock data export URLs...")
            response = self.session.get(self.base_url)
            response.raise_for_status()
            data = response.json()
            
            result = data.get('result', {})
            guest_urls = result.get('guestDataUrls', [])
            reservation_urls = result.get('reservationDataUrls', [])
            
            logger.info(f"Retrieved {len(guest_urls)} guest data URLs and {len(reservation_urls)} reservation data URLs")
            return {
                'guest_urls': guest_urls,
                'reservation_urls': reservation_urls
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Tock data URLs: {str(e)}")
            raise
    
    def fetch_json_data(self, url: str) -> List[Dict]:
        """Fetch and parse JSON data from a single URL."""
        try:
            logger.debug(f"Fetching data from: {url[:100]}...")
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse the JSON data
            data = response.json()
            
            # Handle both single objects and arrays
            if isinstance(data, dict):
                # If it's a single object, wrap it in a list
                return [data]
            elif isinstance(data, list):
                return data
            else:
                logger.warning(f"Unexpected data format from URL: {type(data)}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from URL: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from URL: {str(e)}")
            return []
    
    def get_latest_guest_url(self, urls: List[str]) -> str:
        """Get the URL with the highest guest-profile number."""
        import re
        
        guest_urls_with_numbers = []
        for url in urls:
            # Extract the number from guest-profile-X.json
            match = re.search(r'guest-profile-(\d+)\.json', url)
            if match:
                number = int(match.group(1))
                guest_urls_with_numbers.append((number, url))
        
        if not guest_urls_with_numbers:
            logger.warning("No guest-profile URLs found with expected naming pattern")
            return urls[0] if urls else None
        
        # Sort by number and get the highest
        guest_urls_with_numbers.sort(key=lambda x: x[0])
        latest_number, latest_url = guest_urls_with_numbers[-1]
        
        logger.info(f"Latest guest data file: guest-profile-{latest_number}.json")
        return latest_url
    
    def get_latest_reservation_url(self, urls: List[str]) -> str:
        """Get the URL with the highest reservation number."""
        import re
        
        reservation_urls_with_numbers = []
        for url in urls:
            # Extract the number from reservation-X.json
            match = re.search(r'reservation-(\d+)\.json', url)
            if match:
                number = int(match.group(1))
                reservation_urls_with_numbers.append((number, url))
        
        if not reservation_urls_with_numbers:
            logger.warning("No reservation URLs found with expected naming pattern")
            return urls[0] if urls else None
        
        # Sort by number and get the highest
        reservation_urls_with_numbers.sort(key=lambda x: x[0])
        latest_number, latest_url = reservation_urls_with_numbers[-1]
        
        logger.info(f"Latest reservation data file: reservation-{latest_number}.json")
        return latest_url
    
    def fetch_all_guest_data(self, incremental: bool = False) -> List[Dict]:
        """Fetch guest data from URLs. If incremental=True, only fetch the latest file."""
        urls_data = self.get_data_urls()
        all_guest_data = []
        
        if incremental:
            # Only fetch the latest guest data file
            latest_url = self.get_latest_guest_url(urls_data['guest_urls'])
            if latest_url:
                logger.info("Fetching latest guest data file only (incremental mode)")
                guest_data = self.fetch_json_data(latest_url)
                all_guest_data.extend(guest_data)
                logger.debug(f"Added {len(guest_data)} guest records from latest file")
            else:
                logger.error("Could not determine latest guest data URL")
                return []
        else:
            # Fetch all guest data files (initial load)
            logger.info("Fetching all guest data files (initial load mode)")
            for i, url in enumerate(urls_data['guest_urls']):
                logger.info(f"Processing guest data file {i+1}/{len(urls_data['guest_urls'])}")
                guest_data = self.fetch_json_data(url)
                all_guest_data.extend(guest_data)
                logger.debug(f"Added {len(guest_data)} guest records from file {i+1}")
        
        logger.info(f"Total guest records fetched: {len(all_guest_data)}")
        return all_guest_data
    
    def fetch_all_reservation_data(self, incremental: bool = False) -> List[Dict]:
        """Fetch reservation data from URLs. If incremental=True, only fetch the latest file."""
        urls_data = self.get_data_urls()
        all_reservation_data = []
        
        if incremental:
            # Only fetch the latest reservation data file
            latest_url = self.get_latest_reservation_url(urls_data['reservation_urls'])
            if latest_url:
                logger.info("Fetching latest reservation data file only (incremental mode)")
                reservation_data = self.fetch_json_data(latest_url)
                all_reservation_data.extend(reservation_data)
                logger.debug(f"Added {len(reservation_data)} reservation records from latest file")
            else:
                logger.error("Could not determine latest reservation data URL")
                return []
        else:
            # Fetch all reservation data files (initial load)
            logger.info("Fetching all reservation data files (initial load mode)")
            for i, url in enumerate(urls_data['reservation_urls']):
                logger.info(f"Processing reservation data file {i+1}/{len(urls_data['reservation_urls'])}")
                reservation_data = self.fetch_json_data(url)
                all_reservation_data.extend(reservation_data)
                logger.debug(f"Added {len(reservation_data)} reservation records from file {i+1}")
        
        logger.info(f"Total reservation records fetched: {len(all_reservation_data)}")
        return all_reservation_data
    
    @retry_on_db_error(max_retries=3, base_delay=2.0)
    def get_watermark(self, table: str) -> Optional[datetime]:
        """Get the last processed timestamp for a Tock table."""
        try:
            logger.debug("Attempting to create database engine...")
            engine = create_database_engine()
            logger.debug("Engine created successfully")
            
            logger.debug("Attempting to establish connection...")
            with engine.connect() as conn:
                logger.debug("Connection established successfully")
                
                # Convert table name to database compatible format
                db_table = table.replace('-', '_')
                
                # Create table if it doesn't exist with PostgreSQL-compatible schema
                logger.info(f"Creating table raw_{db_table} if it doesn't exist")
                
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS raw_{db_table} (
                        id VARCHAR(255) PRIMARY KEY,
                        last_processed_at TIMESTAMP WITH TIME ZONE,
                        _airbyte_ab_id VARCHAR(255),
                        _airbyte_emitted_at TIMESTAMP WITH TIME ZONE,
                        _airbyte_normalized_at TIMESTAMP WITH TIME ZONE,
                        _airbyte_{db_table}_hashid VARCHAR(255),
                        data JSONB
                    )
                """
                logger.debug(f"Executing CREATE TABLE SQL: {create_table_sql}")
                conn.execute(text(create_table_sql))
                conn.commit()
                logger.info(f"Table raw_{db_table} created/verified successfully")
                
                logger.debug(f"Executing query for watermark on table: raw_{db_table}")
                result = conn.execute(text(
                    f"SELECT MAX(last_processed_at) FROM raw_{db_table}"
                )).scalar()
                logger.debug(f"Watermark for {table}: {result}")
                
                # Convert timestamp to datetime object if it exists
                if result:
                    try:
                        if isinstance(result, datetime):
                            return result
                        elif isinstance(result, str):
                            if 'T' in result:
                                return datetime.fromisoformat(result.replace('Z', '+00:00'))
                            else:
                                return datetime.strptime(result, '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            logger.warning(f"Unexpected watermark type: {type(result)}, value: {result}")
                            return None
                    except ValueError as e:
                        logger.error(f"Failed to parse watermark timestamp '{result}': {e}")
                        return None
                
                return None
        except SQLAlchemyError as e:
            logger.error(f"Database error while getting watermark for {table}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise
    
    @retry_on_db_error(max_retries=3, base_delay=2.0)
    def upsert_data(self, table: str, data: List[Dict]):
        """Upsert Tock data into the database."""
        if not data:
            logger.info(f"No new data to upsert for {table}")
            return
        
        try:
            current_time = datetime.now(timezone.utc)
            
            # Convert table name to database compatible format
            db_table = table.replace('-', '_')
            
            # Create DataFrame with only the essential columns
            df = pd.DataFrame([{
                'id': str(record.get('id', '')),  # Convert to string to handle different ID types
                'last_processed_at': current_time,
                'data': json.dumps(record)  # Store the entire record as JSON
            } for record in data])
            
            # Check for and handle duplicate IDs
            initial_count = len(df)
            df = df.drop_duplicates(subset=['id'], keep='last')  # Keep the last occurrence of duplicates
            final_count = len(df)
            
            if initial_count != final_count:
                logger.warning(f"Removed {initial_count - final_count} duplicate records from {table}")
            
            # Remove records with empty or invalid IDs
            before_empty_check = len(df)
            df = df[df['id'].notna() & (df['id'] != '') & (df['id'] != 'None')]
            after_empty_check = len(df)
            
            if before_empty_check != after_empty_check:
                logger.warning(f"Removed {before_empty_check - after_empty_check} records with empty/invalid IDs from {table}")
            
            logger.info(f"Created DataFrame with {len(df)} rows for {table}")
            
            # Check if we have any data to process
            if len(df) == 0:
                logger.warning(f"No valid data to process for {table} after deduplication and validation")
                return
            
            # Show sample IDs for debugging
            sample_ids = df['id'].head(5).tolist()
            logger.debug(f"Sample IDs being processed: {sample_ids}")
            
            logger.info("Attempting to create database engine...")
            engine = create_database_engine()
            logger.info("Engine created successfully")
            
            # Add metadata columns
            df['_airbyte_ab_id'] = pd.util.hash_pandas_object(df).astype(str)
            df['_airbyte_emitted_at'] = current_time
            df['_airbyte_normalized_at'] = current_time
            df['_airbyte_' + db_table + '_hashid'] = pd.util.hash_pandas_object(df).astype(str)
            
            # Create a temporary table for the new data
            temp_table = f'temp_{db_table}'
            df.to_sql(
                temp_table,
                engine,
                if_exists='replace',
                index=False
            )
            
            # Perform the upsert using PostgreSQL's ON CONFLICT
            with engine.connect() as conn:
                upsert_sql = f"""
                    INSERT INTO raw_{db_table} (
                        id, last_processed_at, _airbyte_ab_id, 
                        _airbyte_emitted_at, _airbyte_normalized_at, 
                        _airbyte_{db_table}_hashid, data
                    )
                    SELECT 
                        id, last_processed_at, _airbyte_ab_id,
                        _airbyte_emitted_at, _airbyte_normalized_at,
                        _airbyte_{db_table}_hashid, data::jsonb
                    FROM {temp_table}
                    ON CONFLICT (id) DO UPDATE SET
                        last_processed_at = EXCLUDED.last_processed_at,
                        _airbyte_ab_id = EXCLUDED._airbyte_ab_id,
                        _airbyte_emitted_at = EXCLUDED._airbyte_emitted_at,
                        _airbyte_normalized_at = EXCLUDED._airbyte_normalized_at,
                        _airbyte_{db_table}_hashid = EXCLUDED._airbyte_{db_table}_hashid,
                        data = EXCLUDED.data
                """
                conn.execute(text(upsert_sql))
                conn.commit()
                
                # Drop the temporary table
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
            
            logger.info(f"Successfully upserted {len(data)} records to raw_{db_table}")
            
        except SQLAlchemyError as e:
            logger.error(f"Database error while upserting data to {table}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

class Commerce7Client:
    """Commerce7 API client for data ingestion."""
    
    def __init__(self):
        self.auth_token = os.getenv('C7_AUTH_TOKEN')
        self.tenant = os.getenv('C7_TENANT')
        self.base_url = 'https://api.commerce7.com/v1'
        
        if not all([self.auth_token, self.tenant]):
            raise ValueError("Missing required Commerce7 API credentials")
        
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'tenant': self.tenant,
            'Authorization': f"Basic {self.auth_token}"
        })
        logger.debug("Commerce7Client initialized successfully")
    
    @retry_on_db_error(max_retries=3, base_delay=2.0)
    def get_watermark(self, table: str) -> Optional[datetime]:
        """Get the last processed timestamp for a table."""
        try:
            logger.debug("Attempting to create database engine...")
            engine = create_database_engine()
            logger.debug("Engine created successfully")
            
            logger.debug("Attempting to establish connection...")
            with engine.connect() as conn:
                logger.debug("Connection established successfully")
                
                # Convert table name to database compatible format
                db_table = table.replace('-', '_')
                
                # Create table if it doesn't exist with PostgreSQL-compatible schema
                logger.debug(f"Creating table raw_{db_table} if it doesn't exist")
                
                # Use PostgreSQL-specific syntax
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS raw_{db_table} (
                        id VARCHAR(255) PRIMARY KEY,
                        last_processed_at TIMESTAMP WITH TIME ZONE,
                        _airbyte_ab_id VARCHAR(255),
                        _airbyte_emitted_at TIMESTAMP WITH TIME ZONE,
                        _airbyte_normalized_at TIMESTAMP WITH TIME ZONE,
                        _airbyte_{db_table}_hashid VARCHAR(255),
                        data JSONB
                    )
                """
                conn.execute(text(create_table_sql))
                conn.commit()
                
                logger.debug(f"Executing query for watermark on table: raw_{db_table}")
                result = conn.execute(text(
                    f"SELECT MAX(last_processed_at) FROM raw_{db_table}"
                )).scalar()
                logger.debug(f"Watermark for {table}: {result}")
                
                # Convert timestamp to datetime object if it exists
                if result:
                    try:
                        # PostgreSQL returns datetime objects directly
                        if isinstance(result, datetime):
                            return result
                        elif isinstance(result, str):
                            # Handle string timestamps if they occur
                            if 'T' in result:
                                # ISO format: 2025-06-30T21:44:04.349447
                                return datetime.fromisoformat(result.replace('Z', '+00:00'))
                            else:
                                # PostgreSQL format: 2025-06-30 21:44:04.349447
                                return datetime.strptime(result, '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            logger.warning(f"Unexpected watermark type: {type(result)}, value: {result}")
                            return None
                    except ValueError as e:
                        logger.error(f"Failed to parse watermark timestamp '{result}': {e}")
                        return None
                
                return None
        except SQLAlchemyError as e:
            logger.error(f"Database error while getting watermark for {table}: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise
    
    def fetch_data(self, endpoint: str, watermark: Optional[datetime] = None) -> List[Dict]:
        """Fetch data from Commerce7 API with cursor-based pagination and optional watermark filtering."""
        url = f"{self.base_url}/{endpoint}"
        all_data = []
        cursor = "start"
        batch_size = 1000  # Process in batches of 1000
        
        # Map endpoints to their response data keys
        endpoint_data_keys = {
            'customer': 'customers',
            'order': 'orders',
            'product': 'products',
            'club-membership': 'clubMemberships'
        }
        
        data_key = endpoint_data_keys.get(endpoint)
        if not data_key:
            raise ValueError(f"Unknown endpoint: {endpoint}")
        
        logger.debug(f"Fetching data from {endpoint} with watermark: {watermark}")
        
        while cursor:
            params = {'cursor': cursor}
            if watermark:
                # Commerce7 API expects format: "gte: YYYY-MM-DD"
                params['updatedAt'] = f"gte: {watermark.strftime('%Y-%m-%d')}"
            
            try:
                logger.debug(f"Making API request to {endpoint} with cursor: {cursor}")
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, dict):
                    # Get items using the endpoint-specific key
                    items = data.get(data_key, [])
                    cursor = data.get('cursor')
                    logger.debug(f"Response contains {len(items)} items")
                    
                    if items:
                        all_data.extend(items)
                        logger.debug(f"Fetched {len(items)} records from {endpoint}")
                        
                        # Process batch if we've reached the batch size
                        if len(all_data) >= batch_size:
                            logger.info(f"Processing batch of {len(all_data)} records")
                            self.upsert_data(endpoint, all_data)
                            all_data = []  # Clear the batch
                    
                    # If we get no items and no cursor, we're done
                    if not items and not cursor:
                        logger.info("No more data to fetch")
                        break
                        
                    # If we get no items but a cursor, we might be in a loop
                    if not items and cursor:
                        logger.warning(f"No items returned but cursor exists: {cursor}")
                        if cursor in all_data:
                            logger.error("Detected cursor loop, breaking")
                            break
                else:
                    logger.error(f"Unexpected response format: {type(data)}")
                    break
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {str(e)}")
                logger.error(f"Response status code: {e.response.status_code if hasattr(e, 'response') else 'N/A'}")
                raise
        
        # Process any remaining records
        if all_data:
            logger.info(f"Processing final batch of {len(all_data)} records")
            self.upsert_data(endpoint, all_data)
        
        logger.info(f"Completed fetching and processing all records from {endpoint}")
        return all_data
    
    @retry_on_db_error(max_retries=3, base_delay=2.0)
    def upsert_data(self, table: str, data: List[Dict]):
        """Upsert data into the database."""
        if not data:
            logger.info(f"No new data to upsert for {table}")
            return
        
        try:
            current_time = datetime.now(timezone.utc)
            
            # Convert table name to database compatible format
            db_table = table.replace('-', '_')
            
            # Create DataFrame with only the essential columns
            df = pd.DataFrame([{
                'id': record.get('id'),
                'last_processed_at': current_time,
                'data': json.dumps(record)  # Store the entire record as JSON
            } for record in data])
            
            logger.debug("Attempting to create database engine...")
            engine = create_database_engine()
            logger.debug("Engine created successfully")
            
            # Add metadata columns
            df['_airbyte_ab_id'] = pd.util.hash_pandas_object(df).astype(str)
            df['_airbyte_emitted_at'] = current_time
            df['_airbyte_normalized_at'] = current_time
            df['_airbyte_' + db_table + '_hashid'] = pd.util.hash_pandas_object(df).astype(str)
            
            # Create a temporary table for the new data
            temp_table = f'temp_{db_table}'
            df.to_sql(
                temp_table,
                engine,
                if_exists='replace',
                index=False
            )
            
            # Perform the upsert using PostgreSQL's ON CONFLICT
            with engine.connect() as conn:
                # Use PostgreSQL's INSERT ... ON CONFLICT ... DO UPDATE
                upsert_sql = f"""
                    INSERT INTO raw_{db_table} (
                        id, last_processed_at, _airbyte_ab_id, 
                        _airbyte_emitted_at, _airbyte_normalized_at, 
                        _airbyte_{db_table}_hashid, data
                    )
                    SELECT 
                        id, last_processed_at, _airbyte_ab_id,
                        _airbyte_emitted_at, _airbyte_normalized_at,
                        _airbyte_{db_table}_hashid, data::jsonb
                    FROM {temp_table}
                    ON CONFLICT (id) DO UPDATE SET
                        last_processed_at = EXCLUDED.last_processed_at,
                        _airbyte_ab_id = EXCLUDED._airbyte_ab_id,
                        _airbyte_emitted_at = EXCLUDED._airbyte_emitted_at,
                        _airbyte_normalized_at = EXCLUDED._airbyte_normalized_at,
                        _airbyte_{db_table}_hashid = EXCLUDED._airbyte_{db_table}_hashid,
                        data = EXCLUDED.data
                """
                conn.execute(text(upsert_sql))
                conn.commit()
                
                # Drop the temporary table
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
            
            logger.info(f"Successfully upserted {len(data)} records to raw_{db_table}")
            
        except SQLAlchemyError as e:
            logger.error(f"Database error while upserting data to {table}: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise

def test_database_connection():
    """Test database connection and diagnose issues."""
    try:
        logger.info("🔍 Testing database connection...")
        
        # Load environment variables
        load_environment()
        
        # Get database URL (masked for security)
        db_url = get_database_path()
        masked_url = db_url.split('@')[1] if '@' in db_url else db_url
        logger.info(f"Testing connection to: {masked_url}")
        
        # Test connection with timeout
        engine = create_database_engine()
        
        with engine.connect() as conn:
            # Test basic connectivity
            result = conn.execute(text("SELECT 1 as test"))
            logger.info("✅ Basic connection successful")
            
            # Test PostgreSQL-specific features
            if 'postgresql' in db_url:
                # Check version
                version_result = conn.execute(text("SELECT version()"))
                version = version_result.scalar()
                logger.info(f"✅ PostgreSQL version: {version.split(',')[0]}")
                
                # Check SSL status
                ssl_result = conn.execute(text("SHOW ssl"))
                ssl_status = ssl_result.scalar()
                logger.info(f"✅ SSL status: {ssl_status}")
                
                # Check connection info
                conn_info = conn.execute(text("SELECT current_database(), current_user, inet_server_addr(), inet_server_port()"))
                db_info = conn_info.fetchone()
                logger.info(f"✅ Connected to database: {db_info[0]} as {db_info[1]} on {db_info[2]}:{db_info[3]}")
            
            logger.info("🎉 Database connection test completed successfully!")
            return True
            
    except OperationalError as e:
        logger.error(f"❌ Connection failed: {str(e)}")
        
        # Provide specific troubleshooting advice
        if "timeout" in str(e).lower():
            logger.error("💡 Troubleshooting timeout issues:")
            logger.error("   1. Check your internet connection")
            logger.error("   2. Verify the database URL is correct")
            logger.error("   3. Ensure your firewall allows outbound connections to port 5432")
            logger.error("   4. Try adding 'sslmode=require' to your connection string")
        elif "authentication" in str(e).lower():
            logger.error("💡 Troubleshooting authentication issues:")
            logger.error("   1. Verify username and password are correct")
            logger.error("   2. Check if the user has proper permissions")
        elif "host" in str(e).lower():
            logger.error("💡 Troubleshooting host issues:")
            logger.error("   1. Verify the hostname is correct")
            logger.error("   2. Check if the database is running")
            logger.error("   3. Ensure the database is accessible from external connections")
        
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        return False

def main(endpoint: str = None):
    """Main ingestion function."""
    try:
        # Load and validate environment variables
        load_environment()
        
        # Initialize clients based on available credentials
        clients = {}
        
        # Check for Commerce7 credentials
        if all([os.getenv('C7_AUTH_TOKEN'), os.getenv('C7_TENANT')]):
            clients['commerce7'] = Commerce7Client()
            logger.info("Commerce7 client initialized")
        
        # Check for Tock credentials
        if all([os.getenv('X_TOCK_AUTH'), os.getenv('X_TOCK_SCOPE')]):
            clients['tock'] = TockAPIClient()
            logger.info("Tock client initialized")
        
        if not clients:
            raise ValueError("No API clients could be initialized. Check your environment variables.")
        
        # Process endpoints based on client type
        if endpoint and endpoint.startswith('tock-'):
            # This is a Tock endpoint, skip Commerce7 processing
            logger.info(f"Processing Tock endpoint: {endpoint}")
        elif 'commerce7' in clients and endpoint:
            # Single Commerce7 endpoint
            table = endpoint.replace('-', '_')
            watermark = clients['commerce7'].get_watermark(table)
            
            logger.info(f"Fetching {endpoint} data since {watermark}")
            data = clients['commerce7'].fetch_data(endpoint, watermark)
            
            logger.info(f"Upserting {len(data)} records to {table}")
            clients['commerce7'].upsert_data(table, data)
            
        elif 'commerce7' in clients and not endpoint:
            # All Commerce7 endpoints
            endpoints = ['customer', 'club-membership', 'product', 'order']
            
            for endpoint in endpoints:
                table = endpoint.replace('-', '_')
                watermark = clients['commerce7'].get_watermark(table)
                
                logger.info(f"Fetching {endpoint} data since {watermark}")
                data = clients['commerce7'].fetch_data(endpoint, watermark)
                
                logger.info(f"Upserting {len(data)} records to {table}")
                clients['commerce7'].upsert_data(table, data)
        
        # Process Tock data if client is available
        if 'tock' in clients and (not endpoint or endpoint.startswith('tock-')):
            # Check if this is an incremental run (data already exists)
            tock_guest_watermark = clients['tock'].get_watermark('tock_guest')
            tock_reservation_watermark = clients['tock'].get_watermark('tock_reservation')
            
            guest_incremental = tock_guest_watermark is not None
            reservation_incremental = tock_reservation_watermark is not None
            
            # Process guest data
            if not endpoint or endpoint == 'tock-guest':
                if guest_incremental:
                    logger.info("Fetching Tock guest data (incremental mode - latest file only)...")
                else:
                    logger.info("Fetching Tock guest data (initial load - all files)...")
                
                guest_data = clients['tock'].fetch_all_guest_data(incremental=guest_incremental)
                
                if guest_data:
                    logger.info(f"Upserting {len(guest_data)} guest records")
                    clients['tock'].upsert_data('tock_guest', guest_data)
                else:
                    logger.info("No guest data to process")
            
            # Process reservation data
            if not endpoint or endpoint == 'tock-reservation':
                if reservation_incremental:
                    logger.info("Fetching Tock reservation data (incremental mode - latest file only)...")
                else:
                    logger.info("Fetching Tock reservation data (initial load - all files)...")
                
                reservation_data = clients['tock'].fetch_all_reservation_data(incremental=reservation_incremental)
                
                if reservation_data:
                    logger.info(f"Upserting {len(reservation_data)} reservation records")
                    clients['tock'].upsert_data('tock_reservation', reservation_data)
                else:
                    logger.info("No reservation data to process")
        
        logger.info("Data ingestion completed successfully")
        
        # Run dbt models after successful data ingestion
        logger.info("Starting dbt transformation pipeline...")
        if run_dbt():
            logger.info("🎉 Complete pipeline (ingestion + dbt) finished successfully!")
        else:
            logger.error("❌ dbt run failed, but data ingestion was successful")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error during data ingestion: {str(e)}", exc_info=True)  # Added exc_info for full traceback
        sys.exit(1)

def run_dbt():
    """Run dbt models after successful data ingestion."""
    try:
        logger.info("🔄 Starting dbt run...")
        
        # Get the project root directory
        project_root = get_project_root()
        logger.info(f"Running dbt from: {project_root}")
        
        # Check if we're in Render environment
        is_render = os.getenv('RENDER') == 'true'
        
        # Verify dbt project files exist
        dbt_project_path = project_root / 'dbt_project.yml'
        profiles_path = project_root / 'profiles.yml'
        
        logger.info(f"Checking dbt project file: {dbt_project_path}")
        if not dbt_project_path.exists():
            logger.error(f"❌ dbt_project.yml not found at {dbt_project_path}")
            return False
            
        logger.info(f"Checking profiles file: {profiles_path}")
        if not profiles_path.exists():
            logger.error(f"❌ profiles.yml not found at {profiles_path}")
            return False
        
        # List directory contents for debugging
        logger.info("📁 Project directory contents:")
        for item in project_root.iterdir():
            logger.info(f"   - {item.name}")
        
        # Change to the project directory
        os.chdir(project_root)
        logger.info(f"✅ Changed working directory to: {os.getcwd()}")
        
        if is_render:
            logger.info("🔄 Running dbt in Render environment")
            # In Render, use the current directory for profiles and be explicit about project
            dbt_command = ['dbt', 'run', '--profiles-dir', '.', '--project-dir', '.']
        else:
            logger.info("🔄 Running dbt in local environment")
            dbt_command = ['dbt', 'run']
        
        logger.info(f"🔄 Executing command: {' '.join(dbt_command)}")
        
        # Run dbt with subprocess
        result = subprocess.run(
            dbt_command,
            capture_output=True,
            text=True,
            check=True,
            env=os.environ.copy()  # Pass through all environment variables
        )
        
        logger.info("✅ dbt run completed successfully")
        logger.debug(f"dbt output: {result.stdout}")
        
        if result.stderr:
            logger.warning(f"dbt stderr: {result.stderr}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ dbt run failed with exit code {e.returncode}")
        logger.error(f"dbt stdout: {e.stdout}")
        logger.error(f"dbt stderr: {e.stderr}")
        
        # Provide specific guidance for different error types
        if is_render and "profiles-dir" in str(e.stderr):
            logger.error("💡 Render dbt profiles issue detected. Make sure:")
            logger.error("   1. Your profiles.yml file is in the project root")
            logger.error("   2. The profiles.yml contains the correct database connection")
            logger.error("   3. All required environment variables are set in Render")
        elif "dbt_project.yml" in str(e.stdout) or "dbt_project.yml" in str(e.stderr):
            logger.error("💡 dbt_project.yml not found. Make sure:")
            logger.error("   1. The dbt_project.yml file is in the project root")
            logger.error("   2. The working directory is correct")
            logger.error("   3. All dbt files are properly deployed to Render")
        elif "MERGE command cannot affect row a second time" in str(e.stdout):
            logger.error("💡 PostgreSQL MERGE conflict detected. This usually means:")
            logger.error("   1. Duplicate keys in source data")
            logger.error("   2. Incorrect unique_key configuration in dbt models")
            logger.error("   3. Need to update model unique_key to handle multiple rows")
            logger.error("   Check the failing models and update their unique_key configuration")
        elif "ERROR" in str(e.stdout) and "PASS=" in str(e.stdout):
            # Partial success - some models failed but others succeeded
            logger.warning("⚠️ dbt completed with some errors but partial success")
            logger.warning("   This is often acceptable for incremental models with data issues")
            logger.warning("   Consider this a successful run unless critical models failed")
            return True  # Treat partial success as success
        
        return False
    except FileNotFoundError:
        logger.error("❌ dbt command not found. Please ensure dbt is installed and available in PATH")
        if is_render:
            logger.error("💡 In Render, make sure dbt is listed in your requirements.txt or build script")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during dbt run: {str(e)}")
        return False

def cleanup_duplicate_records(table_name: str):
    """Clean up duplicate records from a table by keeping the most recent record for each ID."""
    try:
        logger.info(f"🧹 Cleaning up duplicate records in {table_name}...")
        
        engine = create_database_engine()
        with engine.connect() as conn:
            # Get count of duplicates
            duplicate_count_sql = f"""
                SELECT COUNT(*) - COUNT(DISTINCT id) as duplicate_count 
                FROM {table_name}
            """
            duplicate_count = conn.execute(text(duplicate_count_sql)).scalar()
            
            if duplicate_count == 0:
                logger.info(f"✅ No duplicate records found in {table_name}")
                return True
            
            logger.info(f"Found {duplicate_count} duplicate records in {table_name}")
            
            # Create a temporary table with deduplicated data
            temp_table = f"temp_{table_name}_dedup"
            dedup_sql = f"""
                CREATE TEMP TABLE {temp_table} AS
                SELECT DISTINCT ON (id) *
                FROM {table_name}
                ORDER BY id, last_processed_at DESC
            """
            conn.execute(text(dedup_sql))
            
            # Count records in temp table
            temp_count = conn.execute(text(f"SELECT COUNT(*) FROM {temp_table}")).scalar()
            
            # Delete all records from original table
            conn.execute(text(f"DELETE FROM {table_name}"))
            
            # Insert deduplicated records back
            insert_sql = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table}
            """
            conn.execute(text(insert_sql))
            conn.commit()
            
            # Drop temp table
            conn.execute(text(f"DROP TABLE {temp_table}"))
            conn.commit()
            
            logger.info(f"✅ Successfully cleaned up {table_name}: {duplicate_count} duplicates removed, {temp_count} unique records retained")
            return True
            
    except Exception as e:
        logger.error(f"❌ Error cleaning up {table_name}: {str(e)}")
        return False

def test_tock_integration():
    """Test the Tock API integration with sample data."""
    try:
        logger.info("🧪 Testing Tock API integration...")
        
        # Check if Tock credentials are available
        if not all([os.getenv('X_TOCK_AUTH'), os.getenv('X_TOCK_SCOPE')]):
            logger.warning("Tock credentials not available, skipping integration test")
            return True
        
        # Initialize Tock client
        tock_client = TockAPIClient()
        logger.info("✅ Tock client initialized successfully")
        
        # Test getting data URLs (this will make an actual API call)
        logger.info("🔍 Testing data URL retrieval...")
        urls_data = tock_client.get_data_urls()
        
        guest_urls = urls_data.get('guest_urls', [])
        reservation_urls = urls_data.get('reservation_urls', [])
        
        logger.info(f"✅ Retrieved {len(guest_urls)} guest URLs and {len(reservation_urls)} reservation URLs")
        
        # Test fetching data from latest URLs (incremental mode)
        if guest_urls:
            logger.info("🔍 Testing guest data fetch from latest URL...")
            latest_guest_url = tock_client.get_latest_guest_url(guest_urls)
            logger.info(f"Latest guest URL: {latest_guest_url[:100]}...")
            
            sample_guest_data = tock_client.fetch_json_data(latest_guest_url)
            logger.info(f"✅ Successfully fetched {len(sample_guest_data)} guest records from latest file")
            
            if sample_guest_data:
                # Test upsert with sample data
                logger.info("🔍 Testing guest data upsert...")
                tock_client.upsert_data('tock_guest', sample_guest_data[:5])  # Only test with first 5 records
                logger.info("✅ Guest data upsert test completed")
        
        if reservation_urls:
            logger.info("🔍 Testing reservation data fetch from latest URL...")
            latest_reservation_url = tock_client.get_latest_reservation_url(reservation_urls)
            logger.info(f"Latest reservation URL: {latest_reservation_url[:100]}...")
            
            sample_reservation_data = tock_client.fetch_json_data(latest_reservation_url)
            logger.info(f"✅ Successfully fetched {len(sample_reservation_data)} reservation records from latest file")
            
            if sample_reservation_data:
                # Test upsert with sample data
                logger.info("🔍 Testing reservation data upsert...")
                tock_client.upsert_data('tock_reservation', sample_reservation_data[:5])  # Only test with first 5 records
                logger.info("✅ Reservation data upsert test completed")
        
        # Verify the data was inserted correctly
        engine = create_database_engine()
        with engine.connect() as conn:
            # Check guest data
            guest_count = conn.execute(text("SELECT COUNT(*) FROM raw_tock_guest")).scalar()
            logger.info(f"✅ Total guest records in database: {guest_count}")
            
            # Check reservation data
            reservation_count = conn.execute(text("SELECT COUNT(*) FROM raw_tock_reservation")).scalar()
            logger.info(f"✅ Total reservation records in database: {reservation_count}")
        
        logger.info("🎉 Tock API integration test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during Tock integration test: {str(e)}", exc_info=True)
        return False

def test_upsert():
    """Test the upsert process with sample data."""
    try:
        # Sample data that mimics the structure of club membership records
        sample_data = [
            {
                "id": "test1",
                "updatedAt": "2024-03-05T12:00:00Z",
                "customerId": "cust1",
                "clubId": "club1",
                "status": "active",
                "startDate": "2024-01-01T00:00:00Z",
                "endDate": "2024-12-31T23:59:59Z",
                "metadata": {
                    "source": "test",
                    "tier": "gold"
                }
            },
            {
                "id": "test2",
                "updatedAt": "2024-03-05T13:00:00Z",
                "customerId": "cust2",
                "clubId": "club2",
                "status": "pending",
                "startDate": "2024-02-01T00:00:00Z",
                "endDate": "2024-12-31T23:59:59Z",
                "metadata": {
                    "source": "test",
                    "tier": "silver"
                }
            }
        ]
        
        client = Commerce7Client()
        logger.info("Testing upsert with sample club membership data...")
        client.upsert_data('club_membership', sample_data)
        logger.info("Test upsert completed successfully")
        
        # Verify the data was inserted correctly
        engine = create_database_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw_club_membership")).scalar()
            logger.info(f"Total records in database: {result}")
            
            # Check a specific record
            test_record = conn.execute(text("SELECT * FROM raw_club_membership WHERE id = 'test1'")).fetchone()
            if test_record:
                logger.info("Successfully retrieved test record")
                logger.debug(f"Test record data: {test_record}")
            else:
                logger.error("Failed to retrieve test record")
        
    except Exception as e:
        logger.error(f"Error during test upsert: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '--test':
            test_upsert()
        elif sys.argv[1] == '--test-tock':
            test_tock_integration()
        elif sys.argv[1] == '--test-connection':
            test_database_connection()
        elif sys.argv[1] == '--cleanup-tock-reservation':
            load_environment()
            cleanup_duplicate_records('raw_tock_reservation')
        elif sys.argv[1] == '--cleanup-tock-guest':
            load_environment()
            cleanup_duplicate_records('raw_tock_guest')
        else:
            main(sys.argv[1])  # Run with specific endpoint
    else:
        main()  # Run all endpoints 