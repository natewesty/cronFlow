#!/usr/bin/env python3
"""
dbt Transformation Runner - Production Version

This script runs dbt transformations to convert raw data into warehouse tables.
It's designed to run as a CRON job on Render after data ingestion.
"""

import os
import sys
import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

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

def run_command(command: str, description: str) -> bool:
    """Run a shell command with proper logging and error handling."""
    logger.info(f"Running: {description}")
    logger.info(f"Command: {command}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"✅ {description} completed successfully")
        if result.stdout:
            logger.debug(f"Output: {result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed with exit code {e.returncode}")
        if e.stdout:
            logger.error(f"stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"❌ {description} failed with exception: {e}")
        return False

def check_dbt_installation():
    """Check if dbt is properly installed."""
    return run_command("dbt --version", "Checking dbt installation")

def run_dbt_debug():
    """Run dbt debug to check configuration."""
    return run_command("dbt debug", "Running dbt debug")

def run_dbt_deps():
    """Install dbt dependencies."""
    return run_command("dbt deps", "Installing dbt dependencies")

def run_dbt_run():
    """Run dbt models."""
    return run_command("dbt run", "Running dbt models")

def run_dbt_test():
    """Run dbt tests."""
    return run_command("dbt test", "Running dbt tests")

def run_dbt_docs_generate():
    """Generate dbt documentation."""
    return run_command("dbt docs generate", "Generating dbt documentation")

def main():
    """Main function for dbt transformation pipeline."""
    start_time = datetime.now()
    logger.info("Starting dbt transformation pipeline")
    
    # Validate environment
    required_vars = ['DATABASE_URL'] if os.getenv('DATABASE_URL') else ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Check if we're in the right directory
    if not Path("dbt_project.yml").exists():
        logger.error("dbt_project.yml not found. Please run this script from the project root.")
        sys.exit(1)
    
    # Pipeline steps
    steps = [
        ("check_dbt_installation", check_dbt_installation),
        ("run_dbt_debug", run_dbt_debug),
        ("run_dbt_deps", run_dbt_deps),
        ("run_dbt_run", run_dbt_run),
        ("run_dbt_test", run_dbt_test),
        ("run_dbt_docs_generate", run_dbt_docs_generate)
    ]
    
    success_count = 0
    total_steps = len(steps)
    
    for step_name, step_func in steps:
        try:
            if step_func():
                success_count += 1
            else:
                logger.error(f"Step '{step_name}' failed")
                # Continue with other steps even if one fails
        except Exception as e:
            logger.error(f"Unexpected error in step '{step_name}': {e}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"dbt transformation pipeline completed in {duration}")
    logger.info(f"Successfully completed {success_count}/{total_steps} steps")
    
    # Exit with appropriate code
    if success_count == total_steps:
        logger.info("All dbt transformations completed successfully")
        sys.exit(0)
    else:
        logger.error(f"Some dbt transformations failed ({success_count}/{total_steps})")
        sys.exit(1)

if __name__ == "__main__":
    main() 