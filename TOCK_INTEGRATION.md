# Tock API Integration

This document describes the integration of Tock API data feeds into the cronDon data warehouse.

## Overview

The Tock integration adds two new data sources to the warehouse:
- **Guest Data**: Customer profile information, preferences, dietary restrictions, and contact details
- **Reservation Data**: Booking information, party details, pricing, and transaction data

## Setup

### Environment Variables

Add the following environment variables to your `.env` file:

```bash
# Tock API Credentials
X_TOCK_AUTH=your_tock_authorization_token
X_TOCK_SCOPE=your_tock_scope_token
```

### API Endpoint

The integration connects to:
```
https://dashboard.exploretock.com/api/data/export/urls
```

## Data Flow

1. **API Authentication**: Uses `X-Tock-Authorization` and `X-Tock-Scope` headers
2. **URL Retrieval**: Fetches signed URLs for guest and reservation data exports
3. **Smart Data Fetching**: 
   - **Initial Load**: Downloads all JSON files from Google Cloud Storage
   - **Incremental Updates**: Downloads only the latest files (highest numbered)
4. **Data Processing**: Parses and stores data in PostgreSQL with JSONB columns
5. **dbt Transformation**: Creates staging models for analysis

### Incremental Loading Strategy

The integration automatically detects whether this is an initial load or incremental update:

- **Initial Load**: When no data exists in the tables, fetches ALL files
- **Incremental Updates**: When data already exists, fetches only the latest files:
  - Guest data: Only `guest-profile-X.json` with the highest X number
  - Reservation data: Only `reservation-X.json` with the highest X number

This approach minimizes data transfer and processing time for regular updates while ensuring complete data coverage on first run.

## Usage

### Running the Integration

```bash
# Run all data sources (Commerce7 + Tock)
python ingest.py

# Run only Tock guest data
python ingest.py tock-guest

# Run only Tock reservation data
python ingest.py tock-reservation

# Test Tock integration
python ingest.py --test-tock
```

### Database Tables

#### Raw Tables
- `raw_tock_guest`: Raw guest data from Tock API
- `raw_tock_reservation`: Raw reservation data from Tock API

#### Staging Tables
- `stg_tock_guest`: Parsed and cleaned guest data
- `stg_tock_reservation`: Parsed and cleaned reservation data

## Data Schema

### Guest Data Structure

The guest data includes:
- **Patron Information**: Name, email, phone, address
- **Preferences**: Dietary restrictions, hospitality preferences
- **Profile Data**: Birthdays, anniversaries, social links
- **Business Metadata**: Group ID, verification status, opt-in source
- **External IDs**: Commerce7 contact IDs and other system references

### Reservation Data Structure

The reservation data includes:
- **Booking Details**: Date/time, party size, confirmation code
- **Business Information**: Venue details, timezone, currency
- **Experience Information**: Service type, pricing
- **Customer Information**: Owner and diner patron details
- **Financial Data**: Pricing, taxes, fees, payments
- **Status Tracking**: Cancellation, transfer, version control

## Key Features

### Incremental Loading
- **Smart File Selection**: Automatically detects initial vs incremental runs
- **Initial Load**: Downloads all available files for complete data coverage
- **Incremental Updates**: Downloads only the latest files (highest numbered)
- Uses `last_processed_at` timestamps for efficient data updates
- Handles duplicate records with PostgreSQL `ON CONFLICT` upserts

### Error Handling
- Retry logic with exponential backoff for API calls
- Comprehensive logging for debugging and monitoring
- Graceful handling of missing or malformed data

### Data Quality
- Validates JSON structure and required fields
- Converts timestamps from milliseconds to proper datetime format
- Handles array data with proper counting and extraction

## Monitoring

### Logging
The integration provides detailed logging including:
- API request/response details
- Data processing statistics
- Error messages with context
- Performance metrics

### Testing
Use the built-in test function to validate the integration:
```bash
python ingest.py --test-tock
```

This will:
1. Test API connectivity
2. Fetch sample data
3. Validate database operations
4. Report success/failure status

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify `X_TOCK_AUTH` and `X_TOCK_SCOPE` are correct
   - Check token expiration and permissions

2. **Network Timeouts**
   - Large JSON files may take time to download
   - Check internet connectivity and firewall settings

3. **Database Errors**
   - Ensure PostgreSQL connection is working
   - Check table permissions and schema

4. **Data Parsing Issues**
   - JSON structure changes may require model updates
   - Check logs for specific parsing errors

### Debug Mode
Enable debug logging by setting the log level in `ingest.py`:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Integration with Existing System

The Tock integration seamlessly works alongside the existing Commerce7 integration:
- Shared database connection and error handling
- Consistent data model patterns
- Unified dbt transformation pipeline
- Same deployment and monitoring infrastructure

## Future Enhancements

Potential improvements include:
- Real-time data streaming
- Advanced data quality checks
- Automated schema evolution
- Enhanced error recovery mechanisms
- Performance optimization for large datasets
