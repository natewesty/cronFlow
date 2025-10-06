# KPI Dashboard - nate_sandbox Schema

This KPI dashboard infrastructure is built entirely in the `nate_sandbox` schema and does not touch the operational `public` schema.

## Directory Structure

```
models/
  kpi/
    dim_date.sql              # Date dimension table
    agg_kpi_dashboard.sql     # Main incremental KPI aggregation table
    schema.yml                # Model and source definitions with tests
    README.md                 # This file
  seeds/
    dim_kpi.csv              # KPI dimension seed (example)
    dim_entity.csv           # Entity dimension seed (example)
macros/
  kpi_date_utils.sql         # Utility macro for date window calculations
packages.yml                 # dbt packages (includes dbt_utils)
dbt_project.yml              # Updated with KPI configuration
```

## Prerequisites

Before running the models, you need to create the source table in the `nate_sandbox` schema:

### Required Source Table: `fact_kpi_daily`

```sql
CREATE TABLE nate_sandbox.fact_kpi_daily (
    kpi_id INTEGER NOT NULL,
    entity_id INTEGER,
    date_key DATE NOT NULL,
    value NUMERIC NOT NULL
);

-- Recommended index for performance
CREATE INDEX ix_fact_kpi_daily_kpi_date 
ON nate_sandbox.fact_kpi_daily (kpi_id, date_key);
```

## Implementation Steps

### 1. Install Dependencies

```bash
dbt deps
```

This installs the `dbt_utils` package required for testing.

### 2. Load Seed Data

```bash
dbt seed
```

This loads the example `dim_kpi` and `dim_entity` CSV files into the `nate_sandbox` schema.

### 3. Build the Date Dimension

```bash
dbt run --select dim_date
```

This creates the `nate_sandbox.dim_date` table covering 2015-2035 (configurable in `dbt_project.yml`).

### 4. Build the KPI Dashboard

```bash
dbt run --select agg_kpi_dashboard
```

This creates the `nate_sandbox.agg_kpi_dashboard` table. On first run, it will be empty or only contain data for today (depending on your `fact_kpi_daily` data).

### 5. Optional: Historical Backfill

To generate historical snapshots (e.g., last 60 days), temporarily update `dbt_project.yml`:

```yaml
vars:
  kpi_dashboard_backfill_days: 60  # Change from 1 to 60
```

Then run:

```bash
dbt run --select agg_kpi_dashboard
```

After backfill completes, revert `kpi_dashboard_backfill_days` back to 1 for daily runs.

### 6. Run Tests

```bash
dbt test --select kpi.*
```

## Daily Scheduling

For ongoing operations, keep `kpi_dashboard_backfill_days: 1` and schedule:

```bash
# Run the model to update today's metrics
dbt run --select agg_kpi_dashboard

# Run tests to validate data quality
dbt test --select kpi.*
```

## Schema Configuration

All models and seeds are configured to use the `nate_sandbox` schema:

- **Models**: `models/kpi/*.sql` → `nate_sandbox.{model_name}`
- **Seeds**: `models/seeds/*.csv` → `nate_sandbox.{seed_name}`
- **Sources**: Referenced from `nate_sandbox.fact_kpi_daily`

## Data Model

### Input (fact_kpi_daily)
- **kpi_id**: Integer identifier for the KPI metric
- **entity_id**: Integer identifier for the entity (use 0 for "ALL" or global metrics)
- **date_key**: Date of the metric value
- **value**: The KPI value for that date

### Output (agg_kpi_dashboard)
The dashboard aggregates metrics into these time windows:
- **MTD**: Month-to-date
- **QTD**: Quarter-to-date
- **YTD**: Year-to-date
- **Last28**: Rolling 28-day window

For each window, it provides:
- **value**: Current period value
- **prior**: Same window from prior year
- **delta**: Difference between current and prior
- **delta_pct**: Percentage change

Plus a **payload** JSONB column with all metrics in compact format.

## Performance Optimization

### Recommended Indexes

```sql
-- On fact table (already mentioned above)
CREATE INDEX ix_fact_kpi_daily_kpi_date 
ON nate_sandbox.fact_kpi_daily (kpi_id, date_key);

-- On dashboard table for API queries
CREATE INDEX ix_agg_kpi_dashboard_fetch 
ON nate_sandbox.agg_kpi_dashboard (as_of_date, kpi_id, entity_id);
```

## Frontend Consumption

Example query to fetch today's snapshot:

```sql
SELECT 
    dk.kpi_code, 
    dk.kpi_name, 
    dk.format, 
    dk.target_direction, 
    t.payload
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = current_date
  AND COALESCE(t.entity_id, 0) = 0;
```

Access metrics from the payload:
- `payload->'mtd'->>'v'` = MTD value
- `payload->'mtd'->>'p'` = MTD prior year
- `payload->'mtd'->>'d'` = MTD delta
- `payload->'mtd'->>'dp'` = MTD delta percent

## Configuration Variables

In `dbt_project.yml`:

```yaml
vars:
  dim_date_start: '2015-01-01'                # Start date for dim_date
  dim_date_end: '2035-12-31'                  # End date for dim_date
  kpi_dashboard_lookback_days: 400            # Days to look back for YoY calculations
  kpi_dashboard_backfill_days: 1              # Number of as_of_dates to recompute per run
```

## Notes

- The `nate_sandbox` schema must exist before running these models
- All source data should be in `nate_sandbox.fact_kpi_daily`
- The public schema remains completely untouched
- You can reference public schema data to populate `fact_kpi_daily`, but all KPI models live in `nate_sandbox`
