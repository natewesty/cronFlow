# KPI Dashboard Setup Guide

## Overview

This KPI dashboard system has been implemented in the `nate_sandbox` schema as requested. All files have been created and configured to operate independently from the operational public schema.

## File Structure

```
models/
  kpi/
    dim_date.sql              ✓ Created - Date dimension for KPI calculations
    agg_kpi_dashboard.sql     ✓ Created - Main KPI dashboard aggregation
    schema.yml                ✓ Created - Schema definitions and tests
  seeds/
    dim_kpi.csv               ✓ Created - KPI dimension seed data
    dim_entity.csv            ✓ Created - Entity dimension seed data
macros/
  kpi_date_utils.sql          ✓ Created - Utility macros for date calculations
dbt_project.yml               ✓ Updated - Added KPI configuration for nate_sandbox
packages.yml                  ✓ Created - dbt-utils dependency
```

## Prerequisites

Before running the KPI dashboard, you need to create the `fact_kpi_daily` table in the `nate_sandbox` schema with the following structure:

```sql
CREATE TABLE nate_sandbox.fact_kpi_daily (
    kpi_id INTEGER NOT NULL,
    entity_id INTEGER,
    date_key DATE NOT NULL,
    value NUMERIC NOT NULL
);

CREATE INDEX ix_fact_kpi_daily_kpi_date ON nate_sandbox.fact_kpi_daily (kpi_id, date_key);
```

## Implementation Steps

### 1. Install dbt-utils Package

```bash
dbt deps
```

This will install the dbt-utils package required for the unique combination tests.

### 2. Load Seed Data

```bash
dbt seed
```

This loads the `dim_kpi` and `dim_entity` seed files into `nate_sandbox` schema.

### 3. Build the Date Dimension

```bash
dbt run --select dim_date
```

This creates the date dimension table in `nate_sandbox` covering 2015-2035.

### 4. Build the KPI Dashboard

```bash
dbt run --select agg_kpi_dashboard
```

This creates the incremental KPI dashboard table. On the first run, it will create the table structure and compute metrics for today.

### 5. Run Tests

```bash
dbt test --select kpi.*
```

This validates the data quality with the defined tests.

## Optional: Backfill Historical Data

To generate historical KPI snapshots (e.g., for the last 60 days), temporarily update `dbt_project.yml`:

```yaml
vars:
  kpi_dashboard_backfill_days: 60  # Change from 1 to desired number of days
```

Then run:

```bash
dbt run --select agg_kpi_dashboard
```

After backfill completes, revert the setting back to 1 for daily operations.

## Configuration Variables

The following variables can be adjusted in `dbt_project.yml`:

- `dim_date_start`: Start date for date dimension (default: '2015-01-01')
- `dim_date_end`: End date for date dimension (default: '2035-12-31')
- `kpi_dashboard_lookback_days`: Days to look back for YoY calculations (default: 400)
- `kpi_dashboard_backfill_days`: Number of as_of_dates to recalculate each run (default: 1)

## Schema Configuration

All KPI models are configured to materialize in the `nate_sandbox` schema:

- **dim_date**: Materialized as table in `nate_sandbox`
- **agg_kpi_dashboard**: Materialized as incremental table in `nate_sandbox` with merge strategy
- **Seeds**: Loaded into `nate_sandbox`

## Daily Scheduling

For production use, schedule the following:

```bash
dbt run --select agg_kpi_dashboard
dbt test --select kpi.*
```

With `kpi_dashboard_backfill_days: 1`, this will recalculate today's metrics each run.

## Performance Optimization

### Recommended Indexes

```sql
-- On fact_kpi_daily (if not already created)
CREATE INDEX IF NOT EXISTS ix_fact_kpi_daily_kpi_date 
  ON nate_sandbox.fact_kpi_daily (kpi_id, date_key);

-- On agg_kpi_dashboard (after initial load)
CREATE INDEX IF NOT EXISTS ix_agg_kpi_dashboard_fetch 
  ON nate_sandbox.agg_kpi_dashboard (as_of_date, kpi_id, entity_id);
```

### Optional: Partitioning

For very large datasets, consider partitioning `agg_kpi_dashboard` by month:

```sql
-- Example partitioning setup (run after initial build)
-- This is optional and depends on your data volume
```

## API Query Example

To fetch today's KPI snapshot for frontend consumption:

```sql
SELECT 
    dk.kpi_code, 
    dk.kpi_name, 
    dk.format, 
    dk.target_direction, 
    t.payload
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = CURRENT_DATE
  AND COALESCE(t.entity_id, 0) = 0;
```

The frontend can then parse the JSON payload:
- `payload->'mtd'->>'v'` - MTD value
- `payload->'mtd'->>'p'` - MTD prior year value
- `payload->'mtd'->>'d'` - MTD delta
- `payload->'mtd'->>'dp'` - MTD delta percentage

## Notes

- The `fact_kpi_daily` table must be created and populated before running the dashboard
- All calculations use aligned prior periods (same calendar window one year ago)
- The incremental merge strategy only recomputes the specified backfill window, making daily runs efficient
- Null entity_id is allowed for global/company-wide KPIs
- Use entity_id = 0 if you prefer non-null entity values

## Troubleshooting

If you encounter issues:

1. **Source not found**: Ensure `fact_kpi_daily` exists in `nate_sandbox` schema
2. **dbt_utils not found**: Run `dbt deps` to install packages
3. **Merge not supported**: The incremental strategy uses Postgres-compatible merge emulation
4. **Slow performance**: Check that indexes are created on `fact_kpi_daily`

## Next Steps

1. Create and populate `nate_sandbox.fact_kpi_daily` with your KPI data
2. Update `dim_kpi.csv` seed with your actual KPIs
3. Update `dim_entity.csv` seed with your entities (if using entity-level KPIs)
4. Run the implementation steps above
5. Schedule daily runs for ongoing updates
