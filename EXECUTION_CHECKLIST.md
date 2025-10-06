# KPI Dashboard Execution Checklist

Use this checklist to ensure a smooth implementation.

## 📋 Pre-Flight Checks

- [ ] **Verify agg_daily_revenue exists and has current data**
  ```sql
  SELECT 
      COUNT(*) as total_days,
      MIN(date_day) as earliest_date,
      MAX(date_day) as latest_date
  FROM agg_daily_revenue;
  ```
  Expected: Latest date should be recent (today or yesterday)

- [ ] **Confirm nate_sandbox schema exists**
  ```sql
  SELECT schema_name 
  FROM information_schema.schemata 
  WHERE schema_name = 'nate_sandbox';
  ```
  If not exists: `CREATE SCHEMA nate_sandbox;`

- [ ] **Check dbt connection**
  ```bash
  dbt debug
  ```
  Expected: "All checks passed!"

## 🚀 Installation Steps

### Step 1: Install Dependencies
```bash
dbt deps
```
**Verification:**
- [ ] File created: `dbt_packages/dbt_utils/`
- [ ] No errors in output

### Step 2: Load Seed Data
```bash
dbt seed
```
**Verification:**
- [ ] `nate_sandbox.dim_kpi` created (27 rows)
- [ ] `nate_sandbox.dim_entity` created (1 row)
- [ ] Query to verify:
  ```sql
  SELECT COUNT(*) FROM nate_sandbox.dim_kpi;  -- Should return 27
  SELECT COUNT(*) FROM nate_sandbox.dim_entity;  -- Should return 1
  ```

### Step 3: Build Date Dimension
```bash
dbt run --select dim_date
```
**Verification:**
- [ ] `nate_sandbox.dim_date` created
- [ ] Query to verify:
  ```sql
  SELECT 
      COUNT(*) as total_dates,
      MIN(date_key) as first_date,
      MAX(date_key) as last_date
  FROM nate_sandbox.dim_date;
  ```
  Expected: ~7,670 dates (2015-01-01 to 2035-12-31)

### Step 4: Build KPI Facts
```bash
dbt run --select fact_kpi_daily
```
**Verification:**
- [ ] `nate_sandbox.fact_kpi_daily` created
- [ ] Query to verify:
  ```sql
  SELECT 
      COUNT(*) as total_rows,
      COUNT(DISTINCT date_key) as unique_dates,
      COUNT(DISTINCT kpi_id) as unique_kpis,
      MIN(date_key) as earliest,
      MAX(date_key) as latest
  FROM nate_sandbox.fact_kpi_daily;
  ```
  Expected: 27 KPIs × number of days in agg_daily_revenue

### Step 5: Build KPI Dashboard
```bash
dbt run --select agg_kpi_dashboard
```
**Verification:**
- [ ] `nate_sandbox.agg_kpi_dashboard` created
- [ ] Query to verify:
  ```sql
  SELECT 
      COUNT(*) as total_rows,
      COUNT(DISTINCT as_of_date) as unique_dates,
      COUNT(DISTINCT kpi_id) as unique_kpis,
      MIN(as_of_date) as earliest,
      MAX(as_of_date) as latest
  FROM nate_sandbox.agg_kpi_dashboard;
  ```
  Expected: 27 rows (for today only, based on backfill_days: 1)

### Step 6: Run Tests
```bash
dbt test --select kpi.*
```
**Verification:**
- [ ] All tests pass
- [ ] No warnings in output
- [ ] Expected tests:
  - dim_date uniqueness
  - fact_kpi_daily uniqueness
  - agg_kpi_dashboard uniqueness
  - dim_kpi accepted values
  - Not null constraints

## ✅ Validation Queries

### Validation 1: Check Today's Dashboard Data
```sql
SELECT 
    dk.kpi_code,
    dk.kpi_name,
    t.mtd_value,
    t.ytd_value,
    CASE 
        WHEN t.mtd_delta_pct IS NOT NULL 
        THEN ROUND(t.mtd_delta_pct * 100, 1) || '%'
        ELSE 'N/A'
    END as mtd_yoy
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = CURRENT_DATE
ORDER BY dk.kpi_id
LIMIT 5;
```
**Expected:** 5 rows with data for today's KPIs

- [ ] Query returns 5 rows
- [ ] mtd_value has non-zero values
- [ ] ytd_value has non-zero values
- [ ] mtd_yoy shows percentage or 'N/A' (if no prior year data)

### Validation 2: Check for YoY Data
```sql
SELECT 
    COUNT(*) as total_kpis,
    COUNT(CASE WHEN mtd_prior IS NOT NULL THEN 1 END) as has_mtd_prior,
    COUNT(CASE WHEN ytd_prior IS NOT NULL THEN 1 END) as has_ytd_prior
FROM nate_sandbox.agg_kpi_dashboard
WHERE as_of_date = CURRENT_DATE;
```
**Expected:** 
- [ ] 27 total KPIs
- [ ] If you have data from last year, prior values should be > 0

### Validation 3: Verify fact_kpi_daily Unpivot
```sql
-- Check a specific day's revenue total
WITH source AS (
    SELECT 
        date_day,
        total_daily_revenue 
    FROM agg_daily_revenue 
    WHERE date_day = CURRENT_DATE
),
unpivoted AS (
    SELECT 
        date_key,
        value
    FROM nate_sandbox.fact_kpi_daily
    WHERE date_key = CURRENT_DATE
        AND kpi_id = 14  -- total_daily_revenue
)
SELECT 
    s.total_daily_revenue as source_value,
    u.value as unpivoted_value,
    s.total_daily_revenue - u.value as difference
FROM source s
JOIN unpivoted u ON s.date_day = u.date_key;
```
**Expected:** difference should be 0 (or very small due to floating point)

- [ ] Query returns 1 row
- [ ] Difference is 0 or near 0

### Validation 4: Verify JSON Payload
```sql
SELECT 
    dk.kpi_code,
    t.payload::jsonb -> 'mtd' ->> 'v' as mtd_value,
    t.payload::jsonb -> 'ytd' ->> 'v' as ytd_value
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = CURRENT_DATE
    AND dk.kpi_code = 'total_daily_revenue';
```
**Expected:** 1 row with valid JSON values

- [ ] Query returns 1 row
- [ ] mtd_value and ytd_value are valid numbers

## 🔧 Performance Optimization

### Create Indexes (After Initial Build)
```sql
-- Index for fact_kpi_daily queries
CREATE INDEX IF NOT EXISTS ix_fact_kpi_daily_kpi_date 
ON nate_sandbox.fact_kpi_daily (kpi_id, date_key);

-- Index for dashboard API queries
CREATE INDEX IF NOT EXISTS ix_agg_kpi_dashboard_fetch 
ON nate_sandbox.agg_kpi_dashboard (as_of_date, kpi_id, entity_id);

-- Index for time series queries
CREATE INDEX IF NOT EXISTS ix_agg_kpi_dashboard_kpi_date
ON nate_sandbox.agg_kpi_dashboard (kpi_id, as_of_date DESC);
```
**Verification:**
```sql
SELECT 
    schemaname,
    tablename,
    indexname
FROM pg_indexes
WHERE schemaname = 'nate_sandbox'
ORDER BY tablename, indexname;
```

- [ ] All 3 indexes created
- [ ] No errors in creation

## 📊 Optional: Historical Backfill

If you want historical snapshots (e.g., last 60 days):

### Backfill Steps
1. [ ] **Update dbt_project.yml**
   ```yaml
   vars:
     kpi_dashboard_backfill_days: 60  # Change from 1 to 60
   ```

2. [ ] **Run backfill**
   ```bash
   dbt run --select agg_kpi_dashboard
   ```

3. [ ] **Verify backfill**
   ```sql
   SELECT 
       COUNT(DISTINCT as_of_date) as unique_dates,
       MIN(as_of_date) as earliest,
       MAX(as_of_date) as latest
   FROM nate_sandbox.agg_kpi_dashboard;
   ```
   Expected: 60 unique dates

4. [ ] **Revert dbt_project.yml**
   ```yaml
   vars:
     kpi_dashboard_backfill_days: 1  # Change back to 1
   ```

## 🔄 CRON Job Integration

### Add to Your Existing CRON Job

```bash
#!/bin/bash
# Your existing cron job (example)

# Step 1: Run your data ingestion
python ingest.py

# Step 2: Build marts (including agg_daily_revenue)
dbt run --select marts.*

# Step 3: Build KPI system ← ADD THIS!
dbt run --select fact_kpi_daily agg_kpi_dashboard

# Step 4: Run tests ← ADD THIS!
dbt test --select kpi.*

# Step 5: Any downstream processes
# ...
```

**Verification after first CRON run:**
- [ ] Check logs for errors
- [ ] Verify new date in dashboard:
  ```sql
  SELECT MAX(as_of_date) FROM nate_sandbox.agg_kpi_dashboard;
  ```
- [ ] Should show the current date

## 🐛 Troubleshooting

### Issue: "relation does not exist"
**Symptoms:** Error when running agg_kpi_dashboard
**Solution:**
- [ ] Verify fact_kpi_daily exists: `\dt nate_sandbox.fact_kpi_daily`
- [ ] If not, run: `dbt run --select fact_kpi_daily`

### Issue: "No rows returned"
**Symptoms:** agg_kpi_dashboard is empty
**Solution:**
- [ ] Check agg_daily_revenue has data: `SELECT COUNT(*) FROM agg_daily_revenue;`
- [ ] Check fact_kpi_daily has data: `SELECT COUNT(*) FROM nate_sandbox.fact_kpi_daily;`
- [ ] Rebuild: `dbt run --select fact_kpi_daily agg_kpi_dashboard --full-refresh`

### Issue: "Tests failing"
**Symptoms:** dbt test returns errors
**Solution:**
- [ ] Check which test failed: Look at error message
- [ ] Common: dbt_utils not installed → Run: `dbt deps`
- [ ] Common: Duplicate rows → Check uniqueness in fact_kpi_daily

### Issue: "Slow performance"
**Symptoms:** Queries taking >1 second
**Solution:**
- [ ] Create indexes (see Performance Optimization section)
- [ ] Run ANALYZE: `ANALYZE nate_sandbox.agg_kpi_dashboard;`
- [ ] Check row counts: Should be ~27 rows per day

## 📝 Documentation

### Generate dbt Docs
```bash
dbt docs generate
dbt docs serve
```
**Verification:**
- [ ] Browser opens with documentation
- [ ] Can navigate to KPI models
- [ ] Lineage graph shows dependencies

### Update Project README
- [ ] Document the KPI system in your main README
- [ ] Link to IMPLEMENTATION_GUIDE.md
- [ ] Add example queries

## ✨ Success Criteria

Your implementation is complete when:

- [ ] All models build successfully
- [ ] All tests pass
- [ ] Today's data is visible in agg_kpi_dashboard (27 rows)
- [ ] Validation queries return expected results
- [ ] Indexes are created
- [ ] CRON job is updated
- [ ] Documentation is generated

## 🎉 Final Verification

Run this comprehensive check:

```sql
WITH system_status AS (
    SELECT 
        'dim_date' as model,
        COUNT(*)::text as row_count,
        'Date dimension' as description
    FROM nate_sandbox.dim_date
    
    UNION ALL
    
    SELECT 
        'dim_kpi',
        COUNT(*)::text,
        'KPI definitions'
    FROM nate_sandbox.dim_kpi
    
    UNION ALL
    
    SELECT 
        'dim_entity',
        COUNT(*)::text,
        'Entity definitions'
    FROM nate_sandbox.dim_entity
    
    UNION ALL
    
    SELECT 
        'fact_kpi_daily',
        COUNT(*)::text,
        'Daily KPI facts'
    FROM nate_sandbox.fact_kpi_daily
    
    UNION ALL
    
    SELECT 
        'agg_kpi_dashboard',
        COUNT(*)::text,
        'Dashboard snapshots'
    FROM nate_sandbox.agg_kpi_dashboard
    
    UNION ALL
    
    SELECT 
        'Today''s KPIs',
        COUNT(*)::text,
        'Should be 27'
    FROM nate_sandbox.agg_kpi_dashboard
    WHERE as_of_date = CURRENT_DATE
)
SELECT * FROM system_status;
```

**Expected Output:**
```
model                 | row_count | description
---------------------+-----------+---------------------
dim_date             | 7670      | Date dimension
dim_kpi              | 27        | KPI definitions
dim_entity           | 1         | Entity definitions
fact_kpi_daily       | 9855      | Daily KPI facts (varies)
agg_kpi_dashboard    | 27        | Dashboard snapshots (varies)
Today's KPIs         | 27        | Should be 27
```

- [ ] All models have rows
- [ ] Today's KPIs = 27

---

## 🎯 Next Steps After Completion

1. [ ] Build API endpoints to query agg_kpi_dashboard
2. [ ] Create frontend dashboard components
3. [ ] Set up alerts for key metric thresholds
4. [ ] Plan expansion to entity-level breakdowns
5. [ ] Consider adding forecasting models

**Congratulations! Your KPI Dashboard is live! 🚀**

