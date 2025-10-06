# What's Next For You - Step-by-Step Guide

Hey Nathan! 👋 Here's exactly what you need to do next with your KPI dashboard system.

## 🎯 The Big Picture

You've built `agg_daily_revenue` which calculates all your metrics daily. Now we've created a system that:
1. **Unpivots** that wide table into a long KPI format
2. **Calculates** MTD/QTD/YTD/Last28 metrics with automatic YoY comparisons
3. **Stores** everything in `nate_sandbox` (your public schema is safe!)
4. **Updates incrementally** - only recomputes recent days, not entire history

## 📋 Your Immediate Action Items

### Step 1: Commit & Push to GitHub (Do This First!)

Before testing, save your work:

```bash
# In your terminal (PowerShell)
cd C:\Users\Nathan.TDE-028\bin\cronDon

# Check what changed
git status

# Add all new KPI files
git add models/kpi/
git add models/seeds/
git add macros/
git add packages.yml
git add dbt_project.yml
git add *.md

# Commit
git commit -m "Add KPI dashboard system in nate_sandbox schema

- Created fact_kpi_daily to unpivot agg_daily_revenue
- Updated dim_kpi with all 27 KPIs
- Configured agg_kpi_dashboard for incremental MTD/QTD/YTD
- All KPI models target nate_sandbox schema
- Added comprehensive documentation"

# Push to GitHub
git push origin main
```

### Step 2: Test Locally (Before CRON)

Now let's build and test everything:

```bash
# 1. Install the dbt-utils package
dbt deps

# 2. Load your KPI definitions
dbt seed

# 3. Build the date dimension
dbt run --select dim_date

# 4. Make sure agg_daily_revenue is built (if not already)
dbt run --select agg_daily_revenue

# 5. Build the KPI fact table
dbt run --select fact_kpi_daily

# 6. Build the dashboard
dbt run --select agg_kpi_dashboard

# 7. Run all tests
dbt test --select kpi.*
```

**Expected Output:**
- Each command should succeed with no errors
- Tests should all pass (you'll see green checkmarks or "PASS")

### Step 3: Validate with SQL

Open your database client and run these queries:

#### Query 1: Check that everything built
```sql
-- Should show all your KPI tables
SELECT 
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'nate_sandbox'
ORDER BY tablename;
```

**Expected:**
- dim_date: ~7,670 rows
- dim_entity: 1 row
- dim_kpi: 27 rows
- fact_kpi_daily: (27 × number of days in agg_daily_revenue)
- agg_kpi_dashboard: 27 rows (just today, initially)

#### Query 2: See today's dashboard
```sql
SELECT 
    dk.kpi_name,
    t.mtd_value,
    ROUND(t.mtd_delta_pct * 100, 1) as mtd_yoy_pct,
    t.ytd_value,
    ROUND(t.ytd_delta_pct * 100, 1) as ytd_yoy_pct
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = CURRENT_DATE
    AND dk.kpi_code IN (
        'total_daily_revenue',
        'tasting_room_total_revenue',
        'wine_club_total_revenue',
        'new_member_acquisition'
    )
ORDER BY dk.kpi_id;
```

**Expected:** 4 rows showing your key metrics with YoY comparisons

### Step 4: Add Performance Indexes

These will make your queries fast:

```sql
-- Index for KPI fact lookups
CREATE INDEX IF NOT EXISTS ix_fact_kpi_daily_kpi_date 
ON nate_sandbox.fact_kpi_daily (kpi_id, date_key);

-- Index for dashboard queries
CREATE INDEX IF NOT EXISTS ix_agg_kpi_dashboard_fetch 
ON nate_sandbox.agg_kpi_dashboard (as_of_date, kpi_id, entity_id);

-- Index for time series
CREATE INDEX IF NOT EXISTS ix_agg_kpi_dashboard_kpi_date
ON nate_sandbox.agg_kpi_dashboard (kpi_id, as_of_date DESC);
```

### Step 5: Update Your CRON Job

You mentioned you have a CRON job that builds your data warehouse. Update it to include the KPI system.

**Find your current CRON job script** (it probably runs `dbt run` already)

**Add these lines after `agg_daily_revenue` builds:**

```bash
# Build KPI system (in nate_sandbox)
dbt run --select fact_kpi_daily agg_kpi_dashboard

# Test KPI data quality
dbt test --select kpi.*
```

**Full example of what your CRON job might look like:**

```bash
#!/bin/bash
cd /path/to/cronDon

# Ingest data
python ingest.py

# Build staging models
dbt run --select staging.*

# Build marts (including agg_daily_revenue)
dbt run --select marts.*

# Build KPI system ← NEW!
dbt run --select fact_kpi_daily agg_kpi_dashboard

# Run tests
dbt test --select marts.*
dbt test --select kpi.*  ← NEW!

# Log completion
echo "Data warehouse refresh complete: $(date)"
```

### Step 6: (Optional) Backfill Historical Data

If you want to see historical trends (last 60 days of snapshots):

1. **Edit `dbt_project.yml`** - Change line 44:
   ```yaml
   kpi_dashboard_backfill_days: 60  # Change from 1 to 60
   ```

2. **Run the backfill:**
   ```bash
   dbt run --select agg_kpi_dashboard
   ```
   This will take 10-15 seconds

3. **Verify:**
   ```sql
   SELECT 
       COUNT(DISTINCT as_of_date) as days_computed,
       MIN(as_of_date) as earliest,
       MAX(as_of_date) as latest
   FROM nate_sandbox.agg_kpi_dashboard;
   ```
   Should show 60 days

4. **Change back to daily mode** - Edit `dbt_project.yml` line 44:
   ```yaml
   kpi_dashboard_backfill_days: 1  # Change back to 1
   ```

## 🎨 What You Can Build With This

### API Endpoint Example

If you're building an API (FastAPI, Flask, etc.):

```python
# Example FastAPI endpoint
@app.get("/api/dashboard/today")
async def get_today_dashboard():
    query = """
        SELECT 
            dk.kpi_code,
            dk.kpi_name,
            dk.format,
            dk.target_direction,
            t.payload
        FROM nate_sandbox.agg_kpi_dashboard t
        JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
        WHERE t.as_of_date = CURRENT_DATE
        ORDER BY dk.kpi_id
    """
    results = await db.fetch_all(query)
    return [
        {
            "code": r["kpi_code"],
            "name": r["kpi_name"],
            "format": r["format"],
            "direction": r["target_direction"],
            "metrics": r["payload"]
        }
        for r in results
    ]
```

### Frontend Dashboard Example

The JSON payload is designed for easy frontend consumption:

```javascript
// React/Vue/Svelte component
const kpiData = await fetch('/api/dashboard/today');

kpiData.forEach(kpi => {
  const mtd = kpi.metrics.mtd;
  
  // Display the metric
  displayKPI({
    name: kpi.name,
    value: formatCurrency(mtd.v),        // Current MTD value
    prior: formatCurrency(mtd.p),        // Prior year MTD
    delta: mtd.d,                         // Absolute change
    deltaPercent: (mtd.dp * 100).toFixed(1) + '%',  // Percentage change
    isGood: (mtd.d > 0 && kpi.direction === 'up') || 
            (mtd.d < 0 && kpi.direction === 'down')
  });
});
```

## 📊 Understanding Your Data

### What Each Table Does

1. **dim_kpi** (seed)
   - Your 27 KPI definitions
   - Includes formatting hints (currency, percent, number)
   - Shows desired direction (up is good vs down is good)

2. **dim_entity** (seed)
   - Currently just "ALL" (everything combined)
   - Future: Can add locations, channels, segments

3. **dim_date**
   - Date dimension with fiscal periods
   - Used for window calculations

4. **fact_kpi_daily** (NEW!)
   - Unpivots `agg_daily_revenue` from wide to long format
   - 27 rows per day (one per KPI)
   - This is your "single source of truth" for daily KPI values

5. **agg_kpi_dashboard**
   - Rolls up `fact_kpi_daily` into time periods
   - Calculates MTD/QTD/YTD/Last28
   - Computes YoY comparisons automatically
   - Updates incrementally (only recomputes recent days)

### Example Data Flow

**Today's date: October 6, 2025**

```
agg_daily_revenue (public schema)
└─ Oct 6: total_daily_revenue = $12,345
           ↓ unpivot
fact_kpi_daily (nate_sandbox)
└─ Oct 6: kpi_id=14, value=$12,345
           ↓ aggregate
agg_kpi_dashboard (nate_sandbox)
└─ Oct 6: as_of_date=Oct 6
          mtd_value = $65,000 (Oct 1-6 sum)
          mtd_prior = $58,000 (Oct 1-6 last year)
          mtd_delta = $7,000
          mtd_delta_pct = 12.1%
          ytd_value = $1.2M (Jan 1 - Oct 6)
          ytd_prior = $1.1M
          ...etc
```

## 🐛 If Something Goes Wrong

### Error: "relation does not exist"
**Fix:** Build dependencies in order
```bash
dbt run --select dim_date
dbt run --select agg_daily_revenue
dbt run --select fact_kpi_daily
dbt run --select agg_kpi_dashboard
```

### Error: "package dbt-utils not installed"
**Fix:** Run `dbt deps`

### No data in agg_kpi_dashboard
**Fix:** Check that agg_daily_revenue has recent data:
```sql
SELECT MAX(date_day) FROM agg_daily_revenue;
```

### Tests failing
**Fix:** Look at the specific test that failed and validate data quality

## 📚 Documentation Files I Created

1. **EXECUTION_CHECKLIST.md** - Step-by-step checklist with verification queries
2. **IMPLEMENTATION_GUIDE.md** - Complete technical guide
3. **QUICK_START.md** - Command reference
4. **SUMMARY.md** - High-level overview
5. **ARCHITECTURE_DIAGRAM.md** - Visual system architecture
6. **WHATS_NEXT_FOR_YOU.md** - This file!

**Start with EXECUTION_CHECKLIST.md** - it has checkboxes for every step!

## ✅ Success Checklist

You'll know everything is working when:

- [ ] All dbt models build successfully
- [ ] All tests pass
- [ ] Query returns 27 rows: `SELECT * FROM nate_sandbox.agg_kpi_dashboard WHERE as_of_date = CURRENT_DATE`
- [ ] YoY comparisons are populated (mtd_prior, ytd_prior not null)
- [ ] CRON job runs without errors
- [ ] New data appears daily in agg_kpi_dashboard

## 🎉 You're Ready!

The system is fully configured and ready to deploy. All your changes are in the `nate_sandbox` schema, so there's zero risk to your production data.

**Your next command:**
```bash
dbt deps && dbt seed && dbt run --select dim_date agg_daily_revenue fact_kpi_daily agg_kpi_dashboard
```

Then run through the validation queries in **EXECUTION_CHECKLIST.md**.

Questions? Check the other documentation files - they have tons of examples and troubleshooting tips!

Good luck! 🚀

---

P.S. When you're ready to build your frontend dashboard, the `payload` JSONB column has everything formatted and ready to go. Just one SQL query and you have all 27 KPIs with MTD/QTD/YTD/Last28 metrics and YoY comparisons! 📊

