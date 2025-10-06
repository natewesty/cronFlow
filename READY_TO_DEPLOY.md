# Ready to Deploy - Pre-Push Checklist

## ✅ What Just Changed

Your `ingest.py` has been updated to automatically handle the KPI system build in the correct order:

### New Build Pipeline Flow:
```
1. Data Ingestion (Commerce7 + Tock APIs)
   ↓
2. dbt deps (install packages)
   ↓
3. dbt seed (load KPI definitions)
   ↓
4. dbt run --exclude kpi.* (build staging + marts, including agg_daily_revenue)
   ↓
5. dbt run --select kpi.* (build KPI models: fact_kpi_daily → agg_kpi_dashboard)
```

## 📋 Pre-Deployment Checklist

Before you push to GitHub and trigger the Render rebuild:

### 1. Files to Commit
- [x] `ingest.py` (updated with staged dbt builds)
- [x] `dbt_project.yml` (KPI configuration added)
- [x] `packages.yml` (dbt-utils dependency)
- [x] `models/kpi/dim_date.sql`
- [x] `models/kpi/fact_kpi_daily.sql` (NEW!)
- [x] `models/kpi/agg_kpi_dashboard.sql`
- [x] `models/kpi/schema.yml`
- [x] `models/seeds/dim_kpi.csv` (updated with 27 KPIs)
- [x] `models/seeds/dim_entity.csv`
- [x] `macros/kpi_date_utils.sql`
- [x] Documentation files (*.md)

### 2. Git Commands

```bash
# Check status
git status

# Add all KPI files
git add ingest.py
git add dbt_project.yml
git add packages.yml
git add models/kpi/
git add models/seeds/
git add macros/
git add *.md

# Commit
git commit -m "Add automated KPI dashboard system

- Updated ingest.py to build KPI models in correct order
- Created fact_kpi_daily to unpivot agg_daily_revenue
- Added 27 KPI definitions to dim_kpi seed
- Configured all KPI models for nate_sandbox schema
- Added dbt-utils package dependency
- Includes comprehensive documentation"

# Push to GitHub (triggers Render deployment)
git push origin main
```

### 3. Render Environment Variables

Make sure these are set in your Render dashboard:
- ✅ `DATABASE_URL` or individual `DB_*` variables
- ✅ `C7_AUTH_TOKEN` and `C7_TENANT`
- ✅ `X_TOCK_AUTH` and `X_TOCK_SCOPE`

### 4. What Will Happen on Render

Once you push to GitHub and Render rebuilds:

1. **Render will deploy** the updated code
2. **First CRON run** will execute:
   ```
   python ingest.py
   ```
3. **Inside ingest.py:**
   - Pulls Commerce7 + Tock data
   - Runs `dbt deps` (installs dbt-utils)
   - Runs `dbt seed` (loads dim_kpi with 27 KPIs)
   - Runs `dbt run --exclude kpi.*` (builds marts including agg_daily_revenue)
   - Runs `dbt run --select kpi.*` (builds KPI models)

4. **Result:**
   - `nate_sandbox.dim_kpi` - 27 rows
   - `nate_sandbox.dim_entity` - 1 row
   - `nate_sandbox.dim_date` - ~7,670 rows
   - `nate_sandbox.fact_kpi_daily` - 27 rows per day
   - `nate_sandbox.agg_kpi_dashboard` - 27 rows (for today)

## 🎯 After First CRON Run

### Validation Queries

Once the first CRON run completes, run these queries to verify:

```sql
-- 1. Check that all KPI tables exist
SELECT 
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'nate_sandbox'
ORDER BY tablename;

-- Expected output:
-- nate_sandbox | dim_date              | 7670
-- nate_sandbox | dim_entity            | 1
-- nate_sandbox | dim_kpi               | 27
-- nate_sandbox | fact_kpi_daily        | (27 × days in agg_daily_revenue)
-- nate_sandbox | agg_kpi_dashboard     | 27

-- 2. Check today's dashboard
SELECT 
    dk.kpi_name,
    t.mtd_value,
    t.ytd_value,
    ROUND(t.ytd_delta_pct * 100, 1) as ytd_yoy_pct
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = CURRENT_DATE
    AND dk.kpi_code IN (
        'total_daily_revenue',
        'tasting_room_total_revenue',
        'wine_club_total_revenue'
    )
ORDER BY dk.kpi_id;

-- Expected: 3 rows with your actual metrics

-- 3. Verify fact_kpi_daily unpivot
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT date_key) as unique_dates,
    COUNT(DISTINCT kpi_id) as unique_kpis,
    MIN(date_key) as earliest_date,
    MAX(date_key) as latest_date
FROM nate_sandbox.fact_kpi_daily;

-- Expected: 27 KPIs × number of days in agg_daily_revenue
```

## 🚨 What to Watch For

### Check the Render Logs

After pushing and the CRON job runs, check Render logs for:

✅ **Success indicators:**
```
✅ dbt deps completed
✅ Seed data loaded
✅ Step 2 completed - marts built successfully
✅ Step 3 completed - KPI models built successfully
🎉 Complete pipeline (ingestion + dbt) finished successfully!
```

❌ **Potential issues:**
- "relation does not exist" → Dependencies not built in order (shouldn't happen with new code)
- "package dbt-utils not found" → `dbt deps` failed (check Render logs)
- "No data to process" → Check if agg_daily_revenue has current data

## 🔧 If Something Goes Wrong

### Issue: KPI models fail to build

**Check:**
1. Did `agg_daily_revenue` build successfully?
   ```sql
   SELECT COUNT(*) FROM agg_daily_revenue;
   ```
2. Check Render logs for the specific error
3. Manually run on Render (via shell):
   ```bash
   cd /path/to/project
   dbt run --select agg_daily_revenue
   dbt run --select fact_kpi_daily
   dbt run --select agg_kpi_dashboard
   ```

### Issue: Seeds not loading

**Fix:**
Run manually on Render:
```bash
dbt seed --full-refresh
```

### Issue: "duplicate key" errors

**Check:**
```sql
-- Check for duplicates in fact_kpi_daily
SELECT date_key, kpi_id, entity_id, COUNT(*)
FROM nate_sandbox.fact_kpi_daily
GROUP BY date_key, kpi_id, entity_id
HAVING COUNT(*) > 1;
```

If found, it's a bug in `fact_kpi_daily.sql` - likely an issue with agg_daily_revenue having duplicate dates.

## 📊 Performance Notes

### Build Times (Expected)

- **dbt deps**: ~5-10 seconds (first time only)
- **dbt seed**: ~1-2 seconds
- **dbt run --exclude kpi.***: ~30-60 seconds (depends on your data volume)
- **dbt run --select kpi.***: ~5-10 seconds (first time), ~2-3 seconds (incremental)

**Total added time to CRON job**: ~15-20 seconds per run after initial setup

### Storage Impact

- **nate_sandbox schema**: ~10 MB per year
- **Minimal impact** on your database

## 🎉 Success Criteria

You'll know everything worked when:

1. ✅ Render deployment succeeds
2. ✅ CRON job completes without errors
3. ✅ All validation queries return expected results
4. ✅ `nate_sandbox.agg_kpi_dashboard` has 27 rows for today
5. ✅ YoY comparisons are populated (if you have last year's data)

## 📱 Next Steps After Successful Deployment

1. **Add indexes for performance** (see QUICK_START.md)
2. **Build API endpoints** to query the dashboard
3. **Create frontend components** to display KPIs
4. **(Optional) Backfill historical data** (see EXECUTION_CHECKLIST.md)

## 🔄 Daily Operations Going Forward

Once deployed, the system runs automatically:

1. **CRON job runs** → `python ingest.py`
2. **ingest.py automatically:**
   - Pulls new data from APIs
   - Builds all dbt models in correct order
   - Updates KPI dashboard incrementally (only today)
3. **Result:** Fresh KPIs every time CRON runs!

No manual intervention needed! 🎊

---

## 👍 You're Ready!

When you're ready, just:

```bash
git push origin main
```

Then trigger your CRON job and watch the magic happen! 🚀

Monitor the Render logs to see the pipeline execute. If you see any issues, check the troubleshooting section above or refer to the other documentation files.

Good luck! 🍀

