# Deployment Flow Visualization

## 🔄 What Happens When You Push to GitHub

```
┌─────────────────────────────────────────────────────────────────┐
│                    YOU PUSH TO GITHUB                           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                    RENDER DETECTS CHANGES                       │
│  • Pulls latest code from GitHub                               │
│  • Rebuilds service with new files                             │
│  • Deploys updated ingest.py + dbt models                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│              YOU MANUALLY TRIGGER CRON JOB                      │
│                  (Runs: python ingest.py)                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                   INGEST.PY EXECUTES                            │
│                                                                 │
│  Step 1: Data Ingestion                                        │
│  ├─ Commerce7 API → raw_customer, raw_order, etc.             │
│  └─ Tock API → raw_tock_guest, raw_tock_reservation           │
│                                                                 │
│  Step 2: dbt deps                                              │
│  └─ Installs dbt-utils package                                │
│                                                                 │
│  Step 3: dbt seed                                              │
│  └─ Loads nate_sandbox.dim_kpi (27 rows)                      │
│  └─ Loads nate_sandbox.dim_entity (1 row)                     │
│                                                                 │
│  Step 4: dbt run --exclude kpi.*                               │
│  ├─ Builds staging models (stg_*)                             │
│  ├─ Builds marts models (dim_*, fct_*)                        │
│  └─ Builds agg_daily_revenue ← CRITICAL!                      │
│                                                                 │
│  Step 5: dbt run --select kpi.*                                │
│  ├─ Builds nate_sandbox.dim_date                              │
│  ├─ Builds nate_sandbox.fact_kpi_daily (unpivots)            │
│  └─ Builds nate_sandbox.agg_kpi_dashboard (incremental)      │
│                                                                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                  RESULT: nate_sandbox SCHEMA                    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────┐      │
│  │  dim_date                                            │      │
│  │  • 7,670 rows (2015-2035)                           │      │
│  │  • Fiscal periods, quarters, months                 │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────┐      │
│  │  dim_kpi (seed)                                     │      │
│  │  • 27 rows (your KPI definitions)                   │      │
│  │  • Currency, percent, number formats                │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────┐      │
│  │  dim_entity (seed)                                  │      │
│  │  • 1 row (ALL)                                      │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────┐      │
│  │  fact_kpi_daily                                     │      │
│  │  • 27 rows per day (unpivoted from agg_daily_rev)  │      │
│  │  • Long format: date_key, kpi_id, entity_id, value │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────┐      │
│  │  agg_kpi_dashboard                                  │      │
│  │  • 27 rows for today's as_of_date                   │      │
│  │  • MTD/QTD/YTD/Last28 + YoY comparisons            │      │
│  │  • JSON payload for API consumption                │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Data Flow Detail

```
agg_daily_revenue (public schema)
┌─────────────────────────────────────────────────────────┐
│ date_day | tasting_room_wine_revenue | wine_club_... │
├──────────┼───────────────────────────┼───────────────┤
│ 10-06-25 |         12,345            |    8,765      │ ← Wide format
└─────────────────────────────────────────────────────────┘
                        │
                        │ UNPIVOT
                        ↓
fact_kpi_daily (nate_sandbox)
┌─────────────────────────────────────────────────────────┐
│ date_key | kpi_id | entity_id | value                  │
├──────────┼────────┼───────────┼────────────────────────┤
│ 10-06-25 |   1    |     0     |  12,345  ← TR Wine    │
│ 10-06-25 |   2    |     0     |   3,500  ← TR Fees    │ ← Long format
│ 10-06-25 |   3    |     0     |  15,845  ← TR Total   │
│   ...    |  ...   |    ...    |   ...                  │
│ 10-06-25 |  27    |     0     |     3.5  ← Conversion │
└─────────────────────────────────────────────────────────┘
                        │
                        │ AGGREGATE (MTD/QTD/YTD/Last28)
                        ↓
agg_kpi_dashboard (nate_sandbox)
┌──────────────────────────────────────────────────────────────┐
│ as_of_date | kpi_id | mtd_value | ytd_value | ytd_delta_pct │
├────────────┼────────┼───────────┼───────────┼───────────────┤
│  10-06-25  |   1    |   65,000  | 1,200,000 |    +12.5%     │
│  10-06-25  |   2    |   18,000  |   350,000 |    +8.3%      │
│  10-06-25  |  27    |      3.5  |       3.2 |    +9.4%      │
└──────────────────────────────────────────────────────────────┘
                        │
                        │ QUERY
                        ↓
              ┌─────────────────┐
              │  Your Frontend  │
              │   Dashboard     │
              └─────────────────┘
```

## ⏱️ Timeline

### First Deployment (Today)
```
T+0:00   Push to GitHub
T+0:30   Render deploys (typical: 2-5 minutes)
T+1:00   Manually trigger CRON job
T+1:05   Data ingestion completes
T+1:06   dbt deps + seed complete
T+1:35   dbt run (marts) completes
T+1:40   dbt run (kpi) completes ✅
         
Total: ~5-10 minutes for first run
```

### Subsequent Daily Runs
```
T+0:00   CRON job auto-triggers
T+0:03   Data ingestion (incremental) completes
T+0:04   dbt deps + seed (cached/skipped)
T+0:15   dbt run (marts, incremental) completes
T+0:18   dbt run (kpi, incremental) completes ✅
         
Total: ~15-20 seconds added to existing CRON job
```

## 🎯 Before vs After

### BEFORE (Your Current Setup)
```
CRON Job Execution:
┌─────────────────────────────────────┐
│  python ingest.py                   │
│  ├─ Pull Commerce7 data             │
│  ├─ Pull Tock data                  │
│  └─ Run dbt (all models)            │
│     ├─ staging                      │
│     ├─ marts                        │
│     └─ (no KPI system)              │
└─────────────────────────────────────┘

Result: Raw + staging + marts only
```

### AFTER (With KPI System)
```
CRON Job Execution:
┌─────────────────────────────────────┐
│  python ingest.py                   │
│  ├─ Pull Commerce7 data             │
│  ├─ Pull Tock data                  │
│  └─ Run dbt (staged)                │
│     ├─ dbt deps (packages)          │
│     ├─ dbt seed (KPI defs)          │
│     ├─ staging                      │
│     ├─ marts                        │
│     └─ ✨ KPI SYSTEM ✨             │
│        ├─ dim_date                  │
│        ├─ fact_kpi_daily            │
│        └─ agg_kpi_dashboard         │
└─────────────────────────────────────┘

Result: Everything + automated KPI dashboard
```

## 📈 What You Get

### Immediate Benefits
- ✅ **Automated KPI calculations** every CRON run
- ✅ **Year-over-year comparisons** built-in
- ✅ **MTD/QTD/YTD/Last28** windows automatically calculated
- ✅ **JSON API payload** ready for frontend consumption
- ✅ **Zero manual intervention** required

### Daily Data Refresh
Every time CRON runs:
1. New data flows in from APIs
2. `agg_daily_revenue` gets today's row
3. `fact_kpi_daily` adds 27 new rows (today's metrics)
4. `agg_kpi_dashboard` updates 27 rows (today's rollups)

**Result:** Fresh dashboard data every single day! 🎉

## 🔍 Monitoring Your First Run

### What to Watch in Render Logs

Look for these log messages:

```
✅ SUCCESS INDICATORS:
───────────────────────────────────────────────────────
🔄 Step 0: Installing dbt packages...
✅ dbt deps completed

🔄 Step 1: Loading seed data...
✅ Seed data loaded

🔄 Step 2: Building staging and marts models...
✅ Step 2 completed - marts built successfully

🔄 Step 3: Building KPI models...
✅ Step 3 completed - KPI models built successfully

🎉 Complete pipeline (ingestion + dbt) finished successfully!
```

### If You See Errors

Common first-run issues:

```
❌ "relation does not exist: agg_daily_revenue"
   → Check that Step 2 (marts) completed successfully
   → Verify agg_daily_revenue is in public schema

❌ "package dbt-utils not found"
   → dbt deps failed to install packages
   → Check Render has internet access to dbt package registry

❌ "duplicate key value violates unique constraint"
   → Check for duplicate dates in agg_daily_revenue
   → Run deduplication query (see EXECUTION_CHECKLIST.md)
```

## 🎊 Success!

When you see this in the logs:
```
🎉 Complete pipeline (ingestion + dbt) finished successfully!
```

Your KPI dashboard is LIVE and will auto-update daily! 🚀

---

**Ready to deploy?** Check `READY_TO_DEPLOY.md` for the final checklist!

