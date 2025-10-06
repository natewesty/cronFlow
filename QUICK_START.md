# Quick Start Commands

## 🚀 First Time Setup

```bash
# 1. Install dbt packages
dbt deps

# 2. Load dimension seeds
dbt seed

# 3. Build in order
dbt run --select dim_date
dbt run --select agg_daily_revenue    # If not already built
dbt run --select fact_kpi_daily
dbt run --select agg_kpi_dashboard

# 4. Run tests
dbt test --select kpi.*
```

## 🔄 Daily Operations

```bash
# Full KPI refresh (add to your CRON job)
dbt run --select agg_daily_revenue fact_kpi_daily agg_kpi_dashboard
dbt test --select kpi.*
```

## 🔍 Quick Validation Queries

### Check if today's data exists
```sql
select 
    count(*) as kpi_count,
    max(as_of_date) as latest_date
from nate_sandbox.agg_kpi_dashboard
where as_of_date = current_date;
-- Expected: 27 rows (one per KPI)
```

### View today's top-line metrics
```sql
select 
    dk.kpi_name,
    t.mtd_value,
    t.mtd_delta_pct * 100 as mtd_yoy_pct,
    t.ytd_value,
    t.ytd_delta_pct * 100 as ytd_yoy_pct
from nate_sandbox.agg_kpi_dashboard t
join nate_sandbox.dim_kpi dk on dk.kpi_id = t.kpi_id
where t.as_of_date = current_date
    and dk.kpi_code in (
        'total_daily_revenue',
        'tasting_room_total_revenue',
        'wine_club_total_revenue',
        'ecomm_revenue',
        'total_9l_sold',
        'new_member_acquisition'
    )
order by dk.kpi_id;
```

## 🎯 Testing Individual Models

```bash
# Test specific model
dbt run --select dim_date
dbt run --select fact_kpi_daily
dbt run --select agg_kpi_dashboard

# Test with full refresh
dbt run --select agg_kpi_dashboard --full-refresh
```

## 📊 Backfill Historical Data

```bash
# Edit dbt_project.yml:
# Change: kpi_dashboard_backfill_days: 60

dbt run --select agg_kpi_dashboard

# Then change back to: kpi_dashboard_backfill_days: 1
```

## 🔧 Maintenance

### Rebuild everything from scratch
```bash
dbt run --select kpi.* --full-refresh
```

### Just rebuild the dashboard
```bash
dbt run --select agg_kpi_dashboard --full-refresh
```

### Rebuild seeds if you update them
```bash
dbt seed --full-refresh
dbt run --select fact_kpi_daily agg_kpi_dashboard --full-refresh
```

## 📈 Performance Indexes (run after initial build)

```sql
-- Add these indexes for better query performance
create index if not exists ix_fact_kpi_daily_kpi_date 
on nate_sandbox.fact_kpi_daily (kpi_id, date_key);

create index if not exists ix_agg_kpi_dashboard_fetch 
on nate_sandbox.agg_kpi_dashboard (as_of_date, kpi_id, entity_id);

create index if not exists ix_agg_kpi_dashboard_kpi_date
on nate_sandbox.agg_kpi_dashboard (kpi_id, as_of_date desc);
```

## ⚠️ Common Issues

**Error: "relation does not exist"**
→ Build dependencies first: `dbt run --select agg_daily_revenue`

**No rows returned**
→ Check source data: `select max(date_day) from agg_daily_revenue;`

**Tests failing**
→ Run `dbt deps` if you haven't installed dbt-utils

## 📁 File Structure

```
models/
  kpi/
    dim_date.sql              ← Date dimension
    fact_kpi_daily.sql        ← Unpivoted daily facts (NEW!)
    agg_kpi_dashboard.sql     ← Incremental dashboard
    schema.yml                ← Tests & docs
  seeds/
    dim_kpi.csv               ← 27 KPI definitions (UPDATED!)
    dim_entity.csv            ← Entity dimension
macros/
  kpi_date_utils.sql          ← Date window utilities
```

All output goes to: **`nate_sandbox` schema** ✅

