# KPI Dashboard Implementation Guide

## 🎯 Overview

You've successfully created the foundation for a powerful KPI dashboard system in the `nate_sandbox` schema. This guide will walk you through the implementation steps.

## 📊 Architecture

### Data Flow
```
agg_daily_revenue (existing, public schema)
    ↓
fact_kpi_daily (unpivoted, nate_sandbox)
    ↓
agg_kpi_dashboard (incremental, nate_sandbox)
    ↓
API / Frontend
```

### Key Components

1. **dim_date** - Date dimension table (2015-2035) with fiscal period calculations
2. **dim_kpi** - KPI definitions (27 metrics covering all your revenue, traffic, and club metrics)
3. **dim_entity** - Entity dimension (currently just "ALL", expandable for location/channel breakdowns)
4. **fact_kpi_daily** - Daily KPI facts in long format (date, kpi_id, entity_id, value)
5. **agg_kpi_dashboard** - Incremental rollups with MTD/QTD/YTD/Last28 + YoY comparisons

## 🚀 Implementation Steps

### Step 1: Install Dependencies

```bash
cd /workspace
dbt deps
```

This installs the `dbt-utils` package needed for the unique combination tests.

### Step 2: Load Seeds

```bash
dbt seed
```

This creates:
- `nate_sandbox.dim_kpi` (27 KPIs)
- `nate_sandbox.dim_entity` (1 entity: ALL)

### Step 3: Build the Date Dimension

```bash
dbt run --select dim_date
```

Creates `nate_sandbox.dim_date` with dates from 2015-01-01 to 2035-12-31.

### Step 4: Ensure agg_daily_revenue is Built

Since `fact_kpi_daily` depends on `agg_daily_revenue`, make sure it's built in the public schema:

```bash
dbt run --select agg_daily_revenue
```

### Step 5: Build fact_kpi_daily

```bash
dbt run --select fact_kpi_daily
```

This unpivots your wide `agg_daily_revenue` table into the long format needed by the KPI framework.

### Step 6: Build the KPI Dashboard (Initial)

```bash
dbt run --select agg_kpi_dashboard
```

This creates the incremental table with today's snapshot (based on `kpi_dashboard_backfill_days: 1`).

### Step 7: Run Tests

```bash
dbt test --select kpi.*
```

Validates:
- Uniqueness constraints
- Not null constraints
- Accepted values for format and target_direction
- Unique combination of keys

### Step 8: (Optional) Historical Backfill

If you want to generate historical snapshots for the last 60 days:

1. **Temporarily update `dbt_project.yml`:**

```yaml
vars:
  kpi_dashboard_backfill_days: 60  # Change from 1 to 60
```

2. **Run the backfill:**

```bash
dbt run --select agg_kpi_dashboard
```

3. **Revert the change:**

```yaml
vars:
  kpi_dashboard_backfill_days: 1  # Change back to 1
```

## 📈 Your 27 KPIs

### Revenue Metrics (14)
1. Tasting Room Wine Revenue
2. Tasting Room Fees Revenue
3. Tasting Room Total Revenue
4. Wine Club Orders Revenue
5. Wine Club Fees Revenue
6. Wine Club Total Revenue
7. eCommerce Revenue
8. Phone Revenue
9. Event Fees Orders Revenue
10. Event Fees Reservations Revenue
11. Event Fees Total Revenue
12. Event Wine Revenue
13. Shipping Revenue
14. Total Daily Revenue

### Traffic & Guest Metrics (7)
15. Total Reservations
16. Total Visitors
17. Average Party Size
18. Tasting Room Guests
19. Event Guests
20. Avg Tasting Fee Per Guest
21. Tasting Room Orders Per Guest %

### Wine Sales (1)
22. Total 9L Sold

### Club Membership Metrics (5)
23. Total Active Club Membership
24. New Member Acquisition
25. Member Attrition
26. Club Net Gain/Loss
27. Club Conversion Per Taster %

## 🔍 Querying the Dashboard

### Get Today's KPIs

```sql
select 
    dk.kpi_code,
    dk.kpi_name,
    dk.format,
    dk.target_direction,
    t.mtd_value,
    t.mtd_prior,
    t.mtd_delta,
    t.mtd_delta_pct,
    t.ytd_value,
    t.ytd_prior,
    t.ytd_delta,
    t.ytd_delta_pct,
    t.payload
from nate_sandbox.agg_kpi_dashboard t
join nate_sandbox.dim_kpi dk on dk.kpi_id = t.kpi_id
where t.as_of_date = current_date
    and coalesce(t.entity_id, 0) = 0
order by dk.kpi_id;
```

### Get Specific KPI Time Series

```sql
select 
    t.as_of_date,
    dk.kpi_name,
    t.mtd_value,
    t.qtd_value,
    t.ytd_value,
    t.last28_value,
    t.ytd_delta_pct
from nate_sandbox.agg_kpi_dashboard t
join nate_sandbox.dim_kpi dk on dk.kpi_id = t.kpi_id
where dk.kpi_code = 'total_daily_revenue'
    and t.as_of_date >= current_date - interval '30 days'
order by t.as_of_date desc;
```

### Get JSON Payload for API

```sql
select 
    dk.kpi_code,
    t.payload
from nate_sandbox.agg_kpi_dashboard t
join nate_sandbox.dim_kpi dk on dk.kpi_id = t.kpi_id
where t.as_of_date = current_date;
```

The JSON payload structure:
```json
{
  "as_of": "2025-10-06",
  "mtd": {"v": 12345.67, "p": 11000.00, "d": 1345.67, "dp": 0.122},
  "qtd": {"v": 45678.90, "p": 42000.00, "d": 3678.90, "dp": 0.088},
  "ytd": {"v": 234567.89, "p": 210000.00, "d": 24567.89, "dp": 0.117},
  "last28": {"v": 23456.78, "p": 21000.00, "d": 2456.78, "dp": 0.117}
}
```

## 🔄 Daily Operations

### Schedule in CRON

Add to your cron job after the data ingestion completes:

```bash
# Rebuild agg_daily_revenue (in public schema)
dbt run --select agg_daily_revenue

# Build KPI tables (in nate_sandbox)
dbt run --select fact_kpi_daily
dbt run --select agg_kpi_dashboard

# Run tests
dbt test --select kpi.*
```

### Performance Optimization

Consider adding indexes after the initial build:

```sql
-- Index on fact_kpi_daily for faster filtering
create index if not exists ix_fact_kpi_daily_kpi_date 
on nate_sandbox.fact_kpi_daily (kpi_id, date_key);

-- Index on agg_kpi_dashboard for API queries
create index if not exists ix_agg_kpi_dashboard_fetch 
on nate_sandbox.agg_kpi_dashboard (as_of_date, kpi_id, entity_id);

-- Index for time series queries
create index if not exists ix_agg_kpi_dashboard_kpi_date
on nate_sandbox.agg_kpi_dashboard (kpi_id, as_of_date desc);
```

## 🎨 Frontend Integration

### Example React Component Pseudo-code

```javascript
const kpiData = await fetch('/api/kpi-dashboard?date=today');

kpiData.forEach(kpi => {
  const { kpi_code, kpi_name, format, payload } = kpi;
  
  // Display MTD
  displayMetric({
    name: kpi_name,
    value: formatValue(payload.mtd.v, format),
    priorValue: formatValue(payload.mtd.p, format),
    deltaPercent: payload.mtd.dp,
    isUp: payload.mtd.d > 0
  });
  
  // Display YTD
  displayMetric({
    name: `${kpi_name} (YTD)`,
    value: formatValue(payload.ytd.v, format),
    priorValue: formatValue(payload.ytd.p, format),
    deltaPercent: payload.ytd.dp,
    isUp: payload.ytd.d > 0
  });
});
```

## 🔧 Customization Options

### Add More Entities

If you want to break down KPIs by location, channel, or product line:

1. **Update `dim_entity.csv`:**
```csv
entity_id,entity_code
0,ALL
1,TASTING_ROOM
2,ECOMM
3,PHONE
```

2. **Update `fact_kpi_daily.sql`** to add entity-specific rows

### Add More KPIs

1. Add to `agg_daily_revenue.sql`
2. Add to `dim_kpi.csv` with next available ID
3. Add UNION ALL clause in `fact_kpi_daily.sql`

### Change Date Ranges

Update in `dbt_project.yml`:

```yaml
vars:
  dim_date_start: '2020-01-01'  # Start from 2020
  dim_date_end: '2030-12-31'    # End at 2030
```

Then run: `dbt run --select dim_date --full-refresh`

## 🐛 Troubleshooting

### Issue: "relation does not exist"

**Solution:** Build models in order:
```bash
dbt run --select dim_date
dbt run --select agg_daily_revenue  # if not already built
dbt run --select fact_kpi_daily
dbt run --select agg_kpi_dashboard
```

### Issue: "No data in agg_kpi_dashboard"

**Solution:** Check that `agg_daily_revenue` has current data:
```sql
select max(date_day) from agg_daily_revenue;
```

### Issue: "Slow queries"

**Solution:** 
1. Add indexes (see Performance Optimization above)
2. Reduce `kpi_dashboard_lookback_days` if you don't need full YoY
3. Consider partitioning `agg_kpi_dashboard` by month

## 📚 Additional Resources

- See `KPI_DASHBOARD_SETUP.md` for the original design document
- Run `dbt docs generate && dbt docs serve` for lineage graph
- Check `models/kpi/schema.yml` for all tests and documentation

## ✅ Success Criteria

You'll know it's working when:
1. All tests pass: `dbt test --select kpi.*`
2. Query returns today's metrics: `select * from nate_sandbox.agg_kpi_dashboard where as_of_date = current_date limit 10;`
3. YoY comparisons are populated: `select * from nate_sandbox.agg_kpi_dashboard where mtd_prior is not null limit 10;`

## 🎉 You're Ready!

Your KPI dashboard is fully configured and ready to deploy. The entire system operates in the `nate_sandbox` schema, keeping your operational public schema completely untouched.

