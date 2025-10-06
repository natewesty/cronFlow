# KPI Dashboard Architecture

## 📐 Complete System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PUBLIC SCHEMA                               │
│                        (Untouched/Safe)                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Raw Data Tables                   Dimension Tables                │
│  ┌──────────────┐                 ┌──────────────┐                │
│  │ stg_order    │                 │ dim_customer │                │
│  │ stg_product  │                 │ dim_product  │                │
│  │ stg_tock_*   │                 │ dim_date     │                │
│  └──────┬───────┘                 │ dim_*        │                │
│         │                         └──────┬───────┘                │
│         │                                │                         │
│         └────────────┬───────────────────┘                         │
│                      ↓                                              │
│         ┌──────────────────────────────┐                           │
│         │   agg_daily_revenue          │  ← You just built this!  │
│         │   ─────────────────          │                           │
│         │   • 1 row per day            │                           │
│         │   • Wide format (60+ cols)  │                           │
│         │   • All metrics pre-calc'd   │                           │
│         └──────────────┬───────────────┘                           │
│                        │                                            │
└────────────────────────┼────────────────────────────────────────────┘
                         │
                         │ CROSS-SCHEMA REFERENCE (ref())
                         │
┌────────────────────────┼────────────────────────────────────────────┐
│                        ↓        NATE_SANDBOX SCHEMA                 │
│                                 (Your KPI System)                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Dimension Seeds                                                    │
│  ┌──────────────────┐          ┌──────────────────┐               │
│  │  dim_kpi (seed)  │          │ dim_entity(seed) │               │
│  │  ───────────────  │          │ ────────────────  │               │
│  │  27 KPIs:        │          │  1 entity (ALL)  │               │
│  │  • Revenue (14)  │          │  Expandable for: │               │
│  │  • Traffic (7)   │          │  • Locations     │               │
│  │  • Sales (1)     │          │  • Channels      │               │
│  │  • Club (5)      │          │  • Segments      │               │
│  └──────────────────┘          └──────────────────┘               │
│                                                                     │
│  Date Dimension                                                     │
│  ┌─────────────────────────────────────────┐                      │
│  │  dim_date (table)                       │                      │
│  │  ────────────────                       │                      │
│  │  • 2015-01-01 to 2035-12-31            │                      │
│  │  • Fiscal periods                       │                      │
│  │  • ISO year                             │                      │
│  │  • Month/Quarter/Year boundaries        │                      │
│  └─────────────────────────────────────────┘                      │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  fact_kpi_daily (table) ← NEW!                            │   │
│  │  ──────────────────────                                    │   │
│  │  Unpivots agg_daily_revenue into long format              │   │
│  │                                                            │   │
│  │  Structure:                                                │   │
│  │  ┌─────────┬────────┬───────────┬────────┐               │   │
│  │  │date_key │ kpi_id │ entity_id │ value  │               │   │
│  │  ├─────────┼────────┼───────────┼────────┤               │   │
│  │  │10-06-25 │   1    │     0     │ 12345  │ ← TR Wine     │   │
│  │  │10-06-25 │   2    │     0     │  8765  │ ← TR Fees     │   │
│  │  │10-06-25 │   3    │     0     │ 21110  │ ← TR Total    │   │
│  │  │   ...   │  ...   │    ...    │  ...   │               │   │
│  │  │10-06-25 │  27    │     0     │   3.5  │ ← Conversion% │   │
│  │  └─────────┴────────┴───────────┴────────┘               │   │
│  │                                                            │   │
│  │  27 rows per day × 365 days = 9,855 rows/year             │   │
│  └─────────────────────────┬─────────────────────────────────┘   │
│                             │                                      │
│                             ↓                                      │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  agg_kpi_dashboard (incremental table)                    │   │
│  │  ──────────────────────────────────────                   │   │
│  │  Time-based rollups with YoY comparisons                  │   │
│  │                                                            │   │
│  │  Structure:                                                │   │
│  │  ┌──────────┬────────┬──────────┬─────────┬─────────┐    │   │
│  │  │as_of_date│ kpi_id │ mtd_value│mtd_prior│ payload │... │   │
│  │  ├──────────┼────────┼──────────┼─────────┼─────────┤    │   │
│  │  │10-06-25  │   1    │   5000   │  4500   │ {...}   │    │   │
│  │  │10-06-25  │   2    │   3500   │  3200   │ {...}   │    │   │
│  │  │10-06-25  │  27    │    3.5   │   3.1   │ {...}   │    │   │
│  │  └──────────┴────────┴──────────┴─────────┴─────────┘    │   │
│  │                                                            │   │
│  │  Includes:                                                 │   │
│  │  • MTD/QTD/YTD/Last28 (current + prior year)             │   │
│  │  • Deltas (absolute & percentage)                         │   │
│  │  • JSON payload for API consumption                       │   │
│  │                                                            │   │
│  │  Incremental Strategy:                                     │   │
│  │  • Only recomputes last N days (default: 1)              │   │
│  │  • Merges updates (fast!)                                 │   │
│  │  • Keeps historical snapshots                             │   │
│  └─────────────────────────┬─────────────────────────────────┘   │
│                             │                                      │
└─────────────────────────────┼──────────────────────────────────────┘
                              │
                              ↓
                    ┌─────────────────────┐
                    │   API / Frontend    │
                    │   ────────────────   │
                    │   • Dashboard UI    │
                    │   • Reports         │
                    │   • Alerts          │
                    │   • Exports         │
                    └─────────────────────┘
```

## 🔄 Data Refresh Flow

```
Daily CRON Job:
─────────────────────────────────────────────────────────────────

1. Raw Data Ingestion
   └─> Updates: stg_* tables (public schema)

2. Build Marts  
   └─> dbt run --select agg_daily_revenue
       └─> Creates today's row in public.agg_daily_revenue

3. Build KPI Facts
   └─> dbt run --select fact_kpi_daily
       └─> Unpivots today into 27 rows in nate_sandbox.fact_kpi_daily

4. Update KPI Dashboard
   └─> dbt run --select agg_kpi_dashboard
       └─> Recomputes today's rollups
           ├─> Scans last 400 days for YoY calculations
           ├─> Calculates MTD/QTD/YTD/Last28
           ├─> Computes YoY deltas and percentages
           └─> Merges 27 updated rows into nate_sandbox.agg_kpi_dashboard

5. Run Tests
   └─> dbt test --select kpi.*
       └─> Validates data quality

Total Time: ~5-10 seconds
```

## 📊 Query Patterns

### Pattern 1: Today's Dashboard (All KPIs)
```sql
SELECT * FROM nate_sandbox.agg_kpi_dashboard 
WHERE as_of_date = CURRENT_DATE;
```
**Returns:** 27 rows (one per KPI)
**Speed:** <50ms with index

### Pattern 2: Specific KPI Over Time
```sql
SELECT as_of_date, mtd_value, ytd_value 
FROM nate_sandbox.agg_kpi_dashboard 
WHERE kpi_id = 14  -- Total Daily Revenue
  AND as_of_date >= CURRENT_DATE - 30
ORDER BY as_of_date DESC;
```
**Returns:** 30 rows (one per day)
**Speed:** <100ms with index

### Pattern 3: API JSON Payload
```sql
SELECT dk.kpi_code, dk.kpi_name, dk.format, t.payload
FROM nate_sandbox.agg_kpi_dashboard t
JOIN nate_sandbox.dim_kpi dk ON dk.kpi_id = t.kpi_id
WHERE t.as_of_date = CURRENT_DATE;
```
**Returns:** 27 rows with JSON objects
**Speed:** <100ms with index

## 🏗️ Build Dependencies

```
Dependency Graph:
─────────────────────────────────────────────────

Level 1 (Seeds & Base Tables)
├─ dim_kpi (seed)
├─ dim_entity (seed)  
├─ dim_date (model)
└─ agg_daily_revenue (model, public schema)
       │
       ↓
Level 2 (KPI Facts)
└─ fact_kpi_daily (model)
       │
       ↓
Level 3 (Aggregations)
└─ agg_kpi_dashboard (incremental model)

Build Order:
1. dbt seed
2. dbt run --select dim_date
3. dbt run --select agg_daily_revenue
4. dbt run --select fact_kpi_daily
5. dbt run --select agg_kpi_dashboard
```

## 💾 Storage Estimates

```
Model                    Rows/Day    Size/Row    Daily Growth
────────────────────────────────────────────────────────────────
fact_kpi_daily              27       ~100 bytes     ~3 KB
agg_kpi_dashboard           27       ~500 bytes    ~14 KB
────────────────────────────────────────────────────────────────
TOTAL PER DAY                                      ~17 KB

Annual Storage:
• fact_kpi_daily:      27 × 365 = 9,855 rows    = ~1 MB/year
• agg_kpi_dashboard:   27 × 365 = 9,855 rows    = ~5 MB/year
• With indexes:                                  = ~10 MB/year

10-Year Projection: ~100 MB total (negligible!)
```

## ⚡ Performance Characteristics

### Initial Build (Full History)
- **fact_kpi_daily**: 3-5 seconds (depends on agg_daily_revenue row count)
- **agg_kpi_dashboard**: 5-10 seconds (scans 400 days × 27 KPIs)
- **Total**: ~10-15 seconds

### Incremental Build (Daily)
- **fact_kpi_daily**: 1 second (27 new rows)
- **agg_kpi_dashboard**: 1-2 seconds (27 updated rows, scans last 400 days)
- **Total**: ~2-3 seconds

### Query Performance
- **Today's dashboard**: <50ms (with index)
- **Time series (30 days)**: <100ms (with index)
- **Historical backfill (60 days)**: 10-15 seconds

## 🔐 Schema Isolation Benefits

### Advantages of nate_sandbox
✅ **Zero risk** - Production schema untouched
✅ **Iterate freely** - Drop/recreate without fear
✅ **Easy rollback** - Just drop the schema
✅ **Clear separation** - Dev vs. prod
✅ **Migration path** - Move to production schema when ready

### Migration to Production (Future)
When ready to productionize:
1. Update `dbt_project.yml`: Change `+schema: nate_sandbox` to `+schema: kpi`
2. Run: `dbt run --select kpi.* --full-refresh`
3. Update API endpoints to point to new schema
4. Drop `nate_sandbox` schema

## 🎯 Extension Points

### Add Entity Breakdown
Currently: 1 entity (ALL)
Future: Multiple entities

```sql
-- Example: Add location-based entities
INSERT INTO nate_sandbox.dim_entity VALUES
  (0, 'ALL'),
  (1, 'MAIN_TASTING_ROOM'),
  (2, 'CAVE_TASTING_ROOM'),
  (3, 'ONLINE');

-- Then update fact_kpi_daily to break down by location
```

### Add More KPIs
1. Add column to `agg_daily_revenue`
2. Add row to `dim_kpi.csv`
3. Add UNION ALL in `fact_kpi_daily.sql`
4. Rebuild: `dbt run --select fact_kpi_daily+ --full-refresh`

### Add Custom Time Windows
Currently: MTD/QTD/YTD/Last28
Future: Last90, Last365, Custom fiscal periods

Edit `agg_kpi_dashboard.sql` and add new window calculations

## 📚 File Reference

```
📁 models/kpi/
  ├── 📄 dim_date.sql           (Date dimension, 2015-2035)
  ├── 📄 fact_kpi_daily.sql     (Unpivoted KPI facts) ← NEW!
  ├── 📄 agg_kpi_dashboard.sql  (Incremental rollups)
  └── 📄 schema.yml             (Tests & documentation)

📁 models/seeds/
  ├── 📄 dim_kpi.csv            (27 KPI definitions) ← UPDATED!
  └── 📄 dim_entity.csv         (Entity dimension)

📁 macros/
  └── 📄 kpi_date_utils.sql     (Date window utilities)

📁 Root/
  ├── 📄 dbt_project.yml        (Project config) ← UPDATED!
  ├── 📄 packages.yml           (dbt-utils dependency)
  ├── 📄 IMPLEMENTATION_GUIDE.md (Setup instructions)
  ├── 📄 QUICK_START.md         (Command reference)
  ├── 📄 SUMMARY.md             (Project overview)
  └── 📄 ARCHITECTURE_DIAGRAM.md (This file)
```

---

This architecture provides a solid foundation for enterprise-grade KPI tracking! 🚀

