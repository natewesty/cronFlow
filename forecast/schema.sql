create schema if not exists forecast;

create table if not exists forecast.forecast_runs (
  run_id         bigserial primary key,
  started_at     timestamptz not null default now(),
  finished_at    timestamptz,
  status         text not null,
  origin_date    date not null,
  horizon_days   int  not null,
  engine_version text not null,
  params         jsonb not null,
  is_latest      boolean not null default false,
  error          text
);

create table if not exists forecast.forecast_daily (
  run_id     bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  kpi_id     int  not null,
  entity_id  int  not null default 0,
  date_key   date not null,
  horizon    int  not null,
  p50        numeric not null,
  p10        numeric,
  p90        numeric,
  otb        numeric,
  components jsonb,
  primary key (run_id, kpi_id, entity_id, date_key)
);

create table if not exists forecast.forecast_monthly (
  run_id          bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  kpi_id          int not null,
  entity_id       int not null default 0,
  month_start     date not null,
  is_actual       boolean not null default false,
  organic         numeric,
  organic_neutral numeric,
  release         numeric,
  event           numeric,
  fitted_growth   numeric,
  p50             numeric not null,
  primary key (run_id, kpi_id, entity_id, month_start)
);

create table if not exists forecast.forecast_accuracy (
  run_id         bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  kpi_id         int not null,
  horizon_bucket text not null,
  wmape          numeric,
  bias           numeric,
  ratio_p10      numeric,
  ratio_p90      numeric,
  weekly_wmape   numeric,
  monthly_wmape  numeric,
  n              int,
  primary key (run_id, kpi_id, horizon_bucket)
);

create table if not exists forecast.assumptions (
  key        text primary key,
  value      jsonb not null,
  updated_at timestamptz not null default now(),
  updated_by text
);

create table if not exists forecast.bookings_snapshot (
  snapshot_date    date not null,
  reservation_date date not null,
  attribution      text not null,
  reservations     int,
  guests           int,
  fees             numeric,
  primary key (snapshot_date, reservation_date, attribution)
);

create index if not exists forecast_daily_kpi_date_idx on forecast.forecast_daily (kpi_id, date_key);
create index if not exists forecast_runs_latest_idx on forecast.forecast_runs (is_latest) where is_latest;

-- ===== Forecast v2 =====================================================

alter table forecast.forecast_monthly add column if not exists release_planned boolean not null default false;
alter table forecast.forecast_runs add column if not exists bottle_rows int;

create table if not exists forecast.forecast_backtest_periods (
  run_id bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  kpi_id int not null,
  period text not null,          -- 'week' | 'month'
  ahead int not null,            -- 1 = first full period after origin, 2, 3 ...
  period_start date not null,
  origin_date date not null,
  actual numeric, forecast numeric, p10 numeric, p90 numeric,
  primary key (run_id, kpi_id, period, period_start, ahead)
);

create table if not exists forecast.forecast_accuracy_periods (
  run_id bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  kpi_id int not null, period text not null, ahead int not null,
  wmape numeric, bias numeric, hit_rate numeric, n int,
  primary key (run_id, kpi_id, period, ahead)
);

create table if not exists forecast.bottle_forecast (
  run_id bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  label text not null, vintage int not null, month_start date not null, channel text not null,
  p50 numeric not null, p10 numeric, p90 numeric,
  primary key (run_id, label, vintage, month_start, channel)
);

create table if not exists forecast.bottle_vintage_summary (
  run_id bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  label text not null, vintage int not null,
  varietal text, release_month date, sold_to_date int, cases_made int,
  lifetime_p50 numeric, lifetime_p10 numeric, lifetime_p90 numeric,
  primary key (run_id, label, vintage)
);
alter table forecast.bottle_vintage_summary add column if not exists profile_key text;

create table if not exists forecast.bottle_profiles (
  run_id bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  scope text not null,                   -- 'label' | 'tier'
  key text not null,                     -- label name, or '<varietal>|<price band>'
  channel text not null,
  curve jsonb not null,                  -- [share by month-since-release], 36 entries
  lifetime numeric, trend numeric, log_dispersion numeric, n_vintages int,
  primary key (run_id, scope, key, channel)
);

create table if not exists forecast.bottle_backtest (
  run_id bigint not null references forecast.forecast_runs(run_id) on delete cascade,
  label text not null, vintage int not null,
  first_year_actual numeric, first_year_forecast numeric, p10 numeric, p90 numeric,
  primary key (run_id, label, vintage)
);

create table if not exists forecast.bottle_vintage_plan (   -- app-written, plan-year independent
  label text not null, vintage int not null,
  cases_made int, assumed boolean not null default true,
  updated_at timestamptz not null default now(), updated_by text,
  primary key (label, vintage)
);

create table if not exists forecast.label_plan (
  plan_year int not null, label text not null,
  status text not null,                 -- 'continue' | 'retire' | 'new'
  incoming_vintage int, release_month date,
  override_outgoing numeric, override_incoming numeric,   -- plan-year bottles, null = use model
  price numeric, varietal text, comparable_label text,
  updated_at timestamptz not null default now(), updated_by text,
  primary key (plan_year, label)
);

create table if not exists forecast.club_release_history (   -- rebuilt every run
  release_start date not null, season text not null, tier_id uuid not null, tier_name text,
  label text not null, vintage int, qty_per_box numeric, price numeric,
  orders int, members int, take_rate numeric, bottles int, revenue numeric,
  primary key (release_start, tier_id, label, vintage)
);

create table if not exists forecast.club_release_plan (
  plan_year int not null, season text not null,            -- 'spring' | 'summer' | 'fall' | 'winter'
  release_date date not null,
  tier_id uuid not null, label text not null, vintage int not null, qty numeric not null,
  price numeric,                                            -- null = last known club price for the label
  updated_at timestamptz not null default now(), updated_by text,
  primary key (plan_year, season, tier_id, label, vintage)
);

create table if not exists forecast.club_release_tier_plan (
  plan_year int not null, season text not null, tier_id uuid not null,
  take_rate numeric, members_override int,
  updated_at timestamptz not null default now(), updated_by text,
  primary key (plan_year, season, tier_id)
);
