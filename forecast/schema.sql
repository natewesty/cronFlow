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
