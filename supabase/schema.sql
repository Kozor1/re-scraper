-- ============================================================
--  Property Swipe App — Supabase Schema
--  Run this in the Supabase SQL editor (Dashboard → SQL Editor)
-- ============================================================

-- ── Properties ───────────────────────────────────────────────

create table properties (
  id              bigint generated always as identity primary key,
  source          text    not null,          -- 'sb','ups','hc','jm','pp','tr','dh','mm','ce'
  source_id       text,                       -- original dir/id within source
  url             text,
  address         text    not null,
  title           text,
  price           text,                       -- display string e.g. "Offers Around £230,000"
  price_value     integer,                    -- numeric for range filtering/sorting
  status          text    default 'For Sale', -- 'For Sale', 'Sale Agreed', 'Sold'
  property_type   text,                       -- 'Detached', 'Semi-detached Bungalow', etc.
  bedrooms        text,
  bathrooms       text,
  receptions      text,
  epc_rating      text,
  description     text,
  key_features    jsonb   default '[]',
  rooms           jsonb   default '[]',
  image_urls      jsonb   default '[]',       -- array of CDN URL strings
  lat             float,                      -- latitude  (from geocoder)
  lng             float,                      -- longitude (from geocoder)
  created_at      timestamptz default now(),
  updated_at      timestamptz default now(),

  unique (source, source_id)
);

-- Indexes
create index properties_source_idx    on properties (source);
create index properties_status_idx    on properties (status);
create index properties_price_idx     on properties (price_value);
create index properties_bedrooms_idx  on properties (bedrooms);
create index properties_lat_lng_idx   on properties (lat, lng);


-- ── User swipe history ────────────────────────────────────────

create table swipes (
  id          bigint generated always as identity primary key,
  user_id     uuid   references auth.users(id) on delete cascade,
  property_id bigint references properties(id) on delete cascade,
  liked       boolean not null,   -- true = right swipe, false = left swipe
  swiped_at   timestamptz default now(),

  unique (user_id, property_id)
);

create index swipes_user_id_idx on swipes (user_id);


-- ── Folders ───────────────────────────────────────────────────

create table folders (
  id         bigint generated always as identity primary key,
  user_id    uuid   references auth.users(id) on delete cascade,
  name       text   not null,
  color      text   default '#3B82F6',
  position   integer default 0,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index folders_user_id_idx on folders (user_id);


-- ── Folder memberships ────────────────────────────────────────

create table folder_properties (
  id          bigint generated always as identity primary key,
  folder_id   bigint references folders(id) on delete cascade,
  property_id bigint references properties(id) on delete cascade,
  notes       text,
  added_at    timestamptz default now(),

  unique (folder_id, property_id)
);

create index folder_properties_folder_id_idx on folder_properties (folder_id);


-- ── Row Level Security ────────────────────────────────────────
-- Properties: anyone can read (public catalogue)
-- Swipes / folders: each user can only see their own data

alter table properties        enable row level security;
alter table swipes            enable row level security;
alter table folders           enable row level security;
alter table folder_properties enable row level security;

create policy "Properties are publicly readable"
  on properties for select using (true);

create policy "Users manage their own swipes"
  on swipes for all using (auth.uid() = user_id);

create policy "Users manage their own folders"
  on folders for all using (auth.uid() = user_id);

create policy "Users manage their own folder properties"
  on folder_properties for all
  using (
    folder_id in (select id from folders where user_id = auth.uid())
  );


-- ── Updated_at trigger ────────────────────────────────────────

create or replace function update_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create trigger properties_updated_at
  before update on properties
  for each row execute function update_updated_at();

create trigger folders_updated_at
  before update on folders
  for each row execute function update_updated_at();
