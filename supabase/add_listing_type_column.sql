-- Migration: add listing_type column to properties table
-- Run this once in the Supabase SQL editor.
--
-- listing_type distinguishes sale properties from rentals:
--   'sale'  – for-sale listings (default, covers all existing rows)
--   'rent'  – rental / to-let listings

alter table properties
  add column if not exists listing_type text not null default 'sale';

-- Back-fill: any existing row has no suffix on source, so they're all sales.
-- (This is a no-op because the column default already sets 'sale'.)
update properties set listing_type = 'sale' where listing_type is null;

-- Optional index for fast filtering by type
create index if not exists properties_listing_type_idx on properties (listing_type);

-- Verify
select listing_type, count(*) from properties group by listing_type;
