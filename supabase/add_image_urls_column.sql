-- Migration: fold image_urls into the properties table
-- Run this in the Supabase SQL editor (Dashboard → SQL Editor → New query)
--
-- After running this:
--   1. Run: python3 supabase/migrate_data.py   (re-upserts all properties with image_urls)
--   2. The property_images table can be left in place (harmless) or dropped:
--        DROP TABLE property_images;

-- ── Step 1: add the column ─────────────────────────────────────────────────

ALTER TABLE properties
  ADD COLUMN IF NOT EXISTS image_urls text[] NOT NULL DEFAULT '{}';

-- ── Step 2: back-fill from property_images (if that table still exists) ────
-- Copies existing image URL rows into the new column, ordered by position.

UPDATE properties p
SET image_urls = sub.urls
FROM (
  SELECT
    property_id,
    array_agg(url ORDER BY position) AS urls
  FROM property_images
  GROUP BY property_id
) sub
WHERE p.id = sub.property_id;

-- ── Step 3: replace the properties_with_thumbnail view ─────────────────────
-- DROP + CREATE is required here because CREATE OR REPLACE VIEW fails when
-- the column list changes (the new image_urls column shifts the position of
-- thumbnail_url in the p.* expansion, which PostgreSQL treats as a rename).

DROP VIEW IF EXISTS properties_with_thumbnail;

CREATE VIEW properties_with_thumbnail AS
SELECT
  p.*,
  p.image_urls[1] AS thumbnail_url   -- PostgreSQL arrays are 1-indexed
FROM properties p;
