# Morning Handoff Notes — 14 Sep 2026

## TL;DR

Both repos are committed and pushed. Pipeline is fully automated now:

- **Hourly** incremental update (new/delisted/price/status) — should resume
  automatically after GitHub's 60-day dormancy disable was lifted by our pushes
- **Weekly full scrape** — Sundays 03:00 UTC, first run Sunday 20 Sep
- Hourly runs now persist `properties/` + `geocache.json` via GitHub Actions
  cache, so they're true incrementals (~15 min, was a hidden ~3h full scrape)

## Repos

| Repo | Latest commit | Notes |
|---|---|---|
| `Kozor1/re-scraper` | `c32abed` | scraper pipeline |
| `Kozor1/Swome` | `5660c92` | web + mobile app |

## Todo tomorrow (re-scraper)

1. **Actions tab → "Hourly Property Update"** — confirm the manual run (started
   ~tonight) finished OK; it's doing a full backfill (~3h expected).
2. Confirm a scheduled run fires at the **top of the next hour** on its own
   (proves the 60-day dormancy disable is gone).
3. Optionally trigger **"Weekly Full Scrape" → Run workflow** to force a clean
   rebuild (else it runs Sunday 03:00 UTC automatically).
4. Watch the summary table in the run log: `New / Deleted / Updated` per agent.
   After the backfill, these should be small numbers.

## Todo tomorrow (Swome app)

1. **Actions tab** — the CI run for `5660c92` should be green now (the failing
   `npm ci` was the app's lockfile being out of sync; fixed by pinning
   `react-dom@19.1.0`).
2. Browse the web app + mobile app; check:
   - Nest Estate Agents listings appear (~80 properties)
   - Sale Agreed statuses are up to date
   - No stale listings from before June
3. If UI edits are needed: `git pull` first, then work in:
   - web: `swome-web/` (Next.js), mobile: `swome-app/` (Expo)

## Supabase RLS warning (rls_disabled_in_public)

Supabase emailed that a public table lacks Row-Level Security. All **known**
schema tables (properties, folders, folder_properties, swipes, pairs,
pair_members, pair_invites, profiles) have RLS enabled in migrations, so the
alert is probably one of:

- `public.spatial_ref_sys` — created automatically by PostGIS, never needs RLS;
  Supabase's linter flags it anyway. Safe to dismiss, or silence:
  `alter table public.spatial_ref_sys enable row level security;`
- A table created manually in the dashboard at some point — check the alert
  email for the table name, or run the linter: Supabase dashboard →
  Database → Linter → "rls_disabled_in_public".

If it IS a real app table, enable RLS and add policies, e.g.:

```sql
alter table public.<table> enable row level security;
-- read-only for everyone is usually right for property data:
create policy "public read" on public.<table> for select using (true);
```

## Useful commands (re-scraper)

```
python3 full_scrape.py               # fresh rebuild all 35 agents + Supabase
python3 property_update.py           # incremental sync (what CI runs hourly)
python3 property_update.py nest bmc  # subset
python3 property_update.py --dry-run
```

## Gotchas to remember

- Manual `python3` runs locally use `venv/bin/python3` (Python 3.14).
- GitHub will auto-disable schedules again after 60 days of no pushes —
  if hourly ever goes silent again, Actions tab → workflow → "Enable workflow".
- `logs/` locally is wiped + gitignored; CI keeps its own log artifacts.

---

# Parser / geocoder fixes — 24 Sep 2026

Four data-quality bugs found by probing live detail pages
(probes in `../.probe/` — saved HTML + `probe_parse2.py` re-parses them).

1. **MM: all 232 pins were at the agent's office**
   (11 Portland Avenue, Glengormley, `54.671869, -5.957852`).
   Every McMillan McClure page embeds JSON-LD `"@type": "RealEstateAgent"`
   whose geo is the office, and `extract_embedded_latlng` took the *first*
   `"latitude"` in the HTML. Fixed: the extractor now sweeps all matches and
   skips any coords inside an agent/office JSON-LD script. MM pages have no
   other embedded coords, so they fall through to the geocoder (their
   addresses carry postcodes → street-level). Also fixed: MM key features
   were empty — modern parser now falls back to `.DescriptionBox--bullets`.
   → existing bad `lat/lng` rows need a null-out (see Supabase migration 009
   in the Swome repo) and a geocode re-run **without** recovering from
   town-level fallbacks (see 4).

2. **EE parser was wrong CMS family.** Edmonton Estates pages are Bluecubes
   markup (`prop-det-info-row`, `prop-det-address-one/two`), not PropertyPal
   Classic — bedrooms/receptions/status/description all came out empty and
   addresses as `" 1 Castleburn, "` (dangling comma, no town/postcode).
   `ee_full_scrape.py` now uses `parse_pp_bluecubes_detail` +
   `extract_pp_bluecubes_gallery`. EE's price row has an icon-only label
   (private-use glyph strips to `""`), so the Bluecubes parser now falls back
   to "any info-row value containing a £ amount is the price".

3. **TR parser was the wrong CMS family the other way.** Templeton Robinson
   detail pages are Classic (`ul.dettbl`), not Bluecubes — bedrooms/receptions
   were NULL. `tr_full_scrape.py` now uses `parse_pp_classic_detail` +
   `extract_pp_gallery_images`.

4. **Postcodes + address hygiene.** New shared helper
   `enrich_address_with_postcode(data, soup, html)` (wired into classic +
   bluecubes parsers): appends town/postcode from `h2.prop-det-address-two`
   or the `var address = "…"` JS map blob when the headline address lacks a
   BT postcode; also collapses whitespace/dangling commas and dedupes a
   repeated town segment. Fixes JM legacy rows too (current JM parse was
   already fine via og:title). **Geocoder**: town-only / "Belfast, NI"
   last-resort candidates now return `precise=False` and are *not* written to
   `geocache.json` — the property stays unmapped (per migration 009) instead
   of pinned on the town centre. `--retry-failed` will revisit them.

Validated offline against saved pages (EE, TR, JM, 3× MM) — re-run:
`cd ../.probe && python probe_parse2.py`.

## Next session — start here (finalize parser/geocoder fixes)

Everything below this line is written so a fresh LLM can pick it up cold.
Code changes are DONE, compiled, and offline-validated. What remains is
data cleanup + live validation + a re-scrape.

### Context in 30 seconds
- Four bugs fixed in `re-scraper/re-scraper/` (this repo, the *nested* one —
  parent `C:\…\Work\re-scraper\` is scratch/probe territory):
  1. `scrapers/base.py::extract_embedded_latlng` no longer eats the MM agent's
     office coords from RealEstateAgent JSON-LD (was pinning all mm listings
     at 54.671869, -5.957852).
  2. `ee_full_scrape.py` switched classic → bluecubes parser+gallery.
  3. `tr_full_scrape.py` switched bluecubes → classic parser+gallery.
  4. `enrich_address_with_postcode()` (base.py, wired into classic+bluecubes)
     appends town/BT-postcode to postcode-less addresses; modern parser gains
     `.DescriptionBox--bullets` key-features fallback; `geocode.py` town-only
     candidates are now diagnostics-only (`precise=False`, cached as None).
- Probe corpus: `../.probe/*.html` (saved live pages) + `probe_parse2.py`.
  Re-run anytime: `cd ../.probe && python probe_parse2.py` — all fields should
  parse (no office coords on mm, postcode'd addresses on ee/tr/jm).

### To finalize (in order)
1. **Live smoke test** — run one agent per fix against the live site and eyeball
   output JSONs under `re-scraper/properties/<agent>/property_*/`:
   `python scrapers/ee_full_scrape.py`, `python scrapers/tr_full_scrape.py`,
   `python scrapers/mm_full_scrape.py` (mm needs lat/lng ABSENT from JSON now —
   that's correct; geocoder supplies them later).
2. **DB cleanup (Supabase)** — apply pair with Swome migration 009:
   null out `lat/lng` for mm rows (all currently = office coords), and any rows
   pinned at a town centre. Then let the ingestion path re-geocode.
3. **Geocache purge gotcha** — `geocache.json` already holds town-centre coords
   saved as *successful* hits by old runs. `--retry-failed` only revisits
   `null`s, so those poisoned entries are INVISIBLE to it. Purge them first:
   script it (delete geocache keys whose address belongs to source mm/ee/tr, or
   whose saved coords match a known town centre), then
   `python geocode.py --limit 0` to rebuild, then re-run scrapes/upload.
4. **Full re-scrape** of the four affected agents (mm, ee, tr — jm only needs
   it if you want legacy rows backfilled; current jm parse was already OK):
   `python full_scrape.py` or a subset per `property_update.py` conventions.
5. **Verify in Swome** — map view: no pins at Portland Avenue, Glengormley;
   mm pins scatter at street level; ee cards show beds/receptions/status;
   tr cards show beds/receptions (were NULL in DB).

### Risk notes
- The shared parsers serve ~30 agents — classic-parser whitespace/comma tidy
  and the postcode enricher are additive fallbacks, but watch the first full
  run's logs for any address mojibake on other agents.
- Bluecubes "any info-row value with £ is price" fallback is safe (beds/baths
  rows are bare numbers) — only fires when no labelled price row exists.

