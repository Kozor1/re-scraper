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
