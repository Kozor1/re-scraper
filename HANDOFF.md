# Handoff Notes — 24 Sep 2026

> Consolidated state for both projects. Scrapers repo latest: `fbaa790`.
> Swome repo latest: `81f13dc`. Both repos pulled clean.

## re-scraper (Kozor1/re-scraper)

**Pipeline:** hourly incremental sync on GitHub Actions (`:17` and `:47` past
the hour, redundancy-checked; cache always saved; 300-min cap), weekly full
rebuild Sundays 03:00 UTC.

**Schema state:** Supabase migrations 008 + 009 live in the Swome repo — both
must be applied. 009 nulls the mm rows carrying agent-office coords;
re-scrape/migrate mm afterwards to repopulate true pins.

**Recent fixes** (both sessions combined):

| Area | Fix |
|---|---|
| Prices | spacing fixed (`Offers Over£x` on ag/pr/hg, `£ 150,000` le); ~364 rows patched |
| Images | le fancybox extractor; Pinterest/EPC/PDF junk filtered from carousels |
| Delisting | redirect-to-index guard (`redirected_off_page`); ups soft-404 handling |
| Hourly walk | regex link patterns honoured (mm); Step 4b orphan reconciliation |
| Geo | page-embedded coords preferred — **but** PropertyPal pages can embed the AGENT OFFICE's coords (mm fix at fbaa790 skips that block); NI bbox sanity guard on geocoder |
| Addresses | ups town enrichment; cps/ag/ups stale relists + bt `/PageN` dupes purged |

**Run today:** migration 009 in Supabase SQL Editor, then `python3
supabase/migrate_data.py --source mm` (or let the hourly job repopulate).

## Swome (Kozor1/Swome)

- Web live: **https://swome.vercel.app** — auto-deploys from `main`, see the
  git-author caveat below.
- Mobile: use the **EAS development build** APK for Android; Expo Go can't
  render Google Maps on this device/SDK combination. First working build:
  `aad25a09-8d1d-401f-a4b5-9e7db85b8ecd`. Rebuild:
  `eas build --profile development --platform android --non-interactive`
- Maps config: `GOOGLE_MAPS_API_KEY` flows via `app.config.js` (process.env —
  plain app.json substitution breaks the EAS manifest merge)
- Search: mobile = list → tap → swipe deck of matches (session chip, Back
  steps out); web = search filters the deck directly. Multi-word queries are
  per-term ANDed ilikes ("station road greenisland" works).
- Logo assets: header/loading show the real wordmark; launcher icon + splash
  are baked in the next native rebuild

**Git-author wrinkle (blocks Vercel):** commits pushed from this laptop's
*other* machine show `Lavery <ryan@loughshoretech.com>` / `rlav-lst
<ryan@loughshore.co>`, which Vercel doesn't recognise as a deployable author.
Fix on the other machine:

```bash
git -C ~/Desktop/re-scraper config user.email "r.m.lavery@hotmail.co.uk"
git -C ~/Desktop/Swome     config user.email "r.m.lavery@hotmail.co.uk"
```

Also in the Vercel dashboard you may need to authorise the currently-blocked
deployments manually once (light → "Authorize").

## Routine commands

re-scraper:
```bash
python3 property_update.py                    # hourly-style sync
python3 full_scrape.py                        # weekly full rebuild
python3 supabase/migrate_data.py --source mm
python3 geocode.py --source rb --retry-failed
```

Swome:
```bash
cd swome-web && npm run dev                   # http://localhost:3000
cd swome-app && npm start                     # dev build scans the QR
```
