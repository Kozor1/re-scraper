#!/usr/bin/env node
/**
 * One-shot geocoding script — run independently of the server.
 * Reads all property indexes, geocodes any missing addresses via Nominatim,
 * saves progress to geocache.json every 20 entries and on exit.
 *
 * Usage:  node geocode_all.js
 * Safe to kill and restart — picks up where it left off.
 */

const fs    = require('fs');
const path  = require('path');
const https = require('https');

const ROOT          = __dirname;
const GEOCACHE_PATH = path.join(ROOT, 'geocache.json');
const SOURCES       = ['sb', 'ups', 'hc', 'jm', 'pp', 'tr', 'dh'];

// ── Load geocache ────────────────────────────────────────────────────────────
let geocache = {};
if (fs.existsSync(GEOCACHE_PATH)) {
  try { geocache = JSON.parse(fs.readFileSync(GEOCACHE_PATH, 'utf-8')); }
  catch { geocache = {}; }
}
console.log(`Geocache loaded: ${Object.keys(geocache).length} addresses`);

// ── Collect all unique addresses ─────────────────────────────────────────────
const allAddresses = new Set();
for (const src of SOURCES) {
  const srcDir   = path.join(ROOT, 'properties', src);
  const indexPath = path.join(srcDir, 'property_index.json');

  if (fs.existsSync(indexPath)) {
    // Sources with an index file (sb, ups, hc, jm, pp, tr)
    try {
      const idx = JSON.parse(fs.readFileSync(indexPath, 'utf-8'));
      for (const p of (idx.properties || [])) {
        const propJson = path.join(srcDir, p.id, `${p.id}.json`);
        if (fs.existsSync(propJson)) {
          try {
            const d = JSON.parse(fs.readFileSync(propJson, 'utf-8'));
            const addr = d.address || d.title || p.address;
            if (addr) allAddresses.add(addr);
          } catch { if (p.address) allAddresses.add(p.address); }
        } else if (p.address) {
          allAddresses.add(p.address);
        }
      }
    } catch (e) { console.error(`Error reading ${src} index:`, e.message); }
  } else if (fs.existsSync(srcDir)) {
    // Sources without an index — scan property_N directories directly (e.g. dh)
    try {
      const dirs = fs.readdirSync(srcDir).filter(d => d.startsWith('property_'));
      for (const dir of dirs) {
        const propJson = path.join(srcDir, dir, `${dir}.json`);
        if (fs.existsSync(propJson)) {
          try {
            const d = JSON.parse(fs.readFileSync(propJson, 'utf-8'));
            const addr = d.address || d.title;
            if (addr) allAddresses.add(addr);
          } catch {}
        }
      }
    } catch (e) { console.error(`Error scanning ${src} dir:`, e.message); }
  }
}

const queue = [...allAddresses].filter(a => a && !geocache[a]);
const total = allAddresses.size;
const done  = total - queue.length;
console.log(`${total} unique addresses total, ${done} already geocoded, ${queue.length} to go`);

if (queue.length === 0) {
  console.log('Nothing to do — all addresses already geocoded!');
  process.exit(0);
}

// ── Nominatim geocode ────────────────────────────────────────────────────────
function saveGeocache() {
  try { fs.writeFileSync(GEOCACHE_PATH, JSON.stringify(geocache)); }
  catch (e) { console.error('Save failed:', e.message); }
}

function geocodeAddress(address) {
  return new Promise(resolve => {
    const q    = encodeURIComponent(address);
    const opts = {
      hostname: 'nominatim.openstreetmap.org',
      path:     `/search?format=json&q=${q}&limit=1&countrycodes=gb`,
      headers:  {
        'User-Agent':       'PropertySwipeApp/1.0 (private-prototype)',
        'Accept-Language':  'en',
      },
    };
    const req = https.get(opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const results = JSON.parse(data);
          if (results.length > 0)
            resolve({ lat: parseFloat(results[0].lat), lng: parseFloat(results[0].lon) });
          else
            resolve(null);
        } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(6000, () => { req.destroy(); resolve(null); });
  });
}

// ── Process queue ────────────────────────────────────────────────────────────
let processed  = 0;
let succeeded  = 0;
const startTime = Date.now();

process.on('SIGINT',  () => { saveGeocache(); console.log(`\nSaved. ${Object.keys(geocache).length} addresses geocoded.`); process.exit(0); });
process.on('SIGTERM', () => { saveGeocache(); process.exit(0); });

function eta(remaining) {
  const secDone = (Date.now() - startTime) / 1000;
  if (processed === 0) return '?';
  const rate = processed / secDone;
  const secs = remaining / rate;
  if (secs < 60)  return `~${Math.round(secs)}s`;
  if (secs < 3600) return `~${Math.round(secs/60)}m`;
  return `~${(secs/3600).toFixed(1)}h`;
}

async function processNext(i) {
  if (i >= queue.length) {
    saveGeocache();
    console.log(`\n✅ Done! ${succeeded} new addresses geocoded. Total in cache: ${Object.keys(geocache).length}`);
    return;
  }

  const address = queue[i];
  const coords  = await geocodeAddress(address);
  processed++;

  if (coords) {
    geocache[address] = coords;
    succeeded++;
    if (succeeded % 20 === 0) saveGeocache();
  }

  if (processed % 50 === 0 || processed <= 5) {
    const pct = Math.round(((done + processed) / total) * 100);
    console.log(`[${done + processed}/${total}] ${pct}% — ETA ${eta(queue.length - i - 1)} — last: ${address.slice(0,50)}`);
  }

  // Nominatim rate limit: max 1 req/sec
  setTimeout(() => processNext(i + 1), 1100);
}

console.log(`\nStarting geocoding (1 req/sec, Ctrl+C saves progress)…\n`);
processNext(0);
