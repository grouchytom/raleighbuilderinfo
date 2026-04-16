/**
 * wake-enrichment.js
 *
 * Enriches Raleigh building permit records with Wake County real estate data.
 *
 * SETUP REQUIRED:
 *   The Wake County XLSX cannot be fetched directly from S3 in a browser
 *   (CORS blocked + access restricted). Instead:
 *
 *   1. Download the file manually from:
 *      https://www.wake.gov/departments-government/tax-administration/
 *      data-files-statistics-and-reports/real-estate-property-data-files
 *
 *   2. Save it as  wake-data.xlsx  in the same folder as index.html
 *
 *   3. This module fetches it from http://localhost:8000/wake-data.xlsx
 *      which works fine since it's same-origin.
 *
 * The file is refreshed by Wake County daily. Re-download it whenever you
 * want fresh assessed values / sale data (typically once a week is enough).
 *
 * SheetJS must be loaded before this module runs:
 *   <script src="https://cdn.jsdelivr.net/npm/xlsx/dist/xlsx.full.min.js"></script>
 */

import { normalizeAddress, fuzzyMatch, geocodeToParcelPIN } from './raleigh-permits-api.js';


// ---------------------------------------------------------------------------
// CONSTANTS
// ---------------------------------------------------------------------------

/**
 * Local path to the Wake County residential XLSX.
 * Must be in the same folder as index.html so python3 -m http.server serves it.
 */
const WAKE_LOCAL_PATH = './wake-data.xlsx';

/**
 * Column header names in the Wake County 2025_Residential_Report.xlsx.
 *
 * NOTE: Wake County uses mixed-case headers. These are the known column names
 * from the residential report. If the file uses different headers, the
 * probeWakeColumns() function will log the actual headers for you.
 *
 * The residential report contains single-family homes only, which is exactly
 * what we want for new construction matching.
 */
const WAKE_COLS = {
  REID:        'REID',
  PIN:         'PIN_NUM',
  ADDR:        'SITE_ADDR',
  CITY:        'SITE_CITY',
  ZIP:         'SITE_ZIP',
  OWNER:       'OWNER',
  ASSESSED:    'TOTAL_VALUE_ASSD',
  LAND:        'LAND_VALUE',
  BLDG:        'BLDG_VALUE',
  SALE_PRICE:  'SALE_PRICE',
  SALE_DATE:   'SALE_DATE',
  SALE_VALID:  'SALE_VALID',
  DEED_DATE:   'DEED_DATE',
  YEAR_BUILT:  'YEAR_BUILT',
  SQFT:        'TOTAL_AREA_SQFT',
  BEDS:        'BEDS',
  BATHS:       'BATHS',
};

/** Fuzzy match threshold — scores below this trigger geocoder fallback. */
const MATCH_THRESHOLD = 82;


// ---------------------------------------------------------------------------
// SECTION 1 — LOAD & PARSE XLSX
// ---------------------------------------------------------------------------

/**
 * Load wake-data.xlsx from the local server and parse it with SheetJS.
 *
 * Returns an array of row objects keyed by column header.
 *
 * @param {object} [opts]
 * @param {Function} [opts.onProgress]
 * @param {AbortSignal} [opts.signal]
 * @returns {Promise<object[]>}
 */
async function loadLocalXLSX({ onProgress, signal } = {}) {
  onProgress?.({
    stage:   'wake-download',
    message: 'Loading Wake County data file (wake-data.xlsx)…',
    pct:     0,
  });

  let res;
  try {
    res = await fetch(WAKE_LOCAL_PATH, { signal });
  } catch (err) {
    throw new Error(
      `Could not load wake-data.xlsx from ${WAKE_LOCAL_PATH}.\n` +
      `Make sure you have downloaded the file and placed it in your raleigh-dashboard folder.\n` +
      `Download from: https://www.wake.gov/departments-government/tax-administration/data-files-statistics-and-reports/real-estate-property-data-files\n` +
      `Detail: ${err.message}`
    );
  }

  if (!res.ok) {
    if (res.status === 404) {
      throw new Error(
        `wake-data.xlsx not found (404).\n` +
        `Download the 2025_Residential_Report.xlsx from Wake County, rename it wake-data.xlsx,\n` +
        `and place it in the same folder as index.html.`
      );
    }
    throw new Error(`Failed to load wake-data.xlsx: HTTP ${res.status}`);
  }

  onProgress?.({ stage: 'wake-download', message: 'Parsing Wake County spreadsheet…', pct: 0.5 });

  // Read as ArrayBuffer and parse with SheetJS
  const buffer   = await res.arrayBuffer();
  const XLSX     = window.XLSX;

  if (!XLSX) {
    throw new Error(
      'SheetJS (XLSX) is not loaded. Add this to index.html before other scripts:\n' +
      '<script src="https://cdn.jsdelivr.net/npm/xlsx/dist/xlsx.full.min.js"></script>'
    );
  }

  const workbook = XLSX.read(buffer, { type: 'array', cellDates: true });
  const sheet    = workbook.Sheets[workbook.SheetNames[0]];
  const rows     = XLSX.utils.sheet_to_json(sheet, { defval: null });

  onProgress?.({
    stage:   'wake-parse',
    message: `Parsed ${rows.length.toLocaleString()} Wake County records.`,
    pct:     1,
  });

  return rows;
}

/**
 * probeWakeColumns()
 *
 * Call this from the browser console if Wake County enrichment isn't matching.
 * Logs the actual column headers and a sample row from wake-data.xlsx so you
 * can update WAKE_COLS to match.
 *
 * Usage: await probeWakeColumns();
 */
export async function probeWakeColumns() {
  console.group('Wake County XLSX column probe');
  const rows = await loadLocalXLSX();
  if (rows.length === 0) {
    console.log('File loaded but contains no rows.');
    console.groupEnd();
    return;
  }
  console.log('Column headers:', Object.keys(rows[0]));
  console.log('Sample row 1:', rows[0]);
  console.log('Sample row 2:', rows[1]);
  console.groupEnd();
}


// ---------------------------------------------------------------------------
// SECTION 2 — BUILD LOOKUP MAPS
// ---------------------------------------------------------------------------

/**
 * Build two in-memory lookup maps from the parsed rows:
 *
 *   addressMap  — normalised address → row[]
 *   reidMap     — REID string → row
 *
 * @param {object[]} rows
 * @returns {{ addressMap: Map, reidMap: Map, totalRows: number }}
 */
export function buildLookupMaps(rows) {
  const addressMap = new Map();
  const reidMap    = new Map();

  for (const row of rows) {
    const reid = String(row[WAKE_COLS.REID] || '').trim();
    const addr = String(row[WAKE_COLS.ADDR] || '').trim();

    if (reid) reidMap.set(reid, row);

    if (addr) {
      const key = normalizeAddress(addr);
      if (!addressMap.has(key)) addressMap.set(key, []);
      addressMap.get(key).push(row);
    }
  }

  return { addressMap, reidMap, totalRows: rows.length };
}


// ---------------------------------------------------------------------------
// SECTION 3 — PER-PERMIT MATCHING
// ---------------------------------------------------------------------------

/**
 * Find the best Wake County row for a single permit address.
 *
 * Strategy:
 *   1. Exact key lookup (O(1))
 *   2. Fuzzy scan filtered by street number prefix
 *   3. Geocoder → REID fallback if score < threshold
 *
 * @param {string} permitAddr
 * @param {Map} addressMap
 * @param {Map} reidMap
 * @param {AbortSignal} [signal]
 * @returns {Promise<{ row: object|null, score: number, method: string }>}
 */
async function findWakeRecord(permitAddr, addressMap, reidMap, signal) {
  if (!permitAddr) return { row: null, score: 0, method: 'no-address' };

  const normPermit = normalizeAddress(permitAddr);

  // 1. Exact lookup
  if (addressMap.has(normPermit)) {
    return { row: addressMap.get(normPermit)[0], score: 100, method: 'exact' };
  }

  // 2. Fuzzy scan — only check keys sharing the same street number
  const streetNum = normPermit.split(' ')[0];
  let bestScore = 0, bestRow = null, bestKey = null;

  for (const [key, candidates] of addressMap) {
    if (!key.startsWith(streetNum)) continue;
    const score = fuzzyMatch(normPermit, key);
    if (score > bestScore) {
      bestScore = score;
      bestRow   = candidates[0];
      bestKey   = key;
    }
  }

  if (bestScore >= MATCH_THRESHOLD) {
    return { row: bestRow, score: bestScore, method: 'fuzzy', matchedKey: bestKey };
  }

  // 3. Geocoder → REID fallback
  const pin = await geocodeToParcelPIN(permitAddr, signal).catch(() => null);
  if (pin && reidMap.has(pin)) {
    return { row: reidMap.get(pin), score: 95, method: 'geocoder-pin' };
  }

  return { row: null, score: bestScore, method: 'unmatched' };
}

/**
 * Format a Wake County date value into YYYY-MM-DD, or null.
 *
 * @param {number|Date|string|null} val
 * @returns {string|null}
 */
function formatDate(val) {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString().slice(0, 10);
  const s = String(val).replace(/\D/g, '');
  if (s.length === 8) return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
  return null;
}

/**
 * Extract the fields we care about from a matched Wake row.
 *
 * @param {object} row
 * @param {object} matchMeta  — { score, method, matchedKey }
 * @returns {object}
 */
function extractEnrichment(row, matchMeta) {
  const salePrice = Number(row[WAKE_COLS.SALE_PRICE]) || null;
  const assessed  = Number(row[WAKE_COLS.ASSESSED])   || null;

  return {
    wake_reid:       String(row[WAKE_COLS.REID]  || '').trim() || null,
    wake_pin:        String(row[WAKE_COLS.PIN]   || '').trim() || null,
    wake_owner:      String(row[WAKE_COLS.OWNER] || '').trim() || null,
    wake_assessed:   assessed,
    wake_sale_price: salePrice,
    wake_sale_date:  formatDate(row[WAKE_COLS.SALE_DATE]),
    wake_sale_valid: String(row[WAKE_COLS.SALE_VALID] || '').toUpperCase() === 'Y',
    wake_deed_date:  formatDate(row[WAKE_COLS.DEED_DATE]),
    wake_year_built: Number(row[WAKE_COLS.YEAR_BUILT]) || null,
    wake_sqft:       Number(row[WAKE_COLS.SQFT])       || null,
    wake_beds:       Number(row[WAKE_COLS.BEDS])        || null,
    wake_baths:      Number(row[WAKE_COLS.BATHS])       || null,
    value_ratio:     (salePrice && assessed)
                       ? Math.round((salePrice / assessed) * 100) / 100
                       : null,
    _match: matchMeta,
  };
}


// ---------------------------------------------------------------------------
// SECTION 4 — BATCH ENRICHMENT
// ---------------------------------------------------------------------------

/**
 * Enrich all permits with Wake County data.
 * Each permit gets a .wake object attached, or .wake = null if unmatched.
 *
 * @param {object[]} permits
 * @param {{ addressMap: Map, reidMap: Map }} maps
 * @param {object} [opts]
 * @param {Function} [opts.onProgress]
 * @param {AbortSignal} [opts.signal]
 * @returns {Promise<{ permits: object[], enrichmentStats: object }>}
 */
export async function enrichPermits(permits, { addressMap, reidMap }, { onProgress, signal } = {}) {
  const CONCURRENCY = 4;
  let done = 0, matched = 0, exact = 0, fuzzyHit = 0, pinHit = 0, unmatched = 0;

  for (let i = 0; i < permits.length; i += CONCURRENCY) {
    await Promise.all(
      permits.slice(i, i + CONCURRENCY).map(async (permit, ci) => {
        const idx  = i + ci;
        // Use the confirmed lowercase field name from the live ArcGIS schema
        const addr = permit.originaladdress1;

        const { row, score, method, matchedKey } =
          await findWakeRecord(addr, addressMap, reidMap, signal);

        if (row) {
          permits[idx].wake = extractEnrichment(row, { score, method, matchedKey: matchedKey ?? null });
          matched++;
          if (method === 'exact')        exact++;
          if (method === 'fuzzy')        fuzzyHit++;
          if (method === 'geocoder-pin') pinHit++;
        } else {
          permits[idx].wake              = null;
          permits[idx].wake_match_failed = true;
          unmatched++;
        }

        done++;
        onProgress?.({
          stage:   'enrichment',
          message: `Enriching permits… ${done}/${permits.length} (${matched} matched)`,
          pct:     done / permits.length,
        });
      })
    );

    if (i + CONCURRENCY < permits.length) await new Promise(r => setTimeout(r, 20));
  }

  return {
    permits,
    enrichmentStats: {
      total:     permits.length,
      matched,
      exact,
      fuzzy:     fuzzyHit,
      geocoder:  pinHit,
      unmatched,
      matchRate: Math.round((matched / permits.length) * 100),
    },
  };
}


// ---------------------------------------------------------------------------
// SECTION 5 — MAIN ENTRY POINT
// ---------------------------------------------------------------------------

/**
 * loadWakeData()
 *
 * Loads wake-data.xlsx from the local server, parses it, and builds
 * the address + REID lookup maps ready for enrichPermits().
 *
 * @param {object} [opts]
 * @param {Function} [opts.onProgress]
 * @param {AbortSignal} [opts.signal]
 * @returns {Promise<{ addressMap: Map, reidMap: Map, totalRows: number, fileDate: string, dataset: string }>}
 */
export async function loadWakeData({ onProgress, signal } = {}) {
  const rows = await loadLocalXLSX({ onProgress, signal });

  onProgress?.({
    stage:   'wake-index',
    message: `Building address index from ${rows.length.toLocaleString()} records…`,
    pct:     null,
  });

  const { addressMap, reidMap, totalRows } = buildLookupMaps(rows);

  onProgress?.({
    stage:   'wake-ready',
    message: `Wake County index ready — ${totalRows.toLocaleString()} parcels indexed.`,
    pct:     1,
  });

  return {
    addressMap,
    reidMap,
    totalRows,
    fileDate: 'local file',
    dataset:  '2025 Residential Report (local)',
  };
}


// ---------------------------------------------------------------------------
// SECTION 6 — ENRICHED BUILDER STATS
// ---------------------------------------------------------------------------

/**
 * Add enriched sale price / assessed value stats to each builder record.
 *
 * @param {object[]} builders   — from aggregateBuilders()
 * @param {object[]} permits    — enriched permits
 * @returns {object[]}
 */
export function addEnrichedBuilderStats(builders, permits) {
  const permitMap = new Map(permits.map(p => [p.permitnum, p]));

  return builders.map(builder => {
    const bPermits  = builder.permits.map(p => permitMap.get(p.permitnum) || p);
    const matched   = bPermits.filter(p => p.wake);
    const qualified = matched.filter(p => p.wake?.wake_sale_valid && p.wake?.wake_sale_price);

    const salePrices  = qualified.map(p => p.wake.wake_sale_price);
    const assessments = matched.filter(p => p.wake?.wake_assessed).map(p => p.wake.wake_assessed);

    const avg = arr => arr.length ? Math.round(arr.reduce((s, v) => s + v, 0) / arr.length) : null;

    return {
      ...builder,
      avgSalePrice:   avg(salePrices),
      avgAssessed:    avg(assessments),
      salePriceRange: salePrices.length
        ? { min: Math.min(...salePrices), max: Math.max(...salePrices) }
        : null,
      matchRate:     Math.round((matched.length / bPermits.length) * 100),
      unmatched:     bPermits.length - matched.length,
      qualifiedSales: qualified.length,
    };
  });
}


// ---------------------------------------------------------------------------
// SECTION 7 — CONVENIENCE WRAPPER
// ---------------------------------------------------------------------------

/**
 * runEnrichmentPipeline()
 *
 * Loads wake-data.xlsx and enriches permits in one call.
 *
 * @param {object[]} permits
 * @param {object} [opts]
 * @returns {Promise<{ permits, enrichmentStats, wakeMetadata }>}
 */
export async function runEnrichmentPipeline(permits, opts = {}) {
  const wakeData = await loadWakeData(opts);

  const { permits: enriched, enrichmentStats } = await enrichPermits(
    permits, wakeData, opts
  );

  return {
    permits: enriched,
    enrichmentStats,
    wakeMetadata: {
      fileDate:  wakeData.fileDate,
      dataset:   wakeData.dataset,
      totalRows: wakeData.totalRows,
    },
  };
}
