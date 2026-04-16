/**
 * scripts/fetch-permits.js
 *
 * Fetches all new residential building permits from Raleigh's ArcGIS API
 * and saves them as data/permits.json.
 *
 * Run manually:    node scripts/fetch-permits.js
 * Run via GitHub Action: automatically every night
 *
 * Requires Node 18+ (uses built-in fetch)
 */

import { writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Config — must match raleigh-permits-api.js
// ---------------------------------------------------------------------------

const PERMITS_FS =
  'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services' +
  '/Building_Permits/FeatureServer/0';

const CUTOFF_DATE = '2020-01-01';
const PAGE_SIZE   = 2000;

const FIELDS = [
  'permitnum', 'workclass', 'workclassmapped', 'permittype',
  'permittypemapped', 'permitclassmapped',
  'statuscurrent', 'statuscurrentmapped',
  'applieddate', 'issueddate', 'recordupdatedate',
  'originaladdress1', 'originalcity', 'originalzip',
  'contractorcompanyname', 'contractorlicnum', 'statelicnum',
  'contractoremail', 'contractorphone',
  'contractoraddress1', 'contractorcity', 'contractorstate', 'contractorzip',
  'estprojectcost', 'latitude_perm', 'longitude_perm',
  'pin', 'totalsqft', 'numberstories',
  'description', 'proposeduse', 'proposedworkdescription',
  'constcompletedofficial',
  'parcelownername', 'parcelowneraddress1',
  'reviewercomments', 'perm_comments',
  'fiscalyear',
].join(',');

function buildWhereClause() {
  return `(workclassmapped = 'New') AND permitclassmapped = 'Residential'` +
         ` AND applieddate >= DATE '${CUTOFF_DATE}'`;
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

async function fetchPage(offset) {
  const params = new URLSearchParams({
    where:             buildWhereClause(),
    outFields:         FIELDS,
    returnGeometry:    'false',
    resultOffset:      String(offset),
    resultRecordCount: String(PAGE_SIZE),
    orderByFields:     'applieddate DESC',
    f:                 'json',
  });

  const url = `${PERMITS_FS}/query?${params}`;
  const res = await fetch(url);

  if (!res.ok) throw new Error(`HTTP ${res.status} from ArcGIS`);

  const json = await res.json();
  if (json.error) throw new Error(`ArcGIS error ${json.error.code}: ${json.error.message}`);

  return {
    features:              json.features || [],
    exceededTransferLimit: json.exceededTransferLimit ?? false,
  };
}

async function fetchAll() {
  const all = [];
  let offset = 0, hasMore = true, page = 0;

  while (hasMore) {
    console.log(`  Fetching page ${page + 1} (offset ${offset})…`);
    const { features, exceededTransferLimit } = await fetchPage(offset);
    all.push(...features.map(f => f.attributes));
    hasMore  = exceededTransferLimit && features.length === PAGE_SIZE;
    offset  += features.length;
    page++;

    // Brief pause to be respectful to the ArcGIS server
    if (hasMore) await new Promise(r => setTimeout(r, 200));
  }

  return all;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('Raleigh Builder Intelligence — nightly data fetch');
  console.log(`WHERE: ${buildWhereClause()}`);
  console.log('---');

  const t0 = Date.now();
  let permits;

  try {
    permits = await fetchAll();
  } catch (err) {
    console.error('Failed to fetch permits:', err.message);
    process.exit(1);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`Fetched ${permits.length.toLocaleString()} permits in ${elapsed}s`);

  // Write output
  const outDir  = join(__dirname, '..', 'data');
  const outFile = join(outDir, 'permits.json');

  mkdirSync(outDir, { recursive: true });

  const output = {
    fetchedAt:    new Date().toISOString(),
    permitCount:  permits.length,
    whereClause:  buildWhereClause(),
    permits,
  };

  writeFileSync(outFile, JSON.stringify(output), 'utf8');

  const sizeKb = Math.round(JSON.stringify(output).length / 1024);
  console.log(`Saved ${outFile} (${sizeKb.toLocaleString()} KB)`);
  console.log('Done.');
}

main();
