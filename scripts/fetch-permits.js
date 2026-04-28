// scripts/fetch-permits.js
// Nightly GitHub Action script — fetches permits from ArcGIS + subcontractor data from EnerGov
// Run with: node scripts/fetch-permits.js

import { writeFile, readFile } from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '..', 'data');

// ─── ArcGIS config ────────────────────────────────────────────────────────────
const ARCGIS_BASE = 'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services/Building_Permits/FeatureServer/0/query';
const FIELDS = '*';

// ─── EnerGov config ───────────────────────────────────────────────────────────
const ENERGOV_BASE = 'https://raleighnc-energovpub.tylerhost.net/apps/selfservice/api/energov';
const ENERGOV_HEADERS = {
  'Content-Type': 'application/json;charset=UTF-8',
  'Accept': 'application/json, text/plain, */*',
  'tenantid': '1',
  'tenantname': 'RaleighNCProd',
  'tyler-tenant-culture': 'en-US',
  'tyler-tenanturl': 'RaleighNCProd',
  'Cookie': 'Tyler-Tenant-Culture=en-US',
  'Origin': 'https://raleighnc-energovpub.tylerhost.net',
  'Referer': 'https://raleighnc-energovpub.tylerhost.net/apps/selfservice',
};

// Rate limiting — be a good citizen
const DELAY_MS = 150;
const BATCH_SIZE = 10;
const delay = ms => new Promise(r => setTimeout(r, ms));

// ─── Contact type labels to include (skip noise like "Applicant") ─────────────
const CONTACT_TYPES_INCLUDE = new Set([
  'General Contractor',
  'Electrical Contractor',
  'Plumbing Contractor',
  'Mechanical Contractor',
  'HVAC Contractor',
  'Framing Contractor',
  'Insulation Contractor',
  'Roofing Contractor',
  'Masonry Contractor',
  'Fire Sprinkler Contractor',
  'Low Voltage Contractor',
  'Fire Alarm Contractor',
  'Owner',
  'Contractor',
  'Sub Contractor',
]);

// ─── Sub-record types to include (skip stormwater, ROW, etc.) ─────────────────
const SUBRECORD_TYPES_INCLUDE = [
  'Sub Permit',
  'Electrical',
  'Plumbing',
  'Mechanical',
  'HVAC',
];

// ─── ArcGIS fetch ─────────────────────────────────────────────────────────────
async function fetchArcGIS(mode) {
  const isNew = mode === 'new';

  // Match the exact WHERE clause pattern from the original working script
  const where = isNew
    ? "(workclassmapped = 'New') AND permitclassmapped = 'Residential' AND applieddate >= DATE '2020-01-01'"
    : "(workclassmapped = 'Existing') AND permitclassmapped = 'Residential' AND applieddate >= DATE '2020-01-01'";

  console.log(`  WHERE: ${where}`);

  let allFeatures = [];
  let offset = 0;
  const pageSize = 2000;

  while (true) {
    const params = new URLSearchParams({
      where,
      outFields: FIELDS,
      f: 'json',
      resultOffset: offset,
      resultRecordCount: pageSize,
    });

    const res = await fetch(`${ARCGIS_BASE}?${params}`);
    if (!res.ok) throw new Error(`ArcGIS HTTP ${res.status} for mode=${mode}`);
    const data = await res.json();

    if (data.error) {
      throw new Error(`ArcGIS error: ${JSON.stringify(data.error)}`);
    }

    if (!data.features || data.features.length === 0) break;
    allFeatures.push(...data.features.map(f => f.attributes));
    console.log(`  ArcGIS ${mode}: fetched ${allFeatures.length} permits...`);

    if (!data.exceededTransferLimit) break;
    offset += pageSize;
  }

  return allFeatures;
}

// ─── EnerGov: search for EntityId by permit number ───────────────────────────
async function lookupEntityId(permitnum) {
  const payload = {
    Keyword: permitnum,
    ExactMatch: true,
    SearchModule: 1,
    FilterModule: 1,
    SearchMainAddress: false,
    PageNumber: 1,
    PageSize: 5,
    SortBy: null,
    SortAscending: true,
    // All criteria fields null/empty — only keyword search needed
    PermitCriteria: {
      PermitNumber: null, PermitTypeId: null, PermitWorkclassId: null,
      PermitStatusId: null, ProjectName: null, Address: null,
      PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false,
    },
    PlanCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    InspectionCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    CodeCaseCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    RequestCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    BusinessLicenseCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    ProfessionalLicenseCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    LicenseCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
    ProjectCriteria: { PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false },
  };

  try {
    const res = await fetch(`${ENERGOV_BASE}/search/search`, {
      method: 'POST',
      headers: ENERGOV_HEADERS,
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    const results = data?.Result?.EntityResults;
    if (!results || results.length === 0) return null;

    // Find exact match on CaseNumber
    const match = results.find(r =>
      r.CaseNumber?.toLowerCase() === permitnum.toLowerCase()
    ) || results[0];

    return match?.CaseId || null;
  } catch (e) {
    console.warn(`  ⚠ EntityId lookup failed for ${permitnum}:`, e.message);
    return null;
  }
}

// ─── EnerGov: fetch contacts for an EntityId ─────────────────────────────────
async function fetchContacts(entityId) {
  try {
    const res = await fetch(`${ENERGOV_BASE}/entity/contacts/search/search`, {
      method: 'POST',
      headers: ENERGOV_HEADERS,
      body: JSON.stringify({
        EntityId: entityId,
        ModuleId: 1,
        PageNumber: 1,
        PageSize: 50,
        SortField: '',
        IsSortedInAscendingOrder: true,
      }),
    });
    const data = await res.json();
    if (!data?.Result) return [];

    return data.Result
      .filter(c => {
        // Include all contractor-type contacts; filter out pure admin roles
        const t = c.ContactTypeName || '';
        if (CONTACT_TYPES_INCLUDE.has(t)) return true;
        // Also include anything with "Contractor" in the name not already listed
        if (t.toLowerCase().includes('contractor')) return true;
        return false;
      })
      .map(c => ({
        role: c.ContactTypeName || 'Contractor',
        company: c.GlobalEntityName || '',
        firstName: c.FirstName || '',
        lastName: c.LastName || '',
      }));
  } catch (e) {
    console.warn(`  ⚠ Contacts fetch failed for ${entityId}:`, e.message);
    return [];
  }
}

// ─── EnerGov: fetch sub-records for an EntityId ──────────────────────────────
async function fetchSubRecords(entityId) {
  try {
    const res = await fetch(`${ENERGOV_BASE}/entity/permits/search/search`, {
      method: 'POST',
      headers: ENERGOV_HEADERS,
      body: JSON.stringify({
        EntityId: entityId,
        IsExistingSubRecord: true,
        ModuleId: 1,
        PageNumber: 1,
        PageSize: 50,
        SortField: '',
        IsSortedInAscendingOrder: true,
        CaseTypeWorkClassList: null,
        OptionalSubRecordsCaseTypeWorkClassList: null,
      }),
    });
    const data = await res.json();
    if (!data?.Result) return [];

    return data.Result.map(r => ({
      recordNumber: r.RecordNumber || '',
      recordType: r.RecordType || '',
      recordStatus: r.RecordStatus || '',
      workClass: r.RecordWorkClass || '',
    }));
  } catch (e) {
    console.warn(`  ⚠ Sub-records fetch failed for ${entityId}:`, e.message);
    return [];
  }
}

// ─── Load existing subs cache ─────────────────────────────────────────────────
async function loadExistingSubs() {
  const subsPath = path.join(DATA_DIR, 'subs.json');
  if (!existsSync(subsPath)) return {};
  try {
    const raw = await readFile(subsPath, 'utf8');
    const parsed = JSON.parse(raw);
    return parsed.subs || {};
  } catch {
    return {};
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log('🏗  Raleigh Builder Intelligence — Nightly Data Fetch');
  console.log('='.repeat(55));

  // 1. Fetch ArcGIS permits
  console.log('\n📡 Fetching ArcGIS permits...');
  const [newPermits, renoPermits] = await Promise.all([
    fetchArcGIS('new'),
    fetchArcGIS('reno'),
  ]);

  const allPermits = [...newPermits, ...renoPermits];
  console.log(`✅ Total permits: ${allPermits.length} (${newPermits.length} new, ${renoPermits.length} reno)`);

  // 2. Save ArcGIS data
  await writeFile(
    path.join(DATA_DIR, 'permits-new.json'),
    JSON.stringify({ fetchedAt: new Date().toISOString(), mode: 'new', permitCount: newPermits.length, permits: newPermits }, null, 2)
  );
  await writeFile(
    path.join(DATA_DIR, 'permits-reno.json'),
    JSON.stringify({ fetchedAt: new Date().toISOString(), mode: 'reno', permitCount: renoPermits.length, permits: renoPermits }, null, 2)
  );
  console.log('✅ Saved permits-new.json and permits-reno.json');

  // 3. Fetch subcontractor data from EnerGov
  console.log('\n🔍 Fetching subcontractor data from EnerGov...');
  const existingSubs = await loadExistingSubs();
  console.log(`   Found ${Object.keys(existingSubs).length} permits already cached`);

  const subs = { ...existingSubs };
  let fetched = 0, skipped = 0, failed = 0;

  // Only fetch permits we don't have yet, or completed ones we might be updating
  const permitsToFetch = allPermits.filter(p => {
    const num = p.permitnum;
    if (!num) return false;
    // Always skip if already cached with data (contacts or subrecords present)
    if (subs[num] && (subs[num].contacts?.length > 0 || subs[num].subRecords?.length > 0)) {
      skipped++;
      return false;
    }
    return true;
  });

  console.log(`   Permits to fetch: ${permitsToFetch.length} (${skipped} already cached)`);

  for (let i = 0; i < permitsToFetch.length; i += BATCH_SIZE) {
    const batch = permitsToFetch.slice(i, i + BATCH_SIZE);

    await Promise.all(batch.map(async (permit) => {
      const num = permit.permitnum;
      try {
        // Step 1: resolve permit number → EntityId
        const entityId = await lookupEntityId(num);
        if (!entityId) {
          subs[num] = { contacts: [], subRecords: [], fetchedAt: new Date().toISOString(), notFound: true };
          failed++;
          return;
        }

        await delay(DELAY_MS);

        // Step 2: fetch contacts and sub-records in parallel
        const [contacts, subRecords] = await Promise.all([
          fetchContacts(entityId),
          fetchSubRecords(entityId),
        ]);

        subs[num] = {
          entityId,
          contacts,
          subRecords,
          fetchedAt: new Date().toISOString(),
        };
        fetched++;
      } catch (e) {
        console.warn(`  ⚠ Failed for ${num}:`, e.message);
        subs[num] = { contacts: [], subRecords: [], fetchedAt: new Date().toISOString(), error: true };
        failed++;
      }
    }));

    await delay(DELAY_MS * 2); // Extra pause between batches

    if ((i / BATCH_SIZE) % 10 === 0) {
      const pct = Math.round(((i + BATCH_SIZE) / permitsToFetch.length) * 100);
      console.log(`   Progress: ${Math.min(i + BATCH_SIZE, permitsToFetch.length)}/${permitsToFetch.length} (${pct}%) — fetched: ${fetched}, failed: ${failed}`);
    }
  }

  // 4. Save subs.json
  await writeFile(
    path.join(DATA_DIR, 'subs.json'),
    JSON.stringify({
      fetchedAt: new Date().toISOString(),
      permitCount: Object.keys(subs).length,
      subs,
    }, null, 2)
  );

  console.log('\n✅ Done!');
  console.log(`   Fetched: ${fetched} | Skipped (cached): ${skipped} | Failed: ${failed}`);
  console.log(`   Total in subs.json: ${Object.keys(subs).length}`);
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});