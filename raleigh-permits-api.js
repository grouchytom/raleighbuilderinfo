/**
 * raleigh-permits-api.js
 *
 * Field values confirmed from live record BLDR-012780-2023:
 *
 *   statuscurrent:       "Complete"           ← completed permits
 *   statuscurrentmapped: "Permit Finaled"     ← reliable completion signal
 *   coissueddate:        null                 ← NOT populated, do not use
 *   cocissueddate:       null                 ← NOT populated, do not use
 *   recordupdatedate:    epoch ms             ← use as proxy finalization date
 *   pin:                 "1706438749"         ← Wake County parcel PIN (populated)
 *   cntyacctnum:         null                 ← NOT populated, use pin instead
 *   parcelownername:     "CUSTOM ESTATE..."   ← current owner name (on permit!)
 *   parcelowneraddress1: "207 W MILLBROOK..." ← owner mailing address
 *   contractoremail:     populated            ← builder contact info available
 *   contractorphone:     populated            ← builder phone available
 *   constcompletedofficial: "No"/"Yes"        ← construction completion flag
 */

const PERMITS_FS =
  'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services' +
  '/Building_Permits/FeatureServer/0';

const CUTOFF_DATE = '2020-01-01';
const RESIDENTIAL_WORK_CLASSES = ['New'];
const RESIDENTIAL_PERMIT_CLASS = 'Residential';

const PERMIT_FIELDS = [
  'permitnum', 'workclass', 'workclassmapped',
  'permitclassmapped', 'statuscurrent', 'statuscurrentmapped',
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

const ZIP_TO_AREA = {
  '27601':'Downtown',     '27602':'Downtown',     '27603':'South',
  '27604':'East',         '27605':'Midtown',       '27606':'Southwest',
  '27607':'Midtown',      '27608':'Five Points',   '27609':'North Central',
  '27610':'Southeast',    '27611':'Downtown',      '27612':'Glenwood',
  '27613':'Northwest',    '27614':'North',         '27615':'Northwest',
  '27616':'Northeast',    '27617':'Northwest',     '27695':'NC State',
  '27513':'Cary',         '27518':'Cary',          '27519':'Cary',
  '27523':'Apex',         '27539':'Apex',          '27540':'Holly Springs',
  '27587':'Wake Forest',  '27588':'Wake Forest',   '27526':'Fuquay-Varina',
  '27529':'Garner',       '27560':'Morrisville',   '27703':'Durham/RTP',
};

const PAGE_SIZE = 2000;

/**
 * Completed = statuscurrentmapped is "Permit Finaled"
 * OR statuscurrent is "Complete" (confirmed from live data).
 * coissueddate is null in this dataset — do not rely on it.
 */
const COMPLETED_STATUS_MAPPED = new Set(['Permit Finaled']);
const COMPLETED_STATUS_CURRENT = new Set(['Complete', 'Closed', 'Final', 'CO Issued', 'CO Final']);

const ACTIVE_STATUSES = new Set([
  'Active', 'Issued', 'Permit Issued', 'In Review',
  'Under Review', 'Approved', 'Inspections In Progress', 'Pending',
  'Permit Issued', 'Inspections',
]);

/**
 * Quality flags — what we CAN detect from this dataset:
 *
 * The ArcGIS layer does NOT store inspection history. statuscurrent is
 * point-in-time only. Failed inspections are not visible once the permit
 * advances past that status.
 *
 * What we CAN flag:
 *   1. constcompletedofficial = "No" on a permit that was issued long ago
 *      (construction started but not officially complete)
 *   2. Very long time between issueddate and recordupdatedate (stalled builds)
 *   3. reviewercomments or perm_comments containing flag keywords
 *      (these persist even after status changes)
 */
const COMMENT_FLAG_KEYWORDS = [
  'resubmit', 're-submit', 'correction', 'deficiency',
  'rejected', 'failed', 'violation', 'non-compliant',
  'stop work', 'revise', 'incomplete',
];

const STALLED_DAYS_THRESHOLD = 365; // issued but no progress for > 1 year

// ---------------------------------------------------------------------------
// Deep links — all confirmed working
// ---------------------------------------------------------------------------

/** EnerGov self-service portal — confirmed URL pattern from user */
export function permitPortalURL(permitnum) {
  if (!permitnum) return 'https://raleighnc-energovpub.tylerhost.net/apps/selfservice#/search';
  return `https://raleighnc-energovpub.tylerhost.net/apps/selfservice` +
         `#/search?m=1&fm=1&ps=10&pn=1&em=true&st=${encodeURIComponent(permitnum)}`;
}

/**
 * Wake County Real Estate — use PIN (parcel ID) for direct lookup.
 * Confirmed PIN is populated (e.g. "1706438749").
 * URL pattern: services.wake.gov/realestate/Account.asp?id=REID&stype=pin
 *
 * For address-based fallback, use the pattern from user example:
 * https://services.wake.gov/realestate/Account.asp?id=0508431&stype=addr&stnum=1400&stname=Mordecai
 */
export function wakePropertyURL(pin, address) {
  if (pin && pin.trim()) {
    return `https://services.wake.gov/realestate/Account.asp` +
           `?id=${encodeURIComponent(pin.trim())}&stype=pin`;
  }
  if (address) {
    const parts = address.trim().split(/\s+/);
    const stnum  = parts[0] || '';
    const stname = parts[1] || '';
    return `https://services.wake.gov/realestate/Account.asp` +
           `?stype=addr&stnum=${encodeURIComponent(stnum)}&stname=${encodeURIComponent(stname)}`;
  }
  return 'https://services.wake.gov/realestate/';
}

/** NC License Board — confirmed URL from user */
export function ncLicenseURL(licNum, companyName) {
  // The NCLBGC verify license portal — search by license number or company name
  return `https://portal.nclbgc.org/Public/Search`;
}

/** NC Secretary of State business search — confirmed URL from user */
export function ncSOSURL(companyName) {
  return `https://www.sosnc.gov/online_services/search/by_title/_Business_RegistrationNorth%20Carolina`;
}

/**
 * NC Courts / legal lookup — confirmed URL from user.
 * Covers civil cases, mechanics liens (special proceedings), and judgments.
 * More comprehensive than Wake County ROD for legal research.
 */
export function ncCourtsURL(companyName) {
  return `https://portal-nc.tylertech.cloud/Portal/Home/Dashboard/29`;
}


// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

function buildWhereClause() {
  const wc = RESIDENTIAL_WORK_CLASSES.map(w => `workclassmapped = '${w}'`).join(' OR ');
  return `(${wc}) AND permitclassmapped = '${RESIDENTIAL_PERMIT_CLASS}'` +
         ` AND applieddate >= DATE '${CUTOFF_DATE}'`;
}

async function fetchPermitPage(offset, signal) {
  const params = new URLSearchParams({
    where: buildWhereClause(), outFields: PERMIT_FIELDS,
    returnGeometry: 'false', resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    orderByFields: 'applieddate DESC', f: 'json',
  });
  let res;
  try {
    res = await fetch(`${PERMITS_FS}/query?${params}`, { mode: 'cors', signal });
  } catch(e) {
    throw new Error(`Network error — run via python3 -m http.server 8000\nDetail: ${e.message}`);
  }
  if (!res.ok) throw new Error(`ArcGIS HTTP ${res.status}`);
  const json = await res.json();
  if (json.error) throw new Error(`ArcGIS ${json.error.code}: ${json.error.message}`);
  return { features: json.features||[], exceededTransferLimit: json.exceededTransferLimit??false };
}

async function fetchAllPermits({ onProgress, signal } = {}) {
  const all = [];
  let offset = 0, hasMore = true, page = 0;
  while (hasMore) {
    const { features, exceededTransferLimit } = await fetchPermitPage(offset, signal);
    all.push(...features.map(f => f.attributes));
    hasMore = exceededTransferLimit && features.length === PAGE_SIZE;
    offset += features.length; page++;
    onProgress?.({ stage:'permits', message:`Fetched ${all.length} permits…`, pct:null });
    if (hasMore) await sleep(150);
  }
  return all;
}

export async function probeFields() {
  console.group('ArcGIS probe');
  const o = { mode:'cors' };
  const b = `${PERMITS_FS}/query`;

  const sc = await (await fetch(`${b}?where=1%3D1&outFields=statuscurrent,statuscurrentmapped&returnDistinctValues=true&returnGeometry=false&orderByFields=statuscurrent&f=json`, o)).json();
  console.log('statuscurrent values:', sc.features?.map(f => ({
    current: f.attributes.statuscurrent,
    mapped:  f.attributes.statuscurrentmapped,
  })));

  // Sample a "Complete" permit to verify date fields
  const comp = await (await fetch(`${b}?where=statuscurrentmapped%3D'Permit+Finaled'&outFields=permitnum,statuscurrent,statuscurrentmapped,applieddate,issueddate,recordupdatedate,coissueddate,cocissueddate,pin,cntyacctnum&returnGeometry=false&resultRecordCount=3&f=json`, o)).json();
  console.log('Completed permit samples:', comp.features?.map(f => f.attributes));

  console.log('WHERE clause:', buildWhereClause());
  console.groupEnd();
}

// ---------------------------------------------------------------------------
// Tag permit — quality detection based on what's actually available
// ---------------------------------------------------------------------------

function detectQualityFlag(p) {
  const reasons = [];

  // Check comments for flag keywords (these persist after status changes)
  const comments = [p.reviewercomments, p.perm_comments, p.description]
    .filter(Boolean).join(' ').toLowerCase();
  for (const kw of COMMENT_FLAG_KEYWORDS) {
    if (comments.includes(kw)) { reasons.push(`comment: "${kw}"`); break; }
  }

  // Slow build: completed but took longer than 19 months (575 days)
  if (p.isCompleted && p.buildDays && p.buildDays > 575) {
    const months = Math.round(p.buildDays / 30.4);
    reasons.push(`slow build: ${months} months to complete (threshold: 19 months)`);
  }

  // Stalled active build: issued > 1 year ago, not complete by any signal
  const { completed: alreadyComplete } = detectCompletion(p);
  if (!alreadyComplete && p.constcompletedofficial === 'No' && p.issueddate && ACTIVE_STATUSES.has(p.statuscurrent)) {
    const daysSinceIssued = Math.floor((Date.now() - p.issueddate) / 86_400_000);
    if (daysSinceIssued > STALLED_DAYS_THRESHOLD) {
      reasons.push(`stalled: issued ${Math.round(daysSinceIssued/365*10)/10}y ago, construction not complete`);
    }
  }

  return reasons;
}

/**
 * Multi-signal completion detection.
 *
 * NOTE: parcelownername in the ArcGIS dataset is captured at permit filing time
 * and is NOT updated when the house sells. It is NOT a reliable signal for
 * current ownership or completion status. We use three other signals instead:
 *
 *   1. statuscurrentmapped = "Permit Finaled"
 *   2. statuscurrent = "Complete" / "Closed" / "Final" / "CO Issued" / "CO Final"
 *   3. constcompletedofficial = "Yes"
 *
 * For current ownership, always link to Wake County real estate (PIN-based).
 */
function detectCompletion(p) {
  if (COMPLETED_STATUS_MAPPED.has(p.statuscurrentmapped)) {
    return { completed: true, signal: 'Permit Finaled' };
  }
  if (COMPLETED_STATUS_CURRENT.has(p.statuscurrent)) {
    return { completed: true, signal: p.statuscurrent };
  }
  if (p.constcompletedofficial === 'Yes') {
    return { completed: true, signal: 'Construction complete (official)' };
  }
  return { completed: false, signal: null };
}

function tagPermit(p) {
  p.zip5 = String(p.originalzip || '').slice(0, 5);
  p.area = ZIP_TO_AREA[p.zip5] || 'Other';

  // Completion detection — multi-signal
  const { completed, signal } = detectCompletion(p);
  p.isCompleted      = completed;
  p.completionSignal = signal;

  // Build time: recordupdatedate - applieddate.
  // coissueddate is null in this dataset.
  // recordupdatedate is the last record touch — for completed permits
  // this approximates when the work stopped being tracked.
  if (p.isCompleted && p.applieddate && p.recordupdatedate) {
    const days = Math.floor((p.recordupdatedate - p.applieddate) / 86_400_000);
    p.buildDays = (days >= 30 && days <= 1500) ? days : null;
  } else {
    p.buildDays = null;
  }

  // Quality flags — run AFTER buildDays is set (slow build check needs it)
  const flagReasons = detectQualityFlag(p);
  p.isQualityFlag = flagReasons.length > 0;
  p.qualityReasons = flagReasons;

  return p;
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

export function aggregateBuilders(permits) {
  const map = new Map();

  for (const p of permits) {
    const name = (p.contractorcompanyname || 'Unknown').trim();
    if (!map.has(name)) {
      map.set(name, {
        name,
        licNum:      p.contractorlicnum || p.statelicnum || null,
        email:       p.contractoremail || null,
        phone:       p.contractorphone || null,
        addr:        [p.contractoraddress1, p.contractorcity, p.contractorstate]
                       .filter(Boolean).join(', ') || null,
        permits:     [],
        areas:       new Set(),
        zips:        new Set(),
        earliestApplied: null,
      });
    }
    const b = map.get(name);
    b.permits.push(p);
    if (p.area) b.areas.add(p.area);
    if (p.zip5) b.zips.add(p.zip5);
    // Earliest applied date = how long active in dataset
    if (p.applieddate && (!b.earliestApplied || p.applieddate < b.earliestApplied)) {
      b.earliestApplied = p.applieddate;
    }
    // Pick up contact info from first record that has it
    if (!b.email && p.contractoremail) b.email = p.contractoremail;
    if (!b.phone && p.contractorphone) b.phone = p.contractorphone;
    if (!b.addr  && p.contractoraddress1) {
      b.addr = [p.contractoraddress1, p.contractorcity, p.contractorstate].filter(Boolean).join(', ');
    }
  }

  return [...map.values()].map(b => {
    const ps        = b.permits;
    const completed = ps.filter(p => p.isCompleted);
    const buildTimes= completed.map(p => p.buildDays).filter(d => d !== null);

    const avgBuildDays = buildTimes.length
      ? Math.round(buildTimes.reduce((s,v)=>s+v,0) / buildTimes.length)
      : null;

    const completionRate = ps.length
      ? Math.round(completed.length / ps.length * 100) : 0;

    const vals = ps.filter(p=>p.estprojectcost).map(p=>p.estprojectcost);
    const avgValue = vals.length
      ? Math.round(vals.reduce((s,v)=>s+v,0)/vals.length) : 0;

    const qualityFlags = ps.filter(p => p.isQualityFlag);
    const qualityFlagRate = ps.length
      ? Math.round(qualityFlags.length / ps.length * 100) : 0;

    const yearsActive = b.earliestApplied
      ? Math.max(0.1, Math.round((Date.now() - b.earliestApplied) / (365.25*86_400_000) * 10) / 10)
      : null;

    const statusCounts = {};
    for (const p of ps) {
      const st = p.statuscurrent || 'Unknown';
      statusCounts[st] = (statusCounts[st]||0) + 1;
    }

    return {
      name: b.name, licNum: b.licNum, email: b.email, phone: b.phone, addr: b.addr,
      projects: ps.length,
      active:   ps.filter(p => ACTIVE_STATUSES.has(p.statuscurrent)).length,
      completed: completed.length,
      completionRate, avgBuildDays, avgValue,
      qualityFlagCount: qualityFlags.length, qualityFlagRate,
      yearsActive, areas: [...b.areas].sort(), zips: [...b.zips].sort(),
      statusCounts, permits: ps,
    };
  }).sort((a,b) => b.projects - a.projects);
}

export function filterBuilders(allPermits, { area, zip, status, search } = {}) {
  const q = search ? search.toLowerCase().trim() : '';

  // If search looks like an address (starts with a number), filter permits
  // by address and return those builders — showing only matching projects.
  const isAddressSearch = q && /^\d/.test(q);

  let filtered = allPermits.filter(p => {
    if (area   && p.area          !== area)   return false;
    if (zip    && p.zip5          !== zip)    return false;
    if (status && p.statuscurrent !== status) return false;
    if (isAddressSearch) {
      return (p.originaladdress1 || '').toLowerCase().includes(q);
    }
    return true;
  });

  let builders = aggregateBuilders(filtered);

  // Builder name search (only when not an address search)
  if (q && !isAddressSearch) {
    builders = builders.filter(b => b.name.toLowerCase().includes(q));
  }

  return builders;
}

export async function loadDashboardData({ onProgress, signal } = {}) {
  onProgress?.({ stage:'permits', message:'Connecting to ArcGIS…', pct:0 });
  const rawPermits = await fetchAllPermits({ onProgress, signal });
  onProgress?.({ stage:'permits', message:`Tagging ${rawPermits.length} permits…`, pct:0.95 });
  const permits = rawPermits.map(tagPermit);
  onProgress?.({ stage:'aggregating', message:'Aggregating builders…', pct:null });
  const builders = aggregateBuilders(permits);
  onProgress?.({ stage:'done', message:`Ready — ${builders.length} builders, ${permits.length} permits.`, pct:1 });
  return { builders, permits };
}

/**
 * loadFromCache()
 *
 * Loads pre-fetched permit data from data/permits.json instead of
 * hitting the ArcGIS API live. Returns the same { builders, permits }
 * shape as loadDashboardData().
 *
 * Falls back to loadDashboardData() if the cache file doesn't exist
 * or is stale (older than 48 hours).
 *
 * @param {object} opts
 * @param {Function} [opts.onProgress]
 * @returns {Promise<{builders, permits, fromCache: boolean, cachedAt: string|null}>}
 */
export async function loadFromCacheOrLive({ onProgress, signal } = {}) {
  onProgress?.({ stage:'permits', message:'Loading permit data…', pct:0 });

  // Try the cache first
  try {
    const res = await fetch('./data/permits.json');
    if (res.ok) {
      const json = await res.json();

      // Check freshness — fall through to live if cache is older than 48h
      const cachedAt  = new Date(json.fetchedAt);
      const ageHours  = (Date.now() - cachedAt.getTime()) / 3_600_000;

      if (ageHours < 48 && Array.isArray(json.permits) && json.permits.length > 0) {
        onProgress?.({ stage:'permits', message:`Loaded ${json.permits.length.toLocaleString()} permits from cache…`, pct:0.9 });
        const permits  = json.permits.map(tagPermit);
        onProgress?.({ stage:'aggregating', message:'Aggregating builders…', pct:null });
        const builders = aggregateBuilders(permits);
        onProgress?.({ stage:'done', message:`Ready — ${builders.length} builders, ${permits.length} permits.`, pct:1 });
        return { builders, permits, fromCache: true, cachedAt: json.fetchedAt };
      }
    }
  } catch {
    // Cache unavailable — fall through to live fetch
  }

  // Cache miss or stale — fetch live from ArcGIS
  onProgress?.({ stage:'permits', message:'Cache unavailable — connecting to ArcGIS…', pct:0 });
  const { builders, permits } = await loadDashboardData({ onProgress, signal });
  return { builders, permits, fromCache: false, cachedAt: null };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
