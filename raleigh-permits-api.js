/**
 * raleigh-permits-api.js
 *
 * Confirmed field values (live data 2026-04-10):
 *   statuscurrent:       "Complete" / "Issued" etc.
 *   statuscurrentmapped: "Permit Finaled" = completed
 *   workclassmapped:     "New" = new construction | "Existing" = renovation
 *   coissueddate:        null in this dataset — use recordupdatedate as proxy
 *   pin:                 populated (Wake County parcel ID)
 */

const PERMITS_FS =
  'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services' +
  '/Building_Permits/FeatureServer/0';

const CUTOFF_DATE = '2020-01-01';

// Renovation workclass values to EXCLUDE — not useful for contractor research
const RENO_EXCLUDE_WORKCLASS = [
  'Demolish', 'NON-CONSTRUCTION INSPECTION', 'Change Of Use',
  'Foundation Only', 'Specialty', 'Other', 'Other - Comm', 'Other - Resd',
  'Mobile Home Original', 'Mobile Home Replacement',
  'Manufactured Home Original', 'Manufactured Home Repairs',
  'Manufactured Home Replacement', 'Manufactured Home Original',
  'Swimming Pool - Commercial',
];

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

export const STATUS_WEIGHTS = {
  'Application Accepted':5, 'Pending':10, 'Under Review':20, 'In Review':20,
  'Approved':40, 'Issued':55, 'Permit Issued':55, 'Active':60,
  'Inspections In Progress':75, 'CO Issued':100, 'CO Final':100,
  'Final':100, 'Closed':100, 'Expired':50, 'Withdrawn':0,
};

const ACTIVE_STATUSES = new Set([
  'Active','Issued','Permit Issued','In Review',
  'Under Review','Approved','Inspections In Progress','Pending',
]);
const COMPLETED_STATUS_MAPPED  = new Set(['Permit Finaled']);
const COMPLETED_STATUS_CURRENT = new Set(['Complete','Closed','Final','CO Issued','CO Final']);
const STALLED_DAYS_THRESHOLD   = 365;
const COMMENT_FLAG_KEYWORDS    = [
  'resubmit','re-submit','correction','deficiency',
  'rejected','failed','violation','non-compliant',
  'stop work','revise','incomplete',
];

// ---------------------------------------------------------------------------
// WHERE clause — mode aware
// ---------------------------------------------------------------------------

/**
 * mode: 'new'  → new construction (workclassmapped = 'New')
 * mode: 'reno' → renovation/existing (workclassmapped = 'Existing')
 * mode: 'all'  → both (used by Map Search)
 */
export function buildWhereClause(mode = 'new') {
  const base = `permitclassmapped = 'Residential' AND applieddate >= DATE '${CUTOFF_DATE}'`;
  if (mode === 'new')  return `workclassmapped = 'New' AND ${base}`;
  if (mode === 'reno') return `workclassmapped = 'Existing' AND ${base}`;
  return `workclassmapped IN ('New','Existing') AND ${base}`;
}

// ---------------------------------------------------------------------------
// Deep links
// ---------------------------------------------------------------------------

export function permitPortalURL(permitnum) {
  if (!permitnum) return 'https://raleighnc-energovpub.tylerhost.net/apps/selfservice#/search';
  return `https://raleighnc-energovpub.tylerhost.net/apps/selfservice` +
         `#/search?m=1&fm=1&ps=10&pn=1&em=true&st=${encodeURIComponent(permitnum)}`;
}

export function wakePropertyURL(pin, address) {
  if (pin?.trim()) {
    return `https://services.wake.gov/realestate/Account.asp?id=${encodeURIComponent(pin.trim())}&stype=pin`;
  }
  if (address) {
    const parts = address.trim().split(/\s+/);
    return `https://services.wake.gov/realestate/Account.asp` +
           `?stype=addr&stnum=${encodeURIComponent(parts[0]||'')}&stname=${encodeURIComponent(parts[1]||'')}`;
  }
  return 'https://services.wake.gov/realestate/';
}

export function ncLicenseURL(licNum, companyName) {
  return `https://portal.nclbgc.org/Public/Search`;
}
export function ncSOSURL(companyName) {
  return `https://www.sosnc.gov/online_services/search/by_title/_Business_RegistrationNorth%20Carolina`;
}
export function ncCourtsURL(companyName) {
  return `https://portal-nc.tylertech.cloud/Portal/Home/Dashboard/29`;
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

async function fetchPage(offset, mode, signal) {
  const params = new URLSearchParams({
    where:             buildWhereClause(mode),
    outFields:         PERMIT_FIELDS,
    returnGeometry:    'false',
    resultOffset:      String(offset),
    resultRecordCount: String(PAGE_SIZE),
    orderByFields:     'applieddate DESC',
    f:                 'json',
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

async function fetchAllPermits({ onProgress, signal, mode = 'new' } = {}) {
  const all = [];
  let offset = 0, hasMore = true, page = 0;
  while (hasMore) {
    const { features, exceededTransferLimit } = await fetchPage(offset, mode, signal);
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
  const wc = await (await fetch(`${b}?where=permitclassmapped%3D'Residential'&outFields=workclassmapped,workclass&returnDistinctValues=true&returnGeometry=false&orderByFields=workclassmapped&f=json`, o)).json();
  console.log('All residential work classes:', wc.features?.map(f=>({mapped:f.attributes.workclassmapped,raw:f.attributes.workclass})));
  const sc = await (await fetch(`${b}?where=1%3D1&outFields=statuscurrent&returnDistinctValues=true&returnGeometry=false&orderByFields=statuscurrent&f=json`, o)).json();
  console.log('statuscurrent values:', sc.features?.map(f=>f.attributes.statuscurrent));
  console.groupEnd();
}

// ---------------------------------------------------------------------------
// Tag permit
// ---------------------------------------------------------------------------

function detectCompletion(p) {
  if (COMPLETED_STATUS_MAPPED.has(p.statuscurrentmapped))
    return { completed: true, signal: 'Permit Finaled' };
  if (COMPLETED_STATUS_CURRENT.has(p.statuscurrent))
    return { completed: true, signal: p.statuscurrent };
  if (p.constcompletedofficial === 'Yes')
    return { completed: true, signal: 'Construction complete (official)' };
  return { completed: false, signal: null };
}

function detectQualityFlag(p) {
  const reasons = [];
  const comments = [p.reviewercomments, p.perm_comments, p.description]
    .filter(Boolean).join(' ').toLowerCase();
  for (const kw of COMMENT_FLAG_KEYWORDS) {
    if (comments.includes(kw)) { reasons.push(`comment: "${kw}"`); break; }
  }
  // Slow build: >19 months for new construction, >24 months for renovations
  const slowThreshold = p.workclassmapped === 'New' ? 575 : 730;
  if (p.isCompleted && p.buildDays && p.buildDays > slowThreshold) {
    const months = Math.round(p.buildDays / 30.4);
    const label  = p.workclassmapped === 'New' ? '19' : '24';
    reasons.push(`slow build: ${months} months (threshold: ${label} months)`);
  }
  const { completed: alreadyComplete } = detectCompletion(p);
  if (!alreadyComplete && p.constcompletedofficial === 'No' && p.issueddate && ACTIVE_STATUSES.has(p.statuscurrent)) {
    const daysSinceIssued = Math.floor((Date.now() - p.issueddate) / 86_400_000);
    if (daysSinceIssued > STALLED_DAYS_THRESHOLD) {
      reasons.push(`stalled: issued ${Math.round(daysSinceIssued/365*10)/10}y ago`);
    }
  }
  return reasons;
}

export function tagPermit(p) {
  p.zip5 = String(p.originalzip || '').slice(0, 5);
  p.area = ZIP_TO_AREA[p.zip5] || 'Other';
  p.permitMode = p.workclassmapped === 'New' ? 'new' : 'reno';

  // Filter out renovation noise
  if (p.permitMode === 'reno' && p.workclass &&
      RENO_EXCLUDE_WORKCLASS.some(ex => p.workclass.toLowerCase().includes(ex.toLowerCase()))) {
    p._excluded = true;
    return p;
  }

  const { completed, signal } = detectCompletion(p);
  p.isCompleted      = completed;
  p.completionSignal = signal;

  if (p.isCompleted && p.applieddate && p.recordupdatedate) {
    const days = Math.floor((p.recordupdatedate - p.applieddate) / 86_400_000);
    p.buildDays = (days >= 14 && days <= 1825) ? days : null;
  } else {
    p.buildDays = null;
  }

  // Permits in last 12 months
  p.isLast12Months = p.applieddate
    ? p.applieddate >= Date.now() - 365 * 86_400_000
    : false;

  const flagReasons = detectQualityFlag(p);
  p.isQualityFlag  = flagReasons.length > 0;
  p.qualityReasons = flagReasons;

  return p;
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

function daysSince(epoch) {
  if (!epoch) return 0;
  return Math.max(0, Math.floor((Date.now() - epoch) / 86_400_000));
}

export function aggregateBuilders(permits) {
  const map = new Map();

  for (const p of permits) {
    if (p._excluded) continue;
    const name = (p.contractorcompanyname || 'Unknown').trim();
    if (!map.has(name)) {
      map.set(name, {
        name, licNum: p.contractorlicnum || p.statelicnum || null,
        email: null, phone: null, addr: null,
        permits:[], areas:new Set(), zips:new Set(), earliestApplied:null,
      });
    }
    const b = map.get(name);
    b.permits.push(p);
    if (p.area) b.areas.add(p.area);
    if (p.zip5) b.zips.add(p.zip5);
    if (p.applieddate && (!b.earliestApplied || p.applieddate < b.earliestApplied))
      b.earliestApplied = p.applieddate;
    if (!b.email && p.contractoremail) b.email = p.contractoremail;
    if (!b.phone && p.contractorphone) b.phone = p.contractorphone;
    if (!b.addr && p.contractoraddress1)
      b.addr = [p.contractoraddress1, p.contractorcity, p.contractorstate].filter(Boolean).join(', ');
  }

  return [...map.values()].map(b => {
    const ps        = b.permits;
    const completed = ps.filter(p => p.isCompleted);
    const buildTimes= completed.map(p => p.buildDays).filter(d => d !== null);
    const avgBuildDays = buildTimes.length
      ? Math.round(buildTimes.reduce((s,v)=>s+v,0) / buildTimes.length) : null;
    const completionRate = ps.length
      ? Math.round(completed.length / ps.length * 100) : 0;
    const vals = ps.filter(p=>p.estprojectcost).map(p=>p.estprojectcost);
    const avgValue = vals.length
      ? Math.round(vals.reduce((s,v)=>s+v,0)/vals.length) : 0;
    const qualityFlags   = ps.filter(p => p.isQualityFlag);
    const qualityFlagRate= ps.length
      ? Math.round(qualityFlags.length / ps.length * 100) : 0;
    const last12 = ps.filter(p => p.isLast12Months).length;
    const yearsActive = b.earliestApplied
      ? Math.max(0.1, Math.round((Date.now() - b.earliestApplied) / (365.25*86_400_000) * 10) / 10)
      : null;
    const statusCounts = {};
    for (const p of ps) {
      const st = p.statuscurrent || 'Unknown';
      statusCounts[st] = (statusCounts[st]||0) + 1;
    }
    // Work type breakdown for renovation mode
    const workTypes = {};
    for (const p of ps) {
      const wt = p.workclass || 'Unknown';
      workTypes[wt] = (workTypes[wt]||0) + 1;
    }

    return {
      name: b.name, licNum: b.licNum, email: b.email, phone: b.phone, addr: b.addr,
      projects: ps.length, active: ps.filter(p => ACTIVE_STATUSES.has(p.statuscurrent)).length,
      completed: completed.length, completionRate, avgBuildDays, avgValue,
      qualityFlagCount: qualityFlags.length, qualityFlagRate,
      last12,   // ← permits in last 12 months
      yearsActive, areas: [...b.areas].sort(), zips: [...b.zips].sort(),
      statusCounts, workTypes, permits: ps,
    };
  }).sort((a,b) => b.projects - a.projects);
}

export function filterBuilders(allPermits, { area, zip, status, search } = {}) {
  const q       = search ? search.toLowerCase().trim() : '';
  const isAddr  = q && /^\d/.test(q);
  const filtered = allPermits.filter(p => {
    if (p._excluded) return false;
    if (area   && p.area          !== area)   return false;
    if (zip    && String(p.zip5||'') !== zip) return false;
    if (status && p.statuscurrent !== status) return false;
    if (isAddr  && !(p.originaladdress1||'').toLowerCase().includes(q)) return false;
    if (!isAddr && q && !(p.contractorcompanyname||'').toLowerCase().includes(q)) return false;
    return true;
  });
  return aggregateBuilders(filtered);
}

// ---------------------------------------------------------------------------
// Load functions
// ---------------------------------------------------------------------------

export async function loadDashboardData({ onProgress, signal, mode = 'new' } = {}) {
  onProgress?.({ stage:'permits', message:'Connecting to ArcGIS…', pct:0 });
  const raw     = await fetchAllPermits({ onProgress, signal, mode });
  const permits = raw.map(tagPermit).filter(p => !p._excluded);
  onProgress?.({ stage:'aggregating', message:'Aggregating builders…', pct:null });
  const builders = aggregateBuilders(permits);
  onProgress?.({ stage:'done', message:`Ready — ${builders.length} builders, ${permits.length} permits.`, pct:1 });
  return { builders, permits };
}

export async function loadFromCacheOrLive({ onProgress, signal, mode = 'new' } = {}) {
  onProgress?.({ stage:'permits', message:'Loading permit data…', pct:0 });
  const cacheFile = mode === 'reno' ? './data/permits-reno.json' : './data/permits-new.json';

  try {
    const res = await fetch(cacheFile);
    if (res.ok) {
      const json     = await res.json();
      const cachedAt = new Date(json.fetchedAt);
      const ageHours = (Date.now() - cachedAt.getTime()) / 3_600_000;
      if (ageHours < 48 && Array.isArray(json.permits) && json.permits.length > 0) {
        onProgress?.({ stage:'permits', message:`Loaded ${json.permits.length.toLocaleString()} permits from cache…`, pct:0.9 });
        const permits  = json.permits.map(tagPermit).filter(p => !p._excluded);
        const builders = aggregateBuilders(permits);
        onProgress?.({ stage:'done', message:`Ready — ${builders.length} builders, ${permits.length} permits.`, pct:1 });
        return { builders, permits, fromCache: true, cachedAt: json.fetchedAt, mode };
      }
    }
  } catch { /* fall through */ }

  onProgress?.({ stage:'permits', message:'Cache unavailable — connecting to ArcGIS…', pct:0 });
  const { builders, permits } = await loadDashboardData({ onProgress, signal, mode });
  return { builders, permits, fromCache: false, cachedAt: null, mode };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
