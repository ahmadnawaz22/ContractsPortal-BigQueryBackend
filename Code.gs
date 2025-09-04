/******************************
 * Code.gs  (BigQuery-backed)
 ******************************/

/***** BIGQUERY CONFIG *****/
const BQ_PROJECT_ID = 'masterdata-470911';   // <-- set me (e.g., 'masterdata-470911')
const BQ_DATASET    = 'master_data';      // <-- set me (e.g., 'master_data')
// If you know the exact region (e.g., 'EU', 'US', or a regional location like 'europe-west2'), set it.
// Otherwise leave blank and the code will auto-detect & cache the dataset location.
const BQ_LOCATION   = '';

/***** SOURCE TABLES *****/
const BQ_TABLE_CONTRACTS = 'ContractComponents'; // Orderform data

/***** BUSINESS CONSTANTS *****/
const REF_EXCLUDE_TEXT = 'Unsigned Prospect Contract'; // include in Renewals; exclude elsewhere

/***** UTILITIES *****/
function _asISO_(v) {
  if (!v) return '';
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v)) {
    return Utilities.formatDate(v, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }
  return String(v);
}
function _parseDateKey_(v) {
  if (!v) return Number.POSITIVE_INFINITY;
  const d = new Date(v);
  return isNaN(d) ? Number.POSITIVE_INFINITY : d.getTime();
}

/***** BIGQUERY LOCATION (auto-detect & cache) *****/
function _getBQLocation_() {
  // 1) Explicit config (if provided)
  if (BQ_LOCATION && String(BQ_LOCATION).trim()) return String(BQ_LOCATION).trim();

  // 2) Cached
  const key = 'BQ_LOC:' + BQ_PROJECT_ID + '.' + BQ_DATASET;
  const props = PropertiesService.getScriptProperties();
  const cached = props.getProperty(key);
  if (cached) return cached;

  // 3) Fast path
  try {
    const ds = BigQuery.Datasets.get(BQ_PROJECT_ID, BQ_DATASET);
    if (ds && ds.location) {
      props.setProperty(key, ds.location);
      return ds.location;
    }
  } catch (_e) {}

  // 4) List & match
  try {
    const list = BigQuery.Datasets.list(BQ_PROJECT_ID);
    if (list && list.datasets && list.datasets.length) {
      const hit = list.datasets.find(d => d.datasetReference && d.datasetReference.datasetId === BQ_DATASET);
      if (hit && hit.location) {
        props.setProperty(key, hit.location);
        return hit.location;
      }
    }
  } catch (_e) {}

  // 5) Dry-run to learn location
  try {
    const job = {
      configuration: {
        query: {
          query: 'SELECT 1',
          useLegacySql: false,
          defaultDataset: { projectId: BQ_PROJECT_ID, datasetId: BQ_DATASET },
          dryRun: true
        }
      }
    };
    const res = BigQuery.Jobs.insert(job, BQ_PROJECT_ID);
    const loc = res && res.jobReference && res.jobReference.location;
    if (loc) {
      props.setProperty(key, loc);
      return loc;
    }
  } catch (_e) {}

  // 6) Unknown; letting BigQuery default (often 'US') may error if dataset is elsewhere
  return '';
}

/***** BIGQUERY HELPER *****/
function bqQuery_(sql, params) {
  const req = {
    query: sql,
    useLegacySql: false
  };
  const loc = _getBQLocation_();
  if (loc) req.location = loc;

  if (params && typeof params === 'object') {
    req.parameterMode = 'NAMED';
    req.queryParameters = [];
    for (var k in params) {
      if (!Object.prototype.hasOwnProperty.call(params, k)) continue;
      var v = params[k];
      var type = 'STRING';
      if (typeof v === 'number') type = (Math.floor(v) === v ? 'INT64' : 'FLOAT64');
      else if (typeof v === 'boolean') type = 'BOOL';
      req.queryParameters.push({
        name: k,
        parameterType: { type: type },
        parameterValue: { value: String(v) }
      });
    }
  }

  const res = BigQuery.Jobs.query(req, BQ_PROJECT_ID);
  if (!res || res.jobComplete !== true) throw new Error('BigQuery job did not complete.');

  const rows = res.rows || [];
  const fields = (res.schema && res.schema.fields) || [];
  return rows.map(r => {
    const obj = {};
    for (let i = 0; i < r.f.length; i++) obj[fields[i].name] = r.f[i].v;
    return obj;
  });
}

/***** CONTRACTS BASE SELECT (field mapping) *****/
function _contractsSelectSQL_() {
  // ContractComponents → UI schema
  return `
    SELECT
      ContractcomponentID AS OrderformCode,
      CustomerName,
      Description         AS Ref,
      StartDate,
      EndDate,
      SubProduct,
      PaymentTerm,
      LicenseOrdered,
      Price,
      Amount,
      Comments,
      ClientID,
      AccountManager
    FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE_CONTRACTS}\`
  `;
}

/***** PUBLIC: Customers bootstrap (exclude renewals) *****/
function bootstrapCustomers() {
  const phrase = '%' + REF_EXCLUDE_TEXT.toLowerCase() + '%';
  const sql = `
    SELECT DISTINCT CustomerName
    FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE_CONTRACTS}\`
    WHERE COALESCE(LOWER(Description), '') NOT LIKE @phrase
      AND CustomerName IS NOT NULL AND CustomerName <> ''
    ORDER BY CustomerName
  `;
  const data = bqQuery_(sql, { phrase });
  return { customers: data.map(r => String(r.CustomerName)) };
}

/***** PUBLIC: All customers grouped (exclude renewals) *****/
function getAllCustomerContracts() {
  const phrase = '%' + REF_EXCLUDE_TEXT.toLowerCase() + '%';
  const sql = _contractsSelectSQL_() + `
    WHERE COALESCE(LOWER(Description), '') NOT LIKE @phrase
  `;
  const rows = bqQuery_(sql, { phrase });

  const byCustomer = new Map();
  rows.forEach(r => {
    const cust = String(r.CustomerName || '').trim();
    if (!cust) return;
    if (!byCustomer.has(cust)) byCustomer.set(cust, []);
    byCustomer.get(cust).push(r);
  });

  const groups = [];
  byCustomer.forEach((rs, customerName) => {
    const byContract = new Map();
    const clientIds = new Set();

    rs.forEach(r => {
      const code = String(r.OrderformCode || '').trim();
      if (!byContract.has(code)) {
        byContract.set(code, {
          orderformCode: code,
          ref: r.Ref || '',
          startDate: r.StartDate || '',
          endDate: r.EndDate || '',
          paymentTerm: r.PaymentTerm || '',
          lines: [],
          totalAmount: 0
        });
      }
      const line = {
        subProduct: r.SubProduct || '',
        comments: r.Comments || '',
        licenseOrdered: Number(r.LicenseOrdered || 0),
        price: Number(r.Price || 0),
        amount: Number(r.Amount || 0),
        accountManager: r.AccountManager || ''
      };
      const obj = byContract.get(code);
      obj.lines.push(line);
      obj.totalAmount += line.amount;

      const cid = String(r.ClientID || '').trim();
      if (cid) clientIds.add(cid);
    });

    const contracts = Array.from(byContract.values())
      .map(c => ({ ...c, startDate: _asISO_(c.startDate), endDate: _asISO_(c.endDate) }))
      .sort((a, b) => {
        const ak = _parseDateKey_(a.startDate), bk = _parseDateKey_(b.startDate);
        if (ak !== bk) return ak - bk;
        return (a.orderformCode || '').localeCompare(b.orderformCode || '');
      });

    groups.push({
      customer: customerName,
      clientId: Array.from(clientIds).join(', '),
      contractCount: contracts.length,
      contracts
    });
  });

  groups.sort((a, b) => a.customer.localeCompare(b.customer));
  return { groups, totalGroups: groups.length };
}

/***** PUBLIC: One customer (exclude renewals) *****/
function getCustomerContracts(customerName) {
  const phrase = '%' + REF_EXCLUDE_TEXT.toLowerCase() + '%';
  const sql = _contractsSelectSQL_() + `
    WHERE COALESCE(LOWER(Description), '') NOT LIKE @phrase
      AND CustomerName = @customer
  `;
  const rows = bqQuery_(sql, { phrase, customer: String(customerName || '') });

  const byContract = new Map();
  const clientIds = new Set();

  rows.forEach(r => {
    const code = String(r.OrderformCode || '').trim();
    if (!byContract.has(code)) {
      byContract.set(code, {
        orderformCode: code,
        ref: r.Ref || '',
        startDate: r.StartDate || '',
        endDate: r.EndDate || '',
        paymentTerm: r.PaymentTerm || '',
        lines: [],
        totalAmount: 0
      });
    }
    const line = {
      subProduct: r.SubProduct || '',
      comments: r.Comments || '',
      licenseOrdered: Number(r.LicenseOrdered || 0),
      price: Number(r.Price || 0),
      amount: Number(r.Amount || 0),
      accountManager: r.AccountManager || ''
    };
    const obj = byContract.get(code);
    obj.lines.push(line);
    obj.totalAmount += line.amount;

    const cid = String(r.ClientID || '').trim();
    if (cid) clientIds.add(cid);
  });

  const contracts = Array.from(byContract.values())
    .map(c => ({ ...c, startDate: _asISO_(c.startDate), endDate: _asISO_(c.endDate) }))
    .sort((a, b) => {
      const ak = _parseDateKey_(a.startDate), bk = _parseDateKey_(b.startDate);
      if (ak !== bk) return ak - bk;
      return (a.orderformCode || '').localeCompare(b.orderformCode || '');
    });

  return {
    customer: customerName,
    clientId: Array.from(clientIds).join(', '),
    contractCount: contracts.length,
    contracts
  };
}

/***** PUBLIC: Renewals (ONLY rows whose Description contains the phrase) *****/
function getUnsignedProspectContracts() {
  const phrase = '%' + REF_EXCLUDE_TEXT.toLowerCase() + '%';
  const sql = _contractsSelectSQL_() + `
    WHERE COALESCE(LOWER(Description), '') LIKE @phrase
  `;
  const rows = bqQuery_(sql, { phrase });

  const byContract = new Map();
  rows.forEach(r => {
    const code = String(r.OrderformCode || '').trim();
    const customer = String(r.CustomerName || '').trim();

    if (!byContract.has(code)) {
      byContract.set(code, {
        orderformCode: code,
        customer,
        clientId: String(r.ClientID || '').trim(),
        startDate: r.StartDate || '',
        endDate: r.EndDate || '',
        paymentTerm: r.PaymentTerm || '',
        lines: [],
        totalAmount: 0
      });
    }
    const line = {
      subProduct: r.SubProduct || '',
      comments: r.Comments || '',
      licenseOrdered: Number(r.LicenseOrdered || 0),
      price: Number(r.Price || 0),
      amount: Number(r.Amount || 0),
      accountManager: r.AccountManager || ''
    };
    const obj = byContract.get(code);
    obj.lines.push(line);
    obj.totalAmount += line.amount;
  });

  const contracts = Array.from(byContract.values()).map(c => ({
    ...c,
    startDate: _asISO_(c.startDate),
    endDate: _asISO_(c.endDate),
    _startKey: _parseDateKey_(c.startDate)
  })).sort((a, b) => {
    const d = a._startKey - b._startKey;
    if (d !== 0) return d;
    return (a.orderformCode || '').localeCompare(b.orderformCode || '');
  });

  return { total: contracts.length, contracts };
}

/***** PERSIST FILTER STATE *****/
function saveFilterState(namespace, state) {
  const json = (typeof state === 'string') ? state : JSON.stringify(state || {});
  PropertiesService.getUserProperties().setProperty('filters:' + namespace, json);
  return true;
}
function loadFilterState(namespace) {
  const v = PropertiesService.getUserProperties().getProperty('filters:' + namespace);
  try { return v ? JSON.parse(v) : null; } catch (e) { return null; }
}

/***** SHEET UI HELPERS (optional) *****/
function showSidebar() {
  const html = HtmlService.createTemplateFromFile('Contracts').evaluate().setTitle('Customer Contracts');
  SpreadsheetApp.getUi().showSidebar(html);
}
function showPopup() {
  const html = HtmlService.createHtmlOutputFromFile('Contracts').setWidth(1000).setHeight(700);
  SpreadsheetApp.getUi().showModelessDialog(html, 'Customer Contracts');
}
function showUnsignedProspects() {
  const html = HtmlService.createHtmlOutputFromFile('Prospect').setWidth(1100).setHeight(720);
  SpreadsheetApp.getUi().showModelessDialog(html, 'Renewals');
}
function hello() {
  SpreadsheetApp.getUi().alert('Buttons are wired correctly!');
}
