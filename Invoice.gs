/******************************
 * Invoice.gs  (BigQuery-backed)
 * - Expects BigQuery config & helpers defined in Code.gs:
 *   BQ_PROJECT_ID, BQ_DATASET, BQ_TABLE_INVOICES='invoices', bqQuery_(), _getBQLocation_()
 ******************************/

// If you didn't set this in Code.gs, uncomment to define here:
// const BQ_PROJECT_ID = 'YOUR_PROJECT_ID';
// const BQ_DATASET    = 'YOUR_DATASET';
const BQ_TABLE_INVOICES = 'Invoices';

/***** Helpers *****/
function INV_asISO_(v) {
  if (!v) return '';
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v)) {
    return Utilities.formatDate(v, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }
  return String(v);
}

function INV_normRisk_(v) {
  const s = String(v || '').trim().toLowerCase();
  if (!s) return '';                 // blank = "Expected" in the UI
  if (s.startsWith('doubt')) return 'Doubtful';
  if (s.startsWith('nil'))   return 'NIL Value';
  return String(v).trim();
}

function INV_normTier_(v) {
  // Preserve original value if it's not one of the known spellings
  const raw = String(v || '').trim();
  const s = raw.toLowerCase();
  if (!s) return '';                                     // allow blank tier
  if (s.startsWith('received') || s.startsWith('recv')) return 'Received';
  if (s.startsWith('receivable') || s.includes('receivable')) return 'Receivable';
  // otherwise pass through (e.g., Paid, Unpaid, Overdue, Partial, etc.)
  return raw;
}

/***** PUBLIC: Issued invoices only *****/
function getInvoicesIssued() {
  // Support BOOL or string for IssuedStatus
  const sql = `
    SELECT
      InvoiceID           AS InvoiceNumber,
      ContractComponentID AS Contract,
      ClientName        AS Customer,      -- per mapping: Customer = ClientID
      InvoiceDate         AS IssueDate,
      DueDate             AS DueDate,
      Description         AS Description,
      InvoiceAmount       AS AmountUSD,
      OutstandingAmount   AS Balance,
      PaymentStatus       AS Tier,
      Risk                AS Risk,
      AccountManager      AS AccountManager,
      IssuedStatus        AS Issued
    FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE_INVOICES}\`
    WHERE (SAFE_CAST(IssuedStatus AS BOOL) = TRUE)
       OR (LOWER(CAST(IssuedStatus AS STRING)) IN ('yes','y','true','1'))
  `;

  const rows = bqQuery_(sql, {}); // bqQuery_ should already add the correct location

  const items = rows.map(r => {
    const tier = INV_normTier_(r.Tier || '');
    const risk = INV_normRisk_(r.Risk || '');
    // Coerce numbers safely (allow BigQuery strings or numerics)
    const amt = Number(String(r.AmountUSD ?? '0').replace(/,/g, '')) || 0;
    const bal = Number(String(r.Balance   ?? '0').replace(/,/g, '')) || 0;

    return {
      invoiceNumber:   String(r.InvoiceNumber || '').trim(),
      contract:        String(r.Contract || '').trim(),
      customer:        String(r.Customer || '').trim(),      // ClientID as requested
      issueDate:       INV_asISO_(r.IssueDate || ''),
      dueDate:         INV_asISO_(r.DueDate   || ''),
      description:     String(r.Description || ''),
      amountUSD:       amt,
      balance:         bal,
      tier:            tier,                                  // dynamic values preserved
      risk:            risk,                                  // '' (Expected) | Doubtful | NIL Value | other
      accountManager:  String(r.AccountManager || '').trim(),
      issued:          true
    };
  });

  // Sort for stable UI (Customer → DueDate → InvoiceNumber)
  items.sort((a, b) => {
    const c = a.customer.localeCompare(b.customer);
    if (c !== 0) return c;
    const ad = a.dueDate || '9999-12-31';
    const bd = b.dueDate || '9999-12-31';
    if (ad !== bd) return ad < bd ? -1 : 1;
    return a.invoiceNumber.localeCompare(b.invoiceNumber);
  });

  return {
    invoices: items,
    debug: {
      projectId: BQ_PROJECT_ID,
      dataset: BQ_DATASET,
      table: BQ_TABLE_INVOICES,
      locationUsed: (typeof _getBQLocation_ === 'function') ? _getBQLocation_() : '(n/a)',
      totalRows: rows.length,
      issuedYes: items.length
    }
  };
}

/***** Optional: open the Invoices UI *****/
function showInvoices() {
  const html = HtmlService.createHtmlOutputFromFile('Invoices')
    .setWidth(1100)
    .setHeight(720);
  SpreadsheetApp.getUi().showModelessDialog(html, 'Invoices');
}
