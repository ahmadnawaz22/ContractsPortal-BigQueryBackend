/** Web app router */
function doGet(e) {
  var t = HtmlService.createTemplateFromFile('Mobile');
  t.initialView = (e && e.parameter && e.parameter.view) || 'contracts';
  return t.evaluate()
    .setTitle('Contracts Hub (Mobile)')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1, maximum-scale=1')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/** Simple health check used by Mobile.html */
function ping() {
  return {
    ok: true,
    user: Session.getActiveUser().getEmail() || 'unknown',
    time: new Date().toISOString()
  };
}

/** Sheets menu to copy the web app URL quickly */
function onOpen() {
  try {
    SpreadsheetApp.getUi()
      .createMenu('Contracts')
      .addItem('Open Sidebar', 'showPopup')
      .addItem('Renewals (Popup)', 'showUnsignedProspectsPopup')
      .addItem('Invoices (Popup)', 'showInvoices')
      .addSeparator()
      .addItem('Open Mobile Web App', 'openMobileInDialog')
      .addToUi();
  } catch (_){}
}

function openMobileInDialog() {
  var url = ScriptApp.getService().getUrl();
  if (!url) {
    SpreadsheetApp.getUi().alert('Deploy this project as a Web App first: Deploy → New deployment → Web app.');
    return;
  }
  var html = HtmlService.createHtmlOutput(
    '<div style="padding:12px;font-family:Arial,sans-serif">' +
    '<div style="margin-bottom:8px">Mobile Web App URL:</div>' +
    '<div><a target="_blank" href="'+url+'">'+url+'</a></div>' +
    '</div>'
  ).setWidth(480).setHeight(140);
  SpreadsheetApp.getUi().showModalDialog(html, 'Mobile Web App');
}
