const fs = require('fs');
const path = require('path');

// ---- Configurable settings ----
const REGION = 'https://production-sfo.browserless.io';
const PROXY_PARAMS = '&proxy=residential&proxyCountry=lk';
const SITE_URL = 'https://meteo.gov.lk/';
const FALLBACK_PDF_URL = 'https://meteo.gov.lk/pdfs/twentyfour_pdf.pdf'; // used if the link isn't found on the page
const GOTO_TIMEOUT_MS = 60000;
const ATTEMPTS = 2;
const RETRY_WAIT_MS = 15000;
// -------------------------------

const TOKEN = process.env.BROWSERLESS_TOKEN;
if (!TOKEN) throw new Error('BROWSERLESS_TOKEN is missing');

const ENDPOINT = `${REGION}/stealth/bql?token=${TOKEN}${PROXY_PARAMS}`;

// "reject" skips images/fonts/styles so less proxy data is used
const QUERY = `
mutation Download($url: String!, $js: String!) {
  reject(type: [image, media, font, stylesheet]) { enabled }
  goto(url: $url, waitUntil: load, timeout: ${GOTO_TIMEOUT_MS}) { status }
  evaluate(content: $js, timeout: 60000) { value }
}`;

// Runs inside the page: finds the PDF link, downloads it, returns it as base64
const PAGE_JS = `(async () => {
  const link = [...document.querySelectorAll('a')]
    .find(a => /24 Hour Weather Report/i.test(a.textContent));
  const pdfUrl = link ? link.href : ${JSON.stringify(FALLBACK_PDF_URL)};
  const res = await fetch(pdfUrl);
  if (!res.ok) return JSON.stringify({ ok: false, status: res.status, pdfUrl });
  const buf = new Uint8Array(await res.arrayBuffer());
  let bin = '';
  for (let i = 0; i < buf.length; i += 0x8000) {
    bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
  }
  return JSON.stringify({ ok: true, pdfUrl, size: buf.length, b64: btoa(bin) });
})()`;

async function downloadOnce() {
  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      query: QUERY,
      operationName: 'Download',
      variables: { url: SITE_URL, js: PAGE_JS },
    }),
    signal: AbortSignal.timeout(150000),
  });
  if (!res.ok) throw new Error(`Browserless HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);

  const json = await res.json();
  if (json.errors) throw new Error(`BQL errors: ${JSON.stringify(json.errors).slice(0, 300)}`);

  const raw = json.data?.evaluate?.value;
  if (!raw) throw new Error(`Empty result (goto status ${json.data?.goto?.status})`);

  const out = JSON.parse(raw);
  if (!out.ok) throw new Error(`PDF fetch failed: status ${out.status} for ${out.pdfUrl}`);

  const pdf = Buffer.from(out.b64, 'base64');
  if (pdf.subarray(0, 4).toString() !== '%PDF') throw new Error('Downloaded file is not a PDF');
  console.log(`Downloaded ${pdf.length} bytes from ${out.pdfUrl}`);
  return pdf;
}

async function main() {
  const today = new Date().toISOString().slice(0, 10);
  const dst = path.join('reports', today, `weather-${today}.pdf`);

  let lastErr;
  for (let i = 1; i <= ATTEMPTS; i++) {
    try {
      const pdf = await downloadOnce();
      fs.mkdirSync(path.dirname(dst), { recursive: true });
      fs.writeFileSync(dst, pdf);
      console.log(`PDF saved to: ${dst}`);
      return;
    } catch (e) {
      lastErr = e;
      console.log(`Attempt ${i} failed: ${e.message}`);
      if (/HTTP 40[13]/.test(e.message)) break; // quota or auth problem: retrying only wastes units
      if (i < ATTEMPTS) await new Promise(r => setTimeout(r, RETRY_WAIT_MS));
    }
  }
  console.error('Download failed.');
  process.exit(1);
}

main();