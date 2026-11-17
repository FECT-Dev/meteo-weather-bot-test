// ---- Configurable settings ----
const REGION = 'https://production-sfo.browserless.io';
const TARGET_URL = 'https://meteo.gov.lk/';
const PROXY_PARAMS = '&proxy=residential&proxyCountry=lk'; // set to '' to test without a proxy
const GOTO_TIMEOUT_MS = 60000;
// -------------------------------

const TOKEN = process.env.BROWSERLESS_TOKEN;
if (!TOKEN) throw new Error('BROWSERLESS_TOKEN is missing');

const ENDPOINT = `${REGION}/stealth/bql?token=${TOKEN}${PROXY_PARAMS}`;

const QUERY = `
mutation Test($url: String!, $js: String!) {
  goto(url: $url, waitUntil: load, timeout: ${GOTO_TIMEOUT_MS}) { status }
  evaluate(content: $js, timeout: 20000) { value }
}`;

const JS = `(() => JSON.stringify({
  title: document.title,
  links: [...document.querySelectorAll('a')]
    .filter(a => /24 Hour|Observation/i.test(a.textContent))
    .map(a => ({ text: a.textContent.trim(), href: a.href }))
    .slice(0, 10)
}))()`;

(async () => {
  const start = Date.now();
  try {
    const res = await fetch(ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: QUERY, operationName: 'Test', variables: { url: TARGET_URL, js: JS } }),
      signal: AbortSignal.timeout(120000),
    });
    const text = await res.text();
    console.log(`HTTP ${res.status} in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    console.log(text.slice(0, 1500));
  } catch (e) {
    console.log(`FAILED after ${((Date.now() - start) / 1000).toFixed(1)}s: ${e.message}`);
  }
})();