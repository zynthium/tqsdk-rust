import { readFileSync } from 'node:fs';
import { gzipSync } from 'node:zlib';

const distRoot = new URL('../../src/dashboard-dist/', import.meta.url);
const budgets = [
  { label: 'app.js raw', file: 'assets/app.js', max: 100 * 1024, gzip: false },
  { label: 'app.js gzip', file: 'assets/app.js', max: 36 * 1024, gzip: true },
  { label: 'app.css raw', file: 'assets/app.css', max: 40 * 1024, gzip: false },
  { label: 'app.css gzip', file: 'assets/app.css', max: 10 * 1024, gzip: true },
];

let failed = false;

for (const budget of budgets) {
  const bytes = readFileSync(new URL(budget.file, distRoot));
  const size = budget.gzip ? gzipSync(bytes).length : bytes.length;
  const status = size <= budget.max ? 'ok' : 'over';
  console.log(`${status} ${budget.label}: ${formatBytes(size)} / ${formatBytes(budget.max)}`);
  if (size > budget.max) failed = true;
}

if (failed) {
  process.exitCode = 1;
}

function formatBytes(bytes) {
  return `${(bytes / 1024).toFixed(1)} KiB`;
}
