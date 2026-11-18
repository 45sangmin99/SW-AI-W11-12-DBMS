// Optional DOM smoke test using an existing jsdom installation, no download.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
if (!process.argv[2]) throw new Error('Usage: node verify-dom.mjs /absolute/path/to/jsdom/lib/api.js');
const { JSDOM } = await import(pathToFileURL(process.argv[2]));
const dom = new JSDOM('<div id="drop"></div>', { url: 'http://localhost/' });
globalThis.document = dom.window.document;
let stock = 3;
const orders = [];
const calls = [];
globalThis.fetch = async (route) => {
  calls.push(route);
  if (route === '/api/drop') return { ok: true, json: async () => ({ stock: String(stock), orders: [...orders] }) };
  assert.equal(route, '/api/order');
  stock--;
  orders.unshift(String(orders.length + 1));
  return { ok: true, json: async () => ({ status: 'durable', orderId: orders[0] }) };
};
let source = await readFile(new URL('./app.mjs', import.meta.url), 'utf8');
source = source.replace('"/engine/component.js"', JSON.stringify(new URL('./engine/component.js', import.meta.url).href));
await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const button = () => document.querySelector('button.order');
const settle = async (predicate) => {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(document.body.innerHTML);
    await new Promise(resolve => setTimeout(resolve, 0));
  }
};
await settle(() => button() && !button().disabled);
assert(document.querySelector('.stock').textContent.includes('3 / 3'));
for (let i = 1; i <= 3; i++) {
  button().dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
  await settle(() => document.querySelectorAll('.history li').length === i &&
    document.querySelector('.stock').textContent.includes(`${3-i} / 3`));
}
assert(button().disabled);
assert.equal(button().textContent, '품절');
assert.equal(calls.filter(route => route === '/api/order').length, 3);
assert.equal(new Set([...document.querySelectorAll('.history li')].map(n => n.textContent)).size, 3);
dom.window.close();
console.log('PASS DOM: mount, effects, onClick handlers, three state updates, keyed history and sold-out button');
