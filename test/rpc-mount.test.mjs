// Regression tests for installRpcChannel: dedicated RPC channels must mount via
// the Host `webServer` service directly (DSH v0.1.3-alpha.2 broke
// `connection.rpc.handle()` for every plugin caller), and keep the legacy
// fallback for contexts without a webServer.
import assert from 'node:assert/strict';
import test from 'node:test';
import { Readable } from 'node:stream';
import { installRpcChannel } from '../plugin-src/host/rpc-mount.mjs';

function fakeRes() {
  return {
    statusCode: 0, headers: {}, chunks: '', writableEnded: false,
    writeHead(status, headers) { this.statusCode = status; Object.assign(this.headers, headers ?? {}); },
    end(chunk) { if (chunk) this.chunks += String(chunk); this.writableEnded = true; },
    on() {},
  };
}

async function request(handler, { method = 'POST', url = '/test-ch/ping', body = '', headers } = {}) {
  const req = Readable.from(body ? [Buffer.from(body)] : []);
  req.method = method;
  req.url = url;
  req.headers = headers ?? { 'content-type': 'application/json' };
  const res = fakeRes();
  await handler(req, res);
  return res;
}

function webServerCtx({ rejection = () => undefined } = {}) {
  const routes = [];
  const ctx = {
    connection: { requestRejection: rejection },
    get(name) { return name === 'webServer' ? webServer : undefined; },
    effect: (fn) => fn(),
  };
  const webServer = {
    register(route) {
      routes.push(route);
      return () => routes.splice(routes.indexOf(route), 1);
    },
  };
  return { ctx, routes };
}

test('mounts the channel on webServer when available, bypassing rpc.handle', async () => {
  const { ctx, routes } = webServerCtx();
  ctx.connection.rpc = { handle: () => { throw new Error('rpc.handle must not be used'); } };
  const dispose = installRpcChannel(ctx, '/test-ch', async (endpoint, payload) => ({ ok: true, value: { endpoint, payload } }));
  assert.equal(routes.length, 1);
  assert.equal(routes[0].kind, 'prefix');
  assert.equal(routes[0].path, '/test-ch');

  const ok = await request(routes[0].handler, {
    body: JSON.stringify({ type: 'client-request', rpcId: 'r-1', method: 'ping', payload: { a: 1 } }),
  });
  assert.equal(ok.statusCode, 200);
  assert.deepEqual(JSON.parse(ok.chunks), {
    type: 'server-response',
    rpcId: 'r-1',
    result: { ok: true, value: { endpoint: 'ping', payload: { a: 1 } } },
  });

  dispose();
  assert.equal(routes.length, 0);
});

test('keeps the Connection auth fence and 401/403 responses', async () => {
  const { ctx, routes } = webServerCtx({ rejection: () => 401 });
  installRpcChannel(ctx, '/test-ch', async () => ({ ok: true, value: null }));
  const denied = await request(routes[0].handler, { body: '{}' });
  assert.equal(denied.statusCode, 401);
  assert.equal(denied.chunks, 'unauthorized');
});

test('mirrors the Connection wire envelope branches', async () => {
  const { ctx, routes } = webServerCtx();
  installRpcChannel(ctx, '/test-ch', async (endpoint) => ({ ok: true, value: { endpoint } }));
  const route = routes[0].handler;

  assert.equal((await request(route, { method: 'GET' })).statusCode, 404);
  assert.equal((await request(route, { url: '/other-ch/ping', body: '{}' })).statusCode, 404);
  assert.equal((await request(route, { url: '/test-ch/', body: '{}' })).statusCode, 404);
  assert.equal((await request(route, { body: '{}', headers: {} })).statusCode, 415);
  assert.equal((await request(route, { body: 'not json' })).statusCode, 400);

  const badEnvelope = JSON.parse((await request(route, { body: '{"type":"nope"}' })).chunks);
  assert.equal(badEnvelope.rpcId, 'invalid-request');
  assert.equal(badEnvelope.result.error.code, 'gateway/bad-request');

  const mismatch = JSON.parse((await request(route, {
    body: JSON.stringify({ type: 'client-request', rpcId: 'r-2', method: 'other', payload: {} }),
  })).chunks);
  assert.equal(mismatch.rpcId, 'r-2');
  assert.equal(mismatch.result.error.code, 'gateway/bad-request');
});

test('handler failures answer 500 like the Connection bridge', async () => {
  const { ctx, routes } = webServerCtx();
  installRpcChannel(ctx, '/test-ch', async () => { throw new Error('boom'); });
  const res = await request(routes[0].handler, {
    body: JSON.stringify({ type: 'client-request', rpcId: 'r-3', method: 'ping', payload: {} }),
  });
  assert.equal(res.statusCode, 500);
  assert.match(res.chunks, /handler failure/);
});

test('falls back to connection.rpc.handle without a webServer service', async () => {
  const calls = [];
  const ctx = {
    connection: { rpc: { handle: (...args) => { calls.push(args); return 'disposed'; } } },
  };
  const handler = async () => ({ ok: true, value: null });
  const dispose = installRpcChannel(ctx, '/test-ch', handler, { authority: 'loopback' });
  assert.equal(dispose, 'disposed');
  assert.deepEqual(calls, [['/test-ch', handler, { authority: 'loopback' }]]);
});

test('throws the legacy TypeError when neither transport exists', () => {
  assert.throws(() => installRpcChannel({}, '/test-ch', async () => ({ ok: true, value: null })), TypeError);
});
