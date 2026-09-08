// Shared mounting shim for dedicated Host RPC channels.
//
// Why: `connection.rpc.handle()` mounts its channel as a `webServer` prefix route
// by reading `webServer` off the Connection plugin's OWN fiber context. DSH
// v0.1.3-alpha.2 dropped `webServer` from that plugin's `inject`, so the lookup
// now throws `cannot get property "webServer" without inject` for every caller
// (dsh-im activates channels inside try/catch, so the failure is silent and the
// channels just never appear — browser RPC calls then fall through to the SPA
// handler and answer `405`). On older DSH releases the same lookup only worked
// by that accident.
//
// Fix: resolve `webServer` through `ctx.get()` — the inject-free accessor — and
// register the prefix route directly, replicating the auth fence
// (`connection.requestRejection`) and the client-request/server-response wire
// envelope of the Connection plugin's own route so the protocol is unchanged.
// When the context has no `webServer` service (legacy hosts, programmatic test
// fakes), fall back to `connection.rpc.handle()` verbatim.

const ENDPOINT_SEGMENT_PATTERN = /^[A-Za-z0-9_$.-]+$/;
/** Matches the Connection plugin's default buffered-route body limit. */
const DEFAULT_MAX_BODY_BYTES = 314572800;

function endpointFromChannelPath(channel, pathname) {
  if (!pathname.startsWith(`${channel}/`)) return undefined;
  const endpoint = pathname.slice(channel.length + 1);
  if (endpoint.split('/').some((segment) =>
    segment === '' || segment === '.' || segment === '..' || !ENDPOINT_SEGMENT_PATTERN.test(segment))) return undefined;
  return endpoint;
}

/** Structural check mirroring the Connection plugin's clientRequestSchema. */
function envelopeIssues(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) return [{ message: 'expected an object' }];
  const issues = [];
  if (body.type !== 'client-request') issues.push({ message: 'expected type "client-request"' });
  if (typeof body.rpcId !== 'string' || body.rpcId === '') issues.push({ message: 'rpcId must be a non-empty string' });
  if (typeof body.method !== 'string') issues.push({ message: 'method must be a string' });
  return issues;
}

async function dispatchChannelRequest(ctx, channel, handler, req, res) {
  const sendText = (status, text) => {
    res.writeHead(status, { 'content-type': 'text/plain;charset=UTF-8' });
    res.end(text);
  };
  const sendResponse = (rpcId, result) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ type: 'server-response', rpcId, result }));
  };
  const url = new URL(req.url ?? '/', 'http://dsh.internal');
  const endpoint = endpointFromChannelPath(channel, url.pathname);
  if (req.method !== 'POST' || endpoint === undefined) return sendText(404, 'not found');
  const contentType = String(req.headers['content-type'] ?? '').split(';', 1)[0].trim().toLowerCase();
  if (contentType !== 'application/json') return sendText(415, 'content type must be application/json');
  const abort = new AbortController();
  res.on('close', () => {
    if (!res.writableEnded) abort.abort();
  });
  const chunks = [];
  let received = 0;
  for await (const chunk of req) {
    received += chunk.length;
    if (received > DEFAULT_MAX_BODY_BYTES) {
      res.writeHead(413, { connection: 'close' });
      res.end();
      req.destroy();
      return;
    }
    chunks.push(chunk);
  }
  let body;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    return sendText(400, 'body is not JSON');
  }
  const issues = envelopeIssues(body);
  if (issues.length > 0) {
    const rpcId = typeof body?.rpcId === 'string' && body.rpcId !== '' ? body.rpcId : 'invalid-request';
    return sendResponse(rpcId, {
      ok: false,
      error: { code: 'gateway/bad-request', message: 'invalid client-request message', details: { issues } },
    });
  }
  if (body.method !== endpoint) {
    return sendResponse(body.rpcId, {
      ok: false,
      error: {
        code: 'gateway/bad-request',
        message: `method ${JSON.stringify(body.method)} does not match endpoint ${JSON.stringify(endpoint)}`,
        details: { issues: [] },
      },
    });
  }
  try {
    return sendResponse(body.rpcId, await handler(endpoint, body.payload ?? {}, abort.signal));
  } catch (error) {
    return sendText(500, `handler failure: ${String(error)}`);
  }
}

/**
 * Mount a dedicated RPC channel on the caller's context.
 * @param ctx - Host plugin context (needs `connection`, and `webServer` when available).
 * @param channel - The dedicated channel path, e.g. `/feishu`.
 * @param handler - `(endpoint, payload, signal)` RPC handler returning a Connection rpcResult.
 * @param options - Passed through to `connection.rpc.handle` on the legacy fallback only
 *   (the option was never consumed by newer Connection releases).
 * @returns a disposer removing the channel registration.
 */
export function installRpcChannel(ctx, channel, handler, options = {}) {
  const webServer = typeof ctx?.get === 'function' ? ctx.get('webServer') : undefined;
  if (typeof webServer?.register === 'function') {
    return ctx.effect(() => webServer.register({
      kind: 'prefix',
      path: channel,
      handler: async (req, res) => {
        // Same Host/Origin fence plus persistent browser authentication the
        // Connection plugin applies to its own /api transport.
        const rejection = ctx.connection?.requestRejection?.(req);
        if (rejection !== undefined) {
          res.writeHead(rejection);
          res.end(rejection === 401 ? 'unauthorized' : 'forbidden');
          return;
        }
        await dispatchChannelRequest(ctx, channel, handler, req, res);
      },
    }), `dsh-im: ${channel} rpc channel`);
  }
  if (!ctx?.connection?.rpc || typeof ctx.connection.rpc.handle !== 'function') {
    throw new TypeError('DSH Host Connection RPC is required');
  }
  return ctx.connection.rpc.handle(channel, handler, options);
}
