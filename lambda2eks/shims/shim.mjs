// lambda2eks runtime shim for Node.js handlers.
//
// Replaces the Lambda runtime inside a Lambda base image and calls the unchanged handler.
// Modes (L2E_MODE): http (API Gateway events + Invoke API), sqs (queue worker), schedule (one run).
// Node built-ins only, except @aws-sdk/client-sqs in sqs mode (the Lambda Node base image ships it).

import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const env = process.env;
const TASK_ROOT = env.LAMBDA_TASK_ROOT || '/var/task';
const FUNCTION_NAME = env.AWS_LAMBDA_FUNCTION_NAME || 'function';
const MEMORY = env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || '128';
const TIMEOUT_MS = Number(env.L2E_TIMEOUT_S || '3') * 1000;
const REGION = env.AWS_REGION || 'us-east-1';
const ACCOUNT = env.L2E_ACCOUNT_ID || '000000000000';
const FUNCTION_ARN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${FUNCTION_NAME}`;
const HEALTH_PATH = '/_lambda2eks/healthz';
const INVOKE_PATH = '/2015-03-31/functions/function/invocations';
const TEXT_TYPES = /^(text\/|application\/(json|xml|javascript|x-www-form-urlencoded|[a-z.+-]*\+(json|xml)))/i;

const log = (level, msg) => console.log(JSON.stringify({ time: new Date().toISOString(), level, logger: 'lambda2eks', msg }));

function makeContext(requestId) {
  const deadline = Date.now() + TIMEOUT_MS;
  return {
    functionName: FUNCTION_NAME,
    functionVersion: env.AWS_LAMBDA_FUNCTION_VERSION || '$LATEST',
    invokedFunctionArn: FUNCTION_ARN,
    memoryLimitInMB: MEMORY,
    awsRequestId: requestId || randomUUID(),
    logGroupName: `/aws/lambda/${FUNCTION_NAME}`,
    logStreamName: env.HOSTNAME || 'local',
    callbackWaitsForEmptyEventLoop: true,
    getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
  };
}

export async function loadHandler(spec, root = TASK_ROOT) {
  // "dir/file.fn" -> module dir/file.{js,mjs,cjs}, export fn (nested exports allowed: file.a.b)
  const slash = spec.lastIndexOf('/');
  const dot = spec.indexOf('.', slash + 1);
  if (dot < 0) throw new Error(`handler ${spec} must look like file.function`);
  const base = path.join(root, spec.slice(0, dot));
  const file = ['.js', '.mjs', '.cjs'].map((ext) => base + ext).find(existsSync);
  if (!file) throw new Error(`cannot find ${base}.js, .mjs or .cjs`);
  const mod = await import(pathToFileURL(file).href);
  let fn = mod;
  for (const part of spec.slice(dot + 1).split('.')) fn = fn?.[part] ?? fn?.default?.[part];
  if (typeof fn !== 'function') throw new Error(`${spec} is not a function`);
  return fn;
}

function errorPayload(err) {
  return { errorType: err?.name || 'Error', errorMessage: err?.message || String(err), stackTrace: (err?.stack || '').split('\n') };
}

export class Invoker {
  constructor(handler, concurrency) {
    this.handler = handler;
    this.free = concurrency;
    this.waiting = [];
  }

  async invoke(event, requestId) {
    if (this.free > 0) this.free--;
    else await new Promise((resolve) => this.waiting.push(resolve));
    try {
      return await this.#run(event, makeContext(requestId));
    } finally {
      const next = this.waiting.shift();
      if (next) next();
      else this.free++;
    }
  }

  #run(event, ctx) {
    // Supports async handlers and callback-style handlers. A timed-out handler keeps running.
    return new Promise((resolve) => {
      let settled = false;
      const done = (ok, value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve([ok, value]);
      };
      const timer = setTimeout(() => {
        log('ERROR', `request ${ctx.awsRequestId} timed out after ${TIMEOUT_MS / 1000}s`);
        done(false, { errorType: 'TimeoutError', errorMessage: `Task timed out after ${(TIMEOUT_MS / 1000).toFixed(2)} seconds` });
      }, TIMEOUT_MS);
      const callback = (err, result) => (err ? done(false, errorPayload(err)) : done(true, result));
      try {
        const out = this.handler(event, ctx, callback);
        if (out && typeof out.then === 'function') {
          out.then((r) => done(true, r), (e) => {
            log('ERROR', `request ${ctx.awsRequestId} failed: ${e?.stack || e}`);
            done(false, errorPayload(e));
          });
        } else if (this.handler.length < 3) {
          done(true, out);
        }
      } catch (e) {
        log('ERROR', `request ${ctx.awsRequestId} failed: ${e?.stack || e}`);
        done(false, errorPayload(e));
      }
    });
  }
}

// ------------------------------------------------------------------ HTTP mode

export function compileRoutes(routes) {
  return (routes || []).filter((r) => r.path).map((r) => {
    const names = [];
    let pattern = '';
    for (const seg of r.path.replace(/^\/|\/$/g, '').split('/')) {
      const m = seg.match(/^\{([^}]+?)(\+?)\}$/);
      if (m) {
        names.push(m[1]);
        pattern += m[2] ? '/(.+)' : '/([^/]+)';
      } else if (seg) {
        pattern += '/' + seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      }
    }
    return { method: (r.method || 'ANY').toUpperCase(), template: r.path, regex: new RegExp(`^${pattern || '/'}/?$`), names };
  });
}

export function matchRoute(routes, method, p) {
  if (!routes.length) return ['$default', null];
  for (const r of routes) {
    const m = p.match(r.regex);
    if (m && (r.method === 'ANY' || r.method === method)) {
      const params = Object.fromEntries(r.names.map((n, i) => [n, decodeURIComponent(m[i + 1])]));
      return [r.template, r.names.length ? params : null];
    }
  }
  return [null, null];
}

export function buildEvent(fmt, method, rawPath, query, rawHeaders, body, sourceIp, template, pathParams) {
  const now = new Date();
  const requestId = randomUUID();
  const pairs = [];
  for (let i = 0; i < rawHeaders.length; i += 2) pairs.push([rawHeaders[i], rawHeaders[i + 1]]);
  const get = (name) => pairs.find(([k]) => k.toLowerCase() === name)?.[1] || '';
  const isB64 = body.length > 0 && !TEXT_TYPES.test(get('content-type'));
  const bodyOut = body.length ? (isB64 ? body.toString('base64') : body.toString('utf8')) : null;
  const qsPairs = [...new URLSearchParams(query)];

  if (fmt === '2.0') {
    const headers = {};
    const cookies = [];
    for (const [k, v] of pairs) {
      const lk = k.toLowerCase();
      if (lk === 'cookie') { cookies.push(...v.split(';').map((c) => c.trim()).filter(Boolean)); continue; }
      headers[lk] = lk in headers ? `${headers[lk]},${v}` : v;
    }
    const qs = {};
    for (const [k, v] of qsPairs) qs[k] = k in qs ? `${qs[k]},${v}` : v;
    const routeKey = template === '$default' ? '$default' : `${method} ${template}`;
    const event = {
      version: '2.0', routeKey, rawPath, rawQueryString: query, headers,
      requestContext: {
        accountId: ACCOUNT, apiId: 'lambda2eks', domainName: headers.host || '', domainPrefix: (headers.host || '').split('.')[0],
        http: { method, path: rawPath, protocol: 'HTTP/1.1', sourceIp, userAgent: get('user-agent') },
        requestId, routeKey, stage: '$default', time: now.toUTCString(), timeEpoch: now.getTime(),
      },
      isBase64Encoded: isB64,
    };
    if (cookies.length) event.cookies = cookies;
    if (qsPairs.length) event.queryStringParameters = qs;
    if (pathParams) event.pathParameters = pathParams;
    if (bodyOut !== null) event.body = bodyOut;
    return [event, requestId];
  }

  const mvHeaders = {};
  for (const [k, v] of pairs) (mvHeaders[k] ||= []).push(v);
  const mvQs = {};
  for (const [k, v] of qsPairs) (mvQs[k] ||= []).push(v);
  const last = (o) => (Object.keys(o).length ? Object.fromEntries(Object.entries(o).map(([k, v]) => [k, v.at(-1)])) : null);
  const resource = template === '$default' ? rawPath : template;
  return [{
    resource, path: rawPath, httpMethod: method,
    headers: last(mvHeaders), multiValueHeaders: Object.keys(mvHeaders).length ? mvHeaders : null,
    queryStringParameters: last(mvQs), multiValueQueryStringParameters: Object.keys(mvQs).length ? mvQs : null,
    pathParameters: pathParams, stageVariables: null,
    requestContext: {
      resourcePath: resource, httpMethod: method, path: rawPath, stage: 'prod', requestId,
      requestTimeEpoch: now.getTime(), accountId: ACCOUNT, apiId: 'lambda2eks', protocol: 'HTTP/1.1',
      identity: { sourceIp, userAgent: get('user-agent') },
    },
    body: bodyOut, isBase64Encoded: isB64,
  }, requestId];
}

export function convertResponse(fmt, result) {
  if (fmt === '2.0' && !(result && typeof result === 'object' && 'statusCode' in result)) {
    const body = typeof result === 'string' ? result : JSON.stringify(result ?? null);
    return [200, [['Content-Type', 'application/json']], Buffer.from(body)];
  }
  if (!result || typeof result !== 'object') {
    return [502, [['Content-Type', 'application/json']], Buffer.from('{"message": "Internal server error"}')];
  }
  const headers = Object.entries(result.headers || {}).map(([k, v]) => [k, String(v)]);
  for (const [k, vs] of Object.entries(result.multiValueHeaders || {})) for (const v of vs) headers.push([k, String(v)]);
  for (const c of result.cookies || []) headers.push(['Set-Cookie', c]);
  let body = result.body ?? '';
  if (typeof body !== 'string') body = JSON.stringify(body);
  return [Number(result.statusCode || 200), headers, result.isBase64Encoded ? Buffer.from(body, 'base64') : Buffer.from(body)];
}

function send(res, status, headers, body) {
  const grouped = {};
  for (const [k, v] of headers) (grouped[k] ||= []).push(v);
  for (const [k, vs] of Object.entries(grouped)) res.setHeader(k, vs.length === 1 ? vs[0] : vs);
  res.setHeader('Content-Length', body.length);
  res.writeHead(status);
  res.end(body);
}

function serveHttp(invoker) {
  const fmt = env.L2E_HTTP_EVENT_FORMAT || '1.0';
  const routes = compileRoutes(JSON.parse(env.L2E_HTTP_ROUTES || '[]'));
  const port = Number(env.L2E_PORT || '8080');
  const json = [['Content-Type', 'application/json']];
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const body = Buffer.concat(chunks);
    const url = new URL(req.url, 'http://localhost');
    try {
      if (url.pathname === HEALTH_PATH) return send(res, 200, [['Content-Type', 'text/plain']], Buffer.from('ok'));
      if (url.pathname === INVOKE_PATH && req.method === 'POST') {
        const [ok, result] = await invoker.invoke(JSON.parse(body.toString() || '{}'));
        return send(res, 200, ok ? json : [...json, ['X-Amz-Function-Error', 'Unhandled']], Buffer.from(JSON.stringify(result ?? null)));
      }
      const [template, params] = matchRoute(routes, req.method, url.pathname);
      if (template === null) return send(res, 404, json, Buffer.from('{"message": "Not Found"}'));
      const [event, requestId] = buildEvent(fmt, req.method, url.pathname, url.search.slice(1), req.rawHeaders, body,
        req.socket.remoteAddress || '', template, params);
      const [ok, result] = await invoker.invoke(event, requestId);
      if (!ok) {
        const timeout = result.errorType === 'TimeoutError';
        return send(res, timeout ? 504 : 502, json,
          Buffer.from(timeout ? '{"message": "Endpoint request timed out"}' : '{"message": "Internal server error"}'));
      }
      send(res, ...convertResponse(fmt, result));
    } catch (e) {
      log('ERROR', `shim error: ${e?.stack || e}`);
      if (!res.headersSent) send(res, 500, json, Buffer.from('{"message": "Internal server error"}'));
    }
  });
  process.on('SIGTERM', () => { log('INFO', 'SIGTERM: finishing in-flight requests'); server.close(); });
  server.listen(port, () => log('INFO', `http mode on :${port}, payload format ${fmt}, ${routes.length} route(s)`));
}

// ------------------------------------------------------------------ SQS mode

async function loadSqs() {
  // The base image ships the AWS SDK under /var/runtime; a copy bundled with the function wins.
  for (const from of [path.join(TASK_ROOT, 'index.js'), '/var/runtime/index.mjs']) {
    try {
      return await import(pathToFileURL(createRequire(from).resolve('@aws-sdk/client-sqs')).href);
    } catch { /* try the next location */ }
  }
  throw new Error('@aws-sdk/client-sqs not found in the function bundle or the base image');
}

export function sqsRecord(msg, queueArn) {
  const b64 = (b) => Buffer.from(b).toString('base64');
  const attrs = {};
  for (const [name, a] of Object.entries(msg.MessageAttributes || {})) {
    attrs[name] = {
      stringValue: a.StringValue, binaryValue: a.BinaryValue ? b64(a.BinaryValue) : undefined,
      stringListValues: a.StringListValues || [], binaryListValues: (a.BinaryListValues || []).map(b64), dataType: a.DataType,
    };
  }
  return {
    messageId: msg.MessageId, receiptHandle: msg.ReceiptHandle, body: msg.Body, attributes: msg.Attributes || {},
    messageAttributes: attrs, md5OfBody: msg.MD5OfBody, eventSource: 'aws:sqs', eventSourceARN: queueArn, awsRegion: REGION,
  };
}

export function failedIds(result, records) {
  if (!result || typeof result !== 'object' || !('batchItemFailures' in result)) return new Set();
  const ids = new Set(records.map((r) => r.messageId));
  const out = new Set();
  for (const item of result.batchItemFailures || []) {
    const id = item?.itemIdentifier;
    if (!id || !ids.has(id)) return ids; // Lambda treats a malformed response as a failure of the whole batch
    out.add(id);
  }
  return out;
}

async function runSqs(invoker) {
  const { SQSClient, ReceiveMessageCommand, DeleteMessageBatchCommand } = await loadSqs();
  const queueUrl = env.L2E_SQS_QUEUE_URL;
  const queueArn = env.L2E_SQS_QUEUE_ARN || '';
  const batch = Number(env.L2E_SQS_BATCH_SIZE || '10');
  const partial = env.L2E_SQS_REPORT_BATCH_ITEM_FAILURES === 'true';
  const sqs = new SQSClient({ region: REGION });
  let stopping = false;
  process.on('SIGTERM', () => { log('INFO', 'SIGTERM: finishing the current batch'); stopping = true; });
  log('INFO', `sqs mode on ${queueUrl}, batch ${batch}, partial failures ${partial}`);
  const poll = async () => {
    while (!stopping) {
      let resp;
      try {
        resp = await sqs.send(new ReceiveMessageCommand({
          QueueUrl: queueUrl, MaxNumberOfMessages: Math.min(batch, 10), WaitTimeSeconds: 20,
          MessageSystemAttributeNames: ['All'], MessageAttributeNames: ['All'],
        }));
      } catch (e) {
        log('ERROR', `ReceiveMessage failed, retrying in 5s: ${e?.message || e}`);
        await new Promise((r) => setTimeout(r, 5000));
        continue;
      }
      const msgs = resp.Messages || [];
      if (msgs.length) await processBatch(sqs, DeleteMessageBatchCommand, queueUrl, invoker, msgs.map((m) => sqsRecord(m, queueArn)), partial);
    }
  };
  // One poller per concurrency slot, as Lambda runs one batch per environment.
  await Promise.all(Array.from({ length: Number(env.L2E_CONCURRENCY || '1') }, poll));
}

async function processBatch(sqs, DeleteMessageBatchCommand, queueUrl, invoker, records, partial) {
  const [ok, result] = await invoker.invoke({ Records: records });
  if (!ok) { log('ERROR', `batch of ${records.length} failed; messages return after the visibility timeout`); return; }
  const keep = partial ? failedIds(result, records) : new Set();
  const done = records.filter((r) => !keep.has(r.messageId));
  for (let i = 0; i < done.length; i += 10) {
    const chunk = done.slice(i, i + 10);
    const res = await sqs.send(new DeleteMessageBatchCommand({
      QueueUrl: queueUrl, Entries: chunk.map((r, n) => ({ Id: String(n), ReceiptHandle: r.receiptHandle })),
    }));
    for (const f of res.Failed || []) log('ERROR', `delete failed for ${chunk[Number(f.Id)].messageId}: ${f.Message}`);
  }
}

// ------------------------------------------------------------------ schedule mode

async function runSchedule(invoker) {
  const event = env.L2E_SCHEDULE_INPUT ? JSON.parse(env.L2E_SCHEDULE_INPUT) : {
    version: '0', id: randomUUID(), 'detail-type': 'Scheduled Event', source: 'aws.events', account: ACCOUNT,
    time: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'), region: REGION,
    resources: env.L2E_SCHEDULE_RULE_ARN ? [env.L2E_SCHEDULE_RULE_ARN] : [], detail: {},
  };
  const [ok, result] = await invoker.invoke(event);
  log(ok ? 'INFO' : 'ERROR', `scheduled run ${ok ? 'succeeded' : 'failed'}: ${JSON.stringify(result ?? null).slice(0, 2000)}`);
  process.exit(ok ? 0 : 1);
}

async function main() {
  const invoker = new Invoker(await loadHandler(env.L2E_HANDLER), Number(env.L2E_CONCURRENCY || '1'));
  const mode = env.L2E_MODE || 'http';
  if (mode === 'http') return serveHttp(invoker);
  if (mode === 'sqs') return runSqs(invoker);
  if (mode === 'schedule') return runSchedule(invoker);
  throw new Error(`unknown L2E_MODE ${mode}`);
}

if (import.meta.url === pathToFileURL(process.argv[1] || '').href) {
  main().catch((e) => { log('ERROR', e?.stack || String(e)); process.exit(1); });
}
