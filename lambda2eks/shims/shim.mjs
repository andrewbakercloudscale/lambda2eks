// lambda2eks runtime shim for Node.js handlers.
//
// Replaces the Lambda runtime inside a Lambda base image and calls the unchanged handler.
// Modes (L2E_MODE): http (API Gateway events + Invoke API), sqs (queue worker, optionally unwrapping
// S3 or SNS events from a bridge queue), stream (Kinesis / DynamoDB Streams consumer), schedule (one run).
// Node built-ins only, except the AWS SDK v3 clients in sqs and stream modes (the Lambda Node base image ships them).

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
// Any function name, so SDK clients pointed at this endpoint keep their FunctionName.
const INVOKE_PATH = /^\/2015-03-31\/functions\/[^/]+\/invocations$/;
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
      if (INVOKE_PATH.test(url.pathname) && req.method === 'POST') {
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

async function loadSdk(pkg) {
  // The base image ships the AWS SDK under /var/runtime; a copy bundled with the function wins.
  for (const from of [path.join(TASK_ROOT, 'index.js'), '/var/runtime/index.mjs']) {
    try {
      return await import(pathToFileURL(createRequire(from).resolve(pkg)).href);
    } catch { /* try the next location */ }
  }
  throw new Error(`${pkg} not found in the function bundle or the base image`);
}

// (operation, input) => response, so tests can pass a fake in place of the SDK client.
async function sdkCaller(pkg, Client) {
  const mod = await loadSdk(pkg);
  const client = new mod[Client]({ region: REGION });
  return (op, input) => client.send(new mod[`${op}Command`](input));
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

// Body of a bridge-queue message -> the event Lambda would have received, or null to drop it.
export function unwrapMessage(kind, body, subscriptionArn = '') {
  const msg = JSON.parse(body);
  if (kind === 's3') {
    if (msg.Event === 's3:TestEvent') return null; // S3 sends this once when the notification is configured
    if (!Array.isArray(msg.Records)) throw new Error('no Records');
    return { Records: msg.Records };
  }
  const attrs = Object.fromEntries(Object.entries(msg.MessageAttributes || {}).map(([k, v]) => [k, { Type: v.Type, Value: v.Value }]));
  return {
    Records: [{
      EventSource: 'aws:sns', EventVersion: '1.0', EventSubscriptionArn: subscriptionArn || `${msg.TopicArn}:lambda2eks`,
      Sns: {
        Type: msg.Type, MessageId: msg.MessageId, TopicArn: msg.TopicArn, Subject: msg.Subject ?? null, Message: msg.Message,
        Timestamp: msg.Timestamp, SignatureVersion: msg.SignatureVersion, Signature: msg.Signature,
        SigningCertUrl: msg.SigningCertURL, UnsubscribeUrl: msg.UnsubscribeURL, MessageAttributes: attrs,
      },
    }],
  };
}

async function deleteRecords(sqs, queueUrl, done) {
  for (let i = 0; i < done.length; i += 10) {
    const chunk = done.slice(i, i + 10);
    const res = await sqs('DeleteMessageBatch', {
      QueueUrl: queueUrl, Entries: chunk.map((r, n) => ({ Id: String(n), ReceiptHandle: r.receiptHandle })),
    });
    for (const f of res.Failed || []) log('ERROR', `delete failed for ${chunk[Number(f.Id)].messageId}: ${f.Message}`);
  }
}

export async function processBatch(sqs, queueUrl, invoker, records, partial) {
  const [ok, result] = await invoker.invoke({ Records: records });
  if (!ok) { log('ERROR', `batch of ${records.length} failed; messages return after the visibility timeout`); return; }
  const keep = partial ? failedIds(result, records) : new Set();
  await deleteRecords(sqs, queueUrl, records.filter((r) => !keep.has(r.messageId)));
}

// S3 and SNS invoke Lambda one event at a time, so each message is its own invocation.
export async function processBridged(sqs, queueUrl, invoker, records, kind, subscriptionArn = '') {
  const done = [];
  for (const r of records) {
    let event;
    try {
      event = unwrapMessage(kind, r.body, subscriptionArn);
    } catch (e) {
      log('ERROR', `message ${r.messageId} is not a ${kind} event (${e.message}); it stays on the queue for the redrive policy`);
      continue;
    }
    if (event === null) { done.push(r); continue; }
    const [ok] = await invoker.invoke(event);
    if (ok) done.push(r);
    else log('ERROR', `message ${r.messageId} failed; it returns to the queue after the visibility timeout`);
  }
  await deleteRecords(sqs, queueUrl, done);
}

async function runSqs(invoker) {
  const sqs = await sdkCaller('@aws-sdk/client-sqs', 'SQSClient');
  const queueUrl = env.L2E_SQS_QUEUE_URL;
  const queueArn = env.L2E_SQS_QUEUE_ARN || '';
  const batch = Number(env.L2E_SQS_BATCH_SIZE || '10');
  const partial = env.L2E_SQS_REPORT_BATCH_ITEM_FAILURES === 'true';
  const unwrap = env.L2E_SQS_UNWRAP || '';
  let stopping = false;
  process.on('SIGTERM', () => { log('INFO', 'SIGTERM: finishing the current batch'); stopping = true; });
  log('INFO', `sqs mode on ${queueUrl}, batch ${batch}, partial failures ${partial}${unwrap ? `, unwrapping ${unwrap}` : ''}`);
  const poll = async () => {
    while (!stopping) {
      let resp;
      try {
        resp = await sqs('ReceiveMessage', {
          QueueUrl: queueUrl, MaxNumberOfMessages: Math.min(batch, 10), WaitTimeSeconds: 20,
          MessageSystemAttributeNames: ['All'], MessageAttributeNames: ['All'],
        });
      } catch (e) {
        log('ERROR', `ReceiveMessage failed, retrying in 5s: ${e?.message || e}`);
        await new Promise((r) => setTimeout(r, 5000));
        continue;
      }
      const records = (resp.Messages || []).map((m) => sqsRecord(m, queueArn));
      if (records.length && unwrap) await processBridged(sqs, queueUrl, invoker, records, unwrap, env.L2E_SNS_SUBSCRIPTION_ARN || '');
      else if (records.length) await processBatch(sqs, queueUrl, invoker, records, partial);
    }
  };
  // One poller per concurrency slot, as Lambda runs one batch per environment.
  await Promise.all(Array.from({ length: Number(env.L2E_CONCURRENCY || '1') }, poll));
}

// ------------------------------------------------------------------ stream mode
//
// Same design as the Python shim: replicas divide the shards with leases in a DynamoDB table,
// each lease holds the shard's checkpoint, and a child shard starts only after its parent finished.

const LEASE_TTL_S = 30;
const RENEW_EVERY_S = 10;
const MEMBER = '~member~';
class LeaseLost extends Error {}

export class Leases {
  constructor(ddb, table, consumer, owner) {
    Object.assign(this, { ddb, table, consumer, owner });
  }

  key(id) { return { shard: { S: `${this.consumer}#${id}` } }; }

  heartbeat(now) {
    return this.ddb('PutItem', { TableName: this.table, Item: { ...this.key(MEMBER + this.owner), owner: { S: this.owner }, expires: { N: String(now + LEASE_TTL_S) } } });
  }

  async snapshot() {
    const out = {};
    const input = { TableName: this.table, ConsistentRead: true };
    for (;;) {
      const page = await this.ddb('Scan', input);
      for (const item of page.Items || []) {
        const [consumer, ...rest] = item.shard.S.split('#');
        if (consumer !== this.consumer) continue;
        out[rest.join('#')] = {
          owner: item.owner?.S ?? null, expires: Number(item.expires?.N ?? 0),
          checkpoint: item.checkpoint?.S || null, finished: item.finished?.BOOL ?? false,
        };
      }
      if (!page.LastEvaluatedKey) return out;
      input.ExclusiveStartKey = page.LastEvaluatedKey;
    }
  }

  async update(id, expr, cond, values, names) {
    try {
      await this.ddb('UpdateItem', { TableName: this.table, Key: this.key(id), UpdateExpression: expr, ConditionExpression: cond,
        ExpressionAttributeValues: values, ExpressionAttributeNames: names });
      return true;
    } catch (e) {
      if (e?.name === 'ConditionalCheckFailedException') return false;
      throw e;
    }
  }

  acquire(id, now) {
    return this.update(id, 'SET #o = :me, #e = :exp', 'attribute_not_exists(#o) OR #o = :me OR #e < :now',
      { ':me': { S: this.owner }, ':exp': { N: String(now + LEASE_TTL_S) }, ':now': { N: String(now) } }, { '#o': 'owner', '#e': 'expires' });
  }

  renew(id, now) {
    return this.update(id, 'SET #e = :exp', '#o = :me', { ':me': { S: this.owner }, ':exp': { N: String(now + LEASE_TTL_S) } },
      { '#o': 'owner', '#e': 'expires' });
  }

  async checkpoint(id, seq, finished = false) {
    const ok = await this.update(id, 'SET #c = :seq, #f = :fin', '#o = :me',
      { ':me': { S: this.owner }, ':seq': { S: seq || '' }, ':fin': { BOOL: finished } }, { '#o': 'owner', '#c': 'checkpoint', '#f': 'finished' });
    if (!ok) throw new LeaseLost(id);
  }

  release(id) {
    return this.update(id, 'SET #e = :zero', '#o = :me', { ':me': { S: this.owner }, ':zero': { N: '0' } }, { '#o': 'owner', '#e': 'expires' });
  }
}

const epoch = (v) => (v instanceof Date ? v.getTime() / 1000 : Number(v));

export class KinesisSource {
  constructor(call, streamName, streamArn) {
    Object.assign(this, { call, name: streamName, kind: 'kinesis' });
    this.arn = streamArn || `arn:aws:kinesis:${REGION}:${ACCOUNT}:stream/${streamName}`;
  }

  async shards() {
    const out = [];
    let input = { StreamName: this.name };
    for (;;) {
      const page = await this.call('ListShards', input);
      out.push(...page.Shards.map((sh) => ({ id: sh.ShardId, parent: sh.ParentShardId || null })));
      if (!page.NextToken) return out;
      input = { NextToken: page.NextToken };
    }
  }

  async iterator(shardId, checkpoint, position, startTs) {
    const input = { StreamName: this.name, ShardId: shardId };
    if (checkpoint) Object.assign(input, { ShardIteratorType: 'AFTER_SEQUENCE_NUMBER', StartingSequenceNumber: checkpoint });
    else if (startTs != null) Object.assign(input, { ShardIteratorType: 'AT_TIMESTAMP', Timestamp: new Date(startTs * 1000) });
    else input.ShardIteratorType = position;
    return (await this.call('GetShardIterator', input)).ShardIterator;
  }

  async read(it, limit) {
    const resp = await this.call('GetRecords', { ShardIterator: it, Limit: limit });
    return [resp.Records || [], resp.NextShardIterator ?? null];
  }

  record(r, shardId) {
    return {
      kinesis: {
        kinesisSchemaVersion: '1.0', partitionKey: r.PartitionKey, sequenceNumber: r.SequenceNumber,
        data: Buffer.from(r.Data).toString('base64'), approximateArrivalTimestamp: epoch(r.ApproximateArrivalTimestamp),
      },
      eventSource: 'aws:kinesis', eventVersion: '1.0', eventID: `${shardId}:${r.SequenceNumber}`, eventName: 'aws:kinesis:record',
      invokeIdentityArn: env.L2E_ROLE_ARN || '', awsRegion: REGION, eventSourceARN: this.arn,
    };
  }

  seq(rec) { return rec.kinesis.sequenceNumber; }

  created(raw) { return epoch(raw.ApproximateArrivalTimestamp); }
}

export class DynamoDBSource {
  constructor(call, streamArn) {
    Object.assign(this, { call, arn: streamArn, kind: 'dynamodb' });
  }

  async shards() {
    const out = [];
    const input = { StreamArn: this.arn };
    for (;;) {
      const desc = (await this.call('DescribeStream', input)).StreamDescription;
      out.push(...desc.Shards.map((sh) => ({ id: sh.ShardId, parent: sh.ParentShardId || null })));
      if (!desc.LastEvaluatedShardId) return out;
      input.ExclusiveStartShardId = desc.LastEvaluatedShardId;
    }
  }

  async iterator(shardId, checkpoint, position, startTs) {
    const input = { StreamArn: this.arn, ShardId: shardId };
    if (checkpoint) Object.assign(input, { ShardIteratorType: 'AFTER_SEQUENCE_NUMBER', SequenceNumber: checkpoint });
    else input.ShardIteratorType = startTs != null ? 'TRIM_HORIZON' : position; // no AT_TIMESTAMP; older records are skipped
    return (await this.call('GetShardIterator', input)).ShardIterator;
  }

  async read(it, limit) {
    const resp = await this.call('GetRecords', { ShardIterator: it, Limit: limit });
    return [resp.Records || [], resp.NextShardIterator ?? null];
  }

  record(r) {
    const out = { ...r, dynamodb: { ...r.dynamodb }, eventSourceARN: this.arn };
    if (out.dynamodb.ApproximateCreationDateTime != null) out.dynamodb.ApproximateCreationDateTime = epoch(out.dynamodb.ApproximateCreationDateTime);
    return out;
  }

  seq(rec) { return rec.dynamodb.SequenceNumber; }

  created(raw) { return epoch(raw.dynamodb?.ApproximateCreationDateTime ?? 0); }
}

export class StreamConsumer {
  constructor(source, leases, invoker, opts = {}) {
    Object.assign(this, {
      source, leases, invoker, batchSize: 100, maxRetry: -1, bisect: false, partial: false, position: 'LATEST', startTs: null,
      idleWait: 1, ...opts,
    });
    this.sleep = opts.sleep || ((s) => new Promise((r) => setTimeout(r, s * 1000)));
    this.stopping = false;
    this.workers = new Map(); // shard id -> { stop: { stopped }, promise, done }
  }

  firstFailure(result, records) {
    if (!result || typeof result !== 'object' || !('batchItemFailures' in result)) return null;
    const items = result.batchItemFailures || [];
    if (!items.length) return null;
    const index = new Map(records.map((r, i) => [this.source.seq(r), i]));
    const hits = items.map((item) => index.get(item?.itemIdentifier));
    return hits.includes(undefined) ? 0 : Math.min(...hits); // a malformed response retries the whole batch, as Lambda does
  }

  // Invoke until the records are handled or their retries run out, checkpointing progress.
  async deliver(records, checkpoint, attempt = 0) {
    while (records.length && !this.stopping) {
      const [ok, result] = await this.invoker.invoke({ Records: records });
      const failedAt = ok && this.partial ? this.firstFailure(result, records) : null;
      if (ok && failedAt === null) return true;
      if (ok && failedAt > 0) {
        await checkpoint(this.source.seq(records[failedAt - 1]));
        records = records.slice(failedAt);
      }
      if (this.bisect && records.length > 1) { // as on Lambda, a split does not count as a retry
        const mid = Math.floor(records.length / 2);
        return (await this.deliver(records.slice(0, mid), checkpoint, attempt)) && this.deliver(records.slice(mid), checkpoint, attempt);
      }
      attempt += 1;
      if (this.maxRetry >= 0 && attempt > this.maxRetry) {
        log('ERROR', `skipping ${records.length} record(s) from ${this.source.seq(records[0])} after ${attempt} attempt(s)`);
        return true;
      }
      await this.sleep(Math.min(2 ** attempt, 30));
    }
    return !records.length;
  }

  async consume(shardId, checkpointSeq, stop) {
    const { source, leases } = this;
    let last = checkpointSeq;
    const checkpoint = async (s) => { await leases.checkpoint(shardId, s); last = s; };
    let it = await source.iterator(shardId, checkpointSeq, this.position, this.startTs);
    while (!stop.stopped && !this.stopping) {
      let raw;
      try {
        [raw, it] = await source.read(it, this.batchSize);
      } catch (e) {
        if (e?.name === 'ExpiredIteratorException') { it = await source.iterator(shardId, last, this.position, this.startTs); continue; }
        throw e;
      }
      if (this.startTs != null) raw = raw.filter((r) => source.created(r) >= this.startTs);
      const records = raw.map((r) => source.record(r, shardId));
      if (records.length) {
        if (!(await this.deliver(records, checkpoint))) return; // stopping mid-batch: the next owner resumes from the checkpoint
        await checkpoint(source.seq(records.at(-1)));
      }
      if (it === null) {
        await leases.checkpoint(shardId, last, true);
        log('INFO', `shard ${shardId} finished`);
        return;
      }
      if (!records.length) await this.sleep(this.idleWait);
    }
  }

  async runWorker(shardId, checkpointSeq, stop) {
    try {
      await this.consume(shardId, checkpointSeq, stop);
    } catch (e) {
      if (e instanceof LeaseLost) { log('WARN', `lease on ${shardId} was taken by another replica`); return; }
      log('ERROR', `shard ${shardId} failed: ${e?.stack || e}`);
      await this.leases.release(shardId).catch(() => {});
    }
  }

  // One pass: renew, rebalance and acquire leases. Returns the shards this replica holds.
  async balance(now = Date.now() / 1000) {
    for (const [id, w] of [...this.workers]) {
      if (w.done || w.stop.stopped) this.workers.delete(id);
      else if (!(await this.leases.renew(id, now))) { w.stop.stopped = true; this.workers.delete(id); }
    }
    await this.leases.heartbeat(now);
    const shards = await this.source.shards();
    const ids = new Set(shards.map((s) => s.id));
    let snap = await this.leases.snapshot();
    const owners = new Set([this.leases.owner]);
    for (const [k, v] of Object.entries(snap)) if (k.startsWith(MEMBER) && v.expires > now) owners.add(v.owner);
    snap = Object.fromEntries(Object.entries(snap).filter(([k]) => !k.startsWith(MEMBER)));
    const finished = new Set(Object.entries(snap).filter(([, v]) => v.finished).map(([k]) => k));
    const eligible = shards.filter((s) => !finished.has(s.id) && (!s.parent || !ids.has(s.parent) || finished.has(s.parent))).map((s) => s.id);
    const fair = eligible.length ? Math.ceil(eligible.length / owners.size) : 0;
    for (const id of [...this.workers.keys()].filter((k) => eligible.includes(k)).slice(fair)) { // give extras back to new replicas
      const w = this.workers.get(id);
      this.workers.delete(id);
      w.stop.stopped = true;
      await w.promise;
      await this.leases.release(id);
    }
    for (const id of eligible) {
      if (this.workers.size >= fair) break;
      const lease = snap[id] || {};
      if (this.workers.has(id) || (lease.owner && lease.owner !== this.leases.owner && lease.expires > now)) continue;
      if (await this.leases.acquire(id, now)) {
        const w = { stop: { stopped: false }, done: false };
        w.promise = this.runWorker(id, lease.checkpoint || null, w.stop).finally(() => { w.done = true; });
        this.workers.set(id, w);
      }
    }
    return [...this.workers.keys()].sort();
  }

  async run() {
    while (!this.stopping) {
      try {
        await this.balance();
      } catch (e) {
        log('ERROR', `lease coordination failed; retrying: ${e?.message || e}`);
      }
      for (let i = 0; i < RENEW_EVERY_S * 10 && !this.stopping; i++) await new Promise((r) => setTimeout(r, 100));
    }
    for (const [id, w] of this.workers) {
      w.stop.stopped = true;
      await w.promise;
      await this.leases.release(id).catch(() => {});
    }
  }
}

function startTimestamp(raw) {
  if (!raw) return null;
  return /^\d+(\.\d+)?$/.test(raw) ? Number(raw) : Date.parse(raw) / 1000;
}

async function runStream(invoker) {
  const ddb = await sdkCaller('@aws-sdk/client-dynamodb', 'DynamoDBClient');
  let source;
  if (env.L2E_STREAM_KIND === 'kinesis') {
    source = new KinesisSource(await sdkCaller('@aws-sdk/client-kinesis', 'KinesisClient'), env.L2E_STREAM_NAME, env.L2E_STREAM_ARN);
  } else {
    const arn = env.L2E_STREAM_ARN || (await ddb('DescribeTable', { TableName: env.L2E_DDB_TABLE })).Table.LatestStreamArn;
    if (!arn) throw new Error(`table ${env.L2E_DDB_TABLE} has no stream enabled`);
    source = new DynamoDBSource(await sdkCaller('@aws-sdk/client-dynamodb-streams', 'DynamoDBStreamsClient'), arn);
  }
  const owner = `${env.HOSTNAME || 'local'}-${randomUUID().slice(0, 8)}`;
  const leases = new Leases(ddb, env.L2E_LEASE_TABLE, env.L2E_CONSUMER_NAME || FUNCTION_NAME, owner);
  const consumer = new StreamConsumer(source, leases, invoker, {
    batchSize: Number(env.L2E_STREAM_BATCH_SIZE || '100'),
    maxRetry: Number(env.L2E_STREAM_MAX_RETRY || '-1'),
    bisect: env.L2E_STREAM_BISECT === 'true',
    partial: env.L2E_STREAM_REPORT_BATCH_ITEM_FAILURES === 'true',
    position: env.L2E_STREAM_STARTING_POSITION || 'LATEST',
    startTs: startTimestamp(env.L2E_STREAM_START_TIMESTAMP),
  });
  process.on('SIGTERM', () => { log('INFO', 'SIGTERM: checkpointing and releasing leases'); consumer.stopping = true; });
  log('INFO', `stream mode on ${source.kind} ${source.arn} as ${owner}`);
  await consumer.run();
  process.exit(0);
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
  if (mode === 'stream') return runStream(invoker);
  if (mode === 'schedule') return runSchedule(invoker);
  throw new Error(`unknown L2E_MODE ${mode}`);
}

if (import.meta.url === pathToFileURL(process.argv[1] || '').href) {
  main().catch((e) => { log('ERROR', e?.stack || String(e)); process.exit(1); });
}
