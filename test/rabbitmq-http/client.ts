import debugFn from 'debug';
const debug = debugFn('rabbitmq-http');

const HOST = process.env.RABBITMQ_HOST ?? '127.0.0.1';
const PORT = process.env.RABBITMQ_MANAGEMENT_PORT ?? 15672;
const SSL_MODE = process.env.RABBITMQ_SSL_MODE === 'true';
const MANAGEMENT_API_SCHEME = SSL_MODE ? 'https' : 'http';
const BASE_URL = `${MANAGEMENT_API_SCHEME}://${HOST}:${PORT}/api`;
const credentials = Buffer.from('guest:guest').toString('base64');

interface FetchOptions {
  path: string;
  method?: string;
  qs?: Record<string, string>;
  headers?: Record<string, string>;
  body?: string | Record<string, unknown>;
}

class StatusCodeError extends Error {
  public body: unknown;

  constructor(status: number, statusText: string, body?: unknown) {
    super(`${status} ${statusText}`);
    this.body = body;
  }
}

async function request(options: FetchOptions) {
  if (!HOST) {
    throw new Error('RABBITMQ_HOST must be set via environment variables.');
  }

  const headers = new Headers();
  headers.append('Content-Type', 'application/json');
  headers.append('Authorization', `Basic ${credentials}`);
  for (const k of Object.keys(options.headers || {})) {
    headers.append(k, options.headers![k]);
  }

  const url = new URL(BASE_URL + options.path);
  if (options.qs) {
    for (const k of Object.keys(options.qs)) {
      url.searchParams.append(k, options.qs[k]);
    }
  }

  debug(`${options.method} ${url.toString()}`);
  const req: RequestInit = {
    method: options.method,
    headers
  };
  if (options.body) {
    if (typeof options.body === 'string') {
      req.body = options.body;
    } else {
      req.body = JSON.stringify(options.body);
    }
  }

  const resp = await fetch(url.toString(), req);
  if (resp.status >= 400) {
    const body = await resp.text();
    throw new StatusCodeError(resp.status, resp.statusText, body);
  }

  return resp;
}

async function getList<T = any>(options: FetchOptions): Promise<T[]> {
  const resp = await request({
    path: options.path,
    method: 'GET',
    qs: { page_size: '50', page: '1', ...options.qs}
  });

  const body = (await resp.json()) as any;
  return (body?.items as T[]) ?? body as T[];
}

export function getNodes(): Promise<any[]> {
  return getList({ path: '/nodes' });
}

interface RabbitMQConnection {
  name: string;
}
export function getConnections(): Promise<RabbitMQConnection[]> {
  return getList({ path: '/connections' });
}

export function getChannels() {
  return getList({ path: '/channels' });
}

export function getVhosts() {
  return getList({ path: '/vhosts' });
}

export async function createVhost(name: string) {
  return request({
    path: '/vhosts/' + name,
    method: 'PUT'
  });
}

export async function deleteVhost(name: string) {
  return request({
    path: '/vhosts/' + name,
    method: 'DELETE'
  });
}

export function closeConnection(name: string) {
  return request({
    path: '/connections/' + name,
    method: 'DELETE',
    headers: {
      'X-Reason': 'Server initiated closure'
    }
  });
}

export function closeAllUserConnection(user: string) {
  return request({
    path: '/connections/username/' + user,
    method: 'DELETE',
    headers: {
      'X-Reason': 'Server initiated closure'
    }
  });
}

export function getVhostConnections(vhost: string): Promise<RabbitMQConnection[]> {
  return getList({ path: `/vhosts/${vhost}/connections` });
}

export function getQueues(options: { enable_queue_totals?: boolean; disable_stats?: boolean } = {}): Promise<any[]> {
  const qs = Object.fromEntries(Object.entries(options).map(([k, v]) => {
    return [k, v.toString()];
  }));
  return getList({ path: '/queues', qs });
}

export function upsertQueue(
  name: string,
  vhost: string,
  node: string,
  options: { durable?: boolean; auto_delete?: boolean; arguments?: Record<string, any> } = {}): Promise<any>
{
  return request({
    method: 'PUT',
    path: `/queues/${vhost}/${name}`,
    body: {
      name,
      vhost,
      node,
      ...options
    }
  });
}

export function getVhostQueues(vhost: string, options: { enable_queue_totals?: boolean; disable_stats?: boolean } = {}): Promise<any[]> {
  const qs = Object.fromEntries(Object.entries(options).map(([k, v]) => {
    return [k, v.toString()];
  }));
  return getList({ path: `/queues/${vhost}`, qs });
}

export function getVhostExchanges(vhost: string): Promise<any[]> {
  return getList({ path: `/exchanges/${vhost}` });
}

export function getBindings(vhost: string, queue: string): Promise<any[]> {
  return getList({ path: `/queues/${vhost}/${queue}/bindings`});
}

export async function getChannelsOnConnection(name: string): Promise<any[]> {
  const resp = await request({ path: `/connections/${encodeURIComponent(name)}/channels` });
  const body = await resp.json() as any[];
  return body;
}
