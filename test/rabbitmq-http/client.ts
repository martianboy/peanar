import debugFn from 'debug';
const debug = debugFn('rabbitmq-http');

const HOST = process.env.RABBITMQ_HOST ?? '127.0.0.1';
const PORT = 15672;
const BASE_URL = `http://${HOST}:${PORT}/api`;
const credentials = Buffer.from('guest:guest').toString('base64');

interface FetchOptions {
  path: string;
  method?: string;
  qs?: Record<string, string>;
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

  const url = new URL(BASE_URL + options.path);
  if (options.qs) {
    for (const k of Object.keys(options.qs)) {
      url.searchParams.append(k, options.qs[k]);
    }
  }

  debug(`${options.method} ${url.toString()}`);
  const resp = await fetch(url.toString(), {
    method: options.method,
    headers
  });

  if (resp.status >= 400) {
    const body = await resp.text();
    throw new StatusCodeError(resp.status, resp.statusText, body);
  }

  return resp;
}

async function getList(options: FetchOptions) {
  return request({
    path: options.path,
    method: 'GET',
    // qs: { page_size: '50', page: '1', ...options.qs}
  });
}

export async function getConnections() {
  const resp = await getList({ path: '/connections' });
  return resp.json();
}

export async function getChannels() {
  const resp = await getList({ path: '/channels' });
  return resp.json();
}

export async function getVhosts() {
  const resp = await getList({ path: '/vhosts' });
  return resp.json();
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
