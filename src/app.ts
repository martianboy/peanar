import uuid from 'uuid';
import util from 'util';

import { PeanarInternalError } from './exceptions';
import Broker from './broker';
import Worker, { IWorkerResult } from './worker';
import PeanarJob from './job';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';
import { Writable, TransformCallback } from 'stream';
import Consumer from 'ts-amqp/dist/classes/Consumer';
import { IBasicProperties } from 'ts-amqp/dist/interfaces/Protocol';

export interface IPeanarJobDefinitionInput {
  queue: string;
  name?: string;
  routingKey?: string;
  exchange?: string;
  replyTo?: string;
}

export interface IPeanarJobDefinition {
  name: string;
  queue: string;
  handler: (...args: any[]) => Promise<any>;

  routingKey: string;
  exchange?: string;
  replyTo?: string;

  expires?: number;

  max_retries?: number;
}

export interface IPeanarRequest {
  id: string;
  name: string;
  args: any[];
  attempt: number;
  correlationId?: string;
  deliveryTag?: bigint;
}

export interface IPeanarJob extends IPeanarJobDefinition, IPeanarRequest {
  deliveryTag: bigint;
}

export interface IPeanarResponse {
  id: string;
  name: string;
  status: 'SUCCESS' | 'FAILURE';
  error?: unknown;
  result?: unknown;
}

export interface IPeanarOptions {
  connection?: IConnectionParams;
  jobClass: typeof PeanarJob;
  logger?(...args: any[]): any;
}

interface IWorkerOptions {
  queues?: string[];
  concurrency?: number;
  prefetch?: number;
}

enum EAppState {
  RUNNING = 'RUNNING',
  CLOSING = 'CLOSING',
  CLOSED = 'CLOSED'
}

export default class PeanarApp {
  public registry: Map<string, Map<string, IPeanarJobDefinition>> = new Map;

  public log: (...args: any[]) => any;
  public broker: Broker;
  public jobClass: typeof PeanarJob;

  protected consumers: Consumer[] = [];
  protected workers: Worker[] = [];

  public state: EAppState = EAppState.RUNNING;

  constructor(options: IPeanarOptions) {
    this.broker = new Broker(this, options.connection);
    this.jobClass = options.jobClass;
    this.log = options.logger || console.log.bind(console);
  }

  protected async _ensureConnected() {
    this.log('Peanar: ensureConnected()')

    if (this.broker.channel) return this.broker.channel

    return this.broker.connect()
  }

  protected async _shutdown() {
    await Promise.all(this.consumers.map(c => c.cancel()))
    await Promise.all(this.workers.map(w => util.promisify(w.destroy).call(w, undefined)))

    await this.broker.shutdown();
  }

  public async shutdown() {
    this.log('Peanar: shutdown()')

    this.state = EAppState.CLOSING;
    await this._shutdown();
    this.state = EAppState.CLOSED;
  }

  protected _registerJob(fn: (...args: any[]) => Promise<any>, def: IPeanarJobDefinitionInput) {
    this.log(`Peanar: _registerJob('${def.queue}', ${JSON.stringify(def, null, 2)})`)
    
    const job_def: IPeanarJobDefinition = {
      routingKey: def.queue,
      exchange: '',
      ...def,
      name: (def.name && def.name.length) ? def.name : fn.name,
      handler: fn
    }

    let queue_mapping = this.registry.get(job_def.queue)

    if (!queue_mapping) {
      queue_mapping = new Map
      this.registry.set(job_def.queue, queue_mapping)
    }

    if (queue_mapping.has(job_def.name)) {
      throw new PeanarInternalError('Job already registered!')
    }

    queue_mapping.set(job_def.name, job_def)

    return job_def
  }

  public getJobDefinition(queue: string, name: string): IPeanarJobDefinition | undefined {
    const queue_mapping = this.registry.get(queue);
    
    if (!queue_mapping) return;

    return queue_mapping.get(name);
  }

  public async enqueueJob(def: Omit<IPeanarJobDefinition, 'handler'>, req: IPeanarRequest) {
    this.log(`Peanar: _enqueueJob(${JSON.stringify(def, null, 2)}, ${JSON.stringify(req)})`);

    const channel = await this._ensureConnected();
    const bindings = [];

    if (def.exchange) {
      await this.broker.declareExchange(def.exchange);

      bindings.push({
        exchange: def.exchange,
        routing_key: def.queue
      });
    }

    await this.broker.declareQueue(def.queue, bindings);

    const properties: IBasicProperties = {
      correlationId: req.correlationId,
      replyTo: def.replyTo
    };

    if (typeof def.expires === 'number') {
      properties.expiration = def.expires.toString();
    }

    channel.json.write({
      routing_key: def.routingKey,
      exchange: def.exchange,
      properties,
      body: {
        id: req.id,
        name: req.name,
        args: req.args
      }
    });

    return req.id;
  }

  protected async _enqueueJobResponse(job: PeanarJob, result: IWorkerResult) {
    this.log('Peanar: _enqueueJobResponse()')

    if (!job.def.replyTo) throw new PeanarInternalError('PeanarApp::_enqueueJobResponse() called with no replyTo defined')

    const channel = await this._ensureConnected()
    await this.broker.declareQueue(job.def.replyTo)

    channel.json.write({
      routing_key: job.def.replyTo,
      exchange: '',
      properties: {
        correlationId: job.correlationId || job.id
      },
      body: this._prepareJobResponse(job, result)
    });
  }

  protected _prepareJobRequest(name: string, args: any[]): IPeanarRequest {
    return {
      id: uuid.v4(),
      name,
      args,
      attempt: 1
    };
  }

  protected _prepareJobResponse(job: PeanarJob, result: IWorkerResult): IPeanarResponse {
    const res: IPeanarResponse = {
      id: job.id,
      name: job.name,
      status: result.status,
    };

    if (result.status === 'SUCCESS') {
      res.result = result.result;
    }
    else {
      res.error = result.error;
    }

    return res;
  }

  public job(fn: (...args: any[]) => Promise<any>, def: IPeanarJobDefinitionInput) {
    const job_name = (def.name && def.name.length) ? def.name : fn.name

    this.log(`Peanar: job('${def.queue}', '${job_name}')`)

    const job_def = this._registerJob(fn, def)

    const self = this

    function enqueueJob() {
      self.log(`Peanar: job.enqueueJobLater('${job_name}', ${[...arguments]})`)
      return self.enqueueJob(job_def, self._prepareJobRequest(job_name, [...arguments]))
    }

    enqueueJob.rpc = async function() {}

    return enqueueJob;
  }

  protected async _startWorker(queue: string) {
    const channel = await this._ensureConnected();
    await this.broker.declareQueue(queue);

    const consumer = await channel.basicConsume(queue);
    const worker = new Worker(this, channel, queue)

    this.consumers.push(consumer);
    this.workers.push(worker);

    return consumer
      .pipe(worker)
      .pipe(new Writable({
        objectMode: true,
        write: (result: IWorkerResult, _encoding: string, cb: TransformCallback) => {
          if (result.job.def.replyTo) {
            this._enqueueJobResponse(result.job, result).then(_ => cb(), ex => cb(ex));
          }
          else {
            return cb()
          }
        }
      }));
  }

  public async worker(options: IWorkerOptions) {
    const { queues, concurrency, prefetch = 1 } = options;

    await this._ensureConnected();
    await this.broker.prefetch(prefetch);

    const worker_queues = (Array.isArray(queues) && queues.length > 0)
      ? queues
      : [...this.registry.keys()];

    await Promise.all(worker_queues.map(q => this.broker.declareQueue(q)));

    const queues_to_start = worker_queues.flatMap(q => Array(concurrency).fill(q));

    return Promise.all(queues_to_start.map(q => this._startWorker(q)));
  }
}
