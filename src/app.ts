import uuid from 'uuid';

import { PeanarInternalError } from './exceptions';
import Broker from './broker';
import Consumer from './consumer';
import Worker from './worker';
import PeanarJob from './job';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';

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
}

export interface IPeanarRequest {
  id: string;
  name: string;
  args: any[];
  correlationId?: string;
}

export interface IPeanarJob extends IPeanarJobDefinition, IPeanarRequest {
  deliveryTag: bigint;
}

export interface IPeanarOptions {
  connection?: IConnectionParams;
  jobClass: typeof PeanarJob;
  logger?(...args: any[]): any;
}

interface IWorkerOptions {
  queues?: string[];
  concurrency?: number;
}

export default class PeanarApp {
  public registry: Map<string, Map<string, IPeanarJobDefinition>> = new Map;

  public log: (...args: any[]) => any;
  public broker: Broker;
  public jobClass: typeof PeanarJob;

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

  public async shutdown() {
    this.log('Peanar: shutdown()')

    await this.broker.shutdown()
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
    const queue_mapping = this.registry.get(queue)
    
    if (!queue_mapping) return

    return queue_mapping.get(name)
  }

  protected async _enqueueJob(def: Omit<IPeanarJobDefinition, 'handler'>, req: IPeanarRequest) {
    this.log(`Peanar: _enqueueJob(${JSON.stringify(def, null, 2)}, ${JSON.stringify(req)})`)

    const channel = await this._ensureConnected()
    if (def.exchange) await this.broker.declareExchange(def.exchange)
    await this.broker.declareQueue(def.queue)

    channel.json.write({
      routing_key: def.routingKey,
      properties: {
        correlationId: req.correlationId,
        replyTo: def.replyTo
      },
      body: {
        id: req.id,
        name: req.name,
        args: req.args
      }
    })

    return req.id
  }

  protected _prepareJobRequest(name: string, args: any[]): IPeanarRequest {
    return {
      id: uuid.v4(),
      name,
      args,
    }
  }

  public job(fn: (...args: any[]) => Promise<any>, def: IPeanarJobDefinitionInput) {
    const job_name = (def.name && def.name.length) ? def.name : fn.name

    this.log(`Peanar: job('${def.queue}', '${job_name}')`)

    const job_def = this._registerJob(fn, def)

    const self = this

    function enqueueJob() {
      self.log(`Peanar: job.enqueueJobLater('${job_name}', ${[...arguments]})`)
      return self._enqueueJob(job_def, self._prepareJobRequest(job_name, [...arguments]))
    }

    enqueueJob.rpc = async function() {

    }

    return enqueueJob;
  }

  protected async _startWorker(queue: string) {
    const channel = await this._ensureConnected();
    await this.broker.declareQueue(queue);

    const consumer = await channel.basicConsume(queue);

    return consumer
      .pipe(new Consumer(this, channel, queue))
      .pipe(new Worker(this));
  }

  public async worker(options: IWorkerOptions) {
    const { queues, concurrency } = options;

    await this._ensureConnected();
    await this.broker.prefetch(1)
    
    const worker_queues = (Array.isArray(queues) && queues.length > 0)
      ? queues
      : [...this.registry.keys()]

    await Promise.all(worker_queues.map(q => this.broker.declareQueue(q)))

    const queues_to_start = worker_queues.flatMap(q => Array(concurrency).fill(q))

    await Promise.all(queues_to_start.map(q => this._startWorker(q)))
  }
}
