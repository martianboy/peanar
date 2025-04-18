import * as uuid from 'uuid';
import debugFn from 'debug';
const debug = debugFn('peanar:app');

import { PeanarInternalError } from './exceptions';
import Broker from './amqplib_compat/broker';
import Worker, { IWorkerResult } from './worker';
import PeanarJob from './job';
import { Writable, TransformCallback } from 'stream';
import { IBasicProperties } from 'ts-amqp/dist/interfaces/Protocol';
import Registry from './registry';
import PeanarTransactor from './transact';
import Consumer from './amqplib_compat/consumer';
import {
  IPeanarJobDefinition,
  IPeanarJobDefinitionInput,
  IPeanarOptions,
  IPeanarRequest,
  IPeanarResponse
} from './types';
export {
  IPeanarJobDefinition,
  IPeanarJobDefinitionInput,
  IPeanarOptions,
  IPeanarRequest,
  IPeanarResponse,
};

interface IWorkerOptions {
  queues?: string[];
  concurrency?: number;
  prefetch?: number;
  outputWritable?: Writable;
  logger?: (msg: string) => void;
}

export enum EAppState {
  RUNNING = 'RUNNING',
  CLOSING = 'CLOSING',
  CLOSED = 'CLOSED'
}

export default class PeanarApp {
  public registry = new Registry;

  public log: (...args: any[]) => any;
  public broker: Broker;
  public jobClass: typeof PeanarJob;

  protected consumers: Map<string, Consumer[]> = new Map;
  protected workers: Map<string, Worker[]> = new Map;
  protected jobs: Map<string, (...args: unknown[]) => Promise<unknown>> = new Map;
  protected transactions: Set<PeanarTransactor> = new Set();

  public state: EAppState = EAppState.RUNNING;

  constructor(options: IPeanarOptions = {}) {
    this.broker = new Broker({
      connection: options.connection,
      poolSize: options.poolSize || 5,
      prefetch: options.prefetch || 1
    });

    this.jobClass = options.jobClass || PeanarJob;
    this.log = options.logger || console.log.bind(console);
  }

  protected async _shutdown(timeout?: number) {
    // Immediately stop receiving new messages
    await Promise.all([...this.consumers.values()].flat().map(c => c.cancel()));
    debug('shutdown(): consumers cancelled');

    // Anything that is subject to the timeout should go in here
    await Promise.all([
      // Wait a few seconds for running jobs to finish their work
      ...[...this.workers.values()].flat().map(w => w.shutdown(timeout)),

      // Also wait for pending transactions to be finalized
      ...[...this.transactions].map(t => t.waitUntil(timeout).catch(() => {
        this.log(`Transaction ${t.queueName} timed out while shutting down Peanar.`);
      })),
    ]);
    debug('shutdown(): workers shut down');
    debug('shutdown(): transactions concluded');

    // Close the channel pool and then the amqp connection
    await this.broker.shutdown();
    debug('shutdown(): broker shut down');
  }

  public async shutdown(timeout?: number) {
    debug('shutdown() called');

    this.state = EAppState.CLOSING;
    await this._shutdown(timeout);
    this.state = EAppState.CLOSED;
  }

  protected _registerWorker(queue: string, worker: Worker) {
    const workers = this.workers.get(queue) || [];
    workers.push(worker);
    this.workers.set(queue, workers);
  }

  protected _registerConsumer(queue: string, consumer: Consumer) {
    const consumers = this.consumers.get(queue) || [];
    consumers.push(consumer);
    this.consumers.set(queue, consumers);
  }

  public call(name: string, args: unknown[]) {
    const enqueue = this.jobs.get(name);

    if (typeof enqueue !== 'function') {
      throw new PeanarInternalError(`Job ${name} is not registered with the app.`);
    }

    return enqueue.apply(this, args);
  }

  protected async _publish(
    routing_key: string,
    exchange: string | undefined,
    def: IPeanarJobDefinition,
    req: IPeanarRequest
  ) {
    if (this.state !== EAppState.RUNNING) {
      throw new PeanarInternalError('PeanarApp::_publish() called while app is not in running state.');
    }

    const properties: IBasicProperties = {
      correlationId: req.correlationId,
      replyTo: def.replyTo
    };

    if (typeof def.expires === 'number') {
      properties.expiration = def.expires.toString();
    }

    if (typeof def.max_priority === 'number') {
      if (typeof req.priority === 'number' && req.priority <= def.max_priority) {
        properties.priority = req.priority;
      } else if (typeof def.default_priority === 'number') {
        properties.priority = def.default_priority;
      }
    }

    await this.broker.publish({
      routing_key,
      exchange,
      properties,
      body: {
        id: req.id,
        name: req.name,
        args: req.args
      }
    });

    return req.id;
  }

  public async enqueueJobRequest(def: IPeanarJobDefinition, req: IPeanarRequest) {
    debug(`Peanar: enqueueJob(${def.queue}:${def.name}})`);

    return this._publish(def.routingKey, def.exchange, def, req);
  }

  protected async _enqueueJobResponse(job: PeanarJob, result: IWorkerResult) {
    debug('Peanar: _enqueueJobResponse()')

    if (!job.def.replyTo) throw new PeanarInternalError('PeanarApp::_enqueueJobResponse() called with no replyTo defined')

    await this.broker.publish({
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

  protected _createTransactor(def: IPeanarJobDefinition) {
    return () => {
      const t = new PeanarTransactor(def, this);
      this.transactions.add(t);
      t.once('conclude', () => this.transactions.delete(t));

      return t;
    };
  }

  protected _createDelayedEnqueuer(def: IPeanarJobDefinition) {
    return (...args: unknown[]) => {
      return this._publish(
        `${def.queue}.delayed`,
        '',
        def,
        this._prepareJobRequest(def.name, args)
      );
    };
  }

  protected _createEnqueuerWithPriority(def: IPeanarJobDefinition) {
    return (priority: number, ...args: unknown[]) => {
      debug(`Peanar: job.enqueueJob('${def.name}').withPriority(${priority})`);
      const req = this._prepareJobRequest(def.name, args);
      req.priority = priority;

      return this.enqueueJobRequest(def, req);
    }
  }

  protected _createEnqueuer(def: IPeanarJobDefinition) {
    const self = this;
    function enqueueJob(...args: unknown[]): Promise<string> {
      debug(`Peanar: job.enqueueJob('${def.name}')`);
      return self.enqueueJobRequest(def, self._prepareJobRequest(def.name, args));
    }

    enqueueJob.rpc = async (...args: unknown[]) => {};
    enqueueJob.delayed = this._createDelayedEnqueuer(def);
    enqueueJob.transaction = this._createTransactor(def);
    enqueueJob.withPriority = this._createEnqueuerWithPriority(def);
    return enqueueJob;
  }

  public job(def: IPeanarJobDefinitionInput) {
    if (def.max_priority && def.default_priority) {
      if (def.default_priority > def.max_priority) {
        throw new Error('max_priority should be greater than or equeal to the default priority.');
      }
    } else if (def.default_priority) {
      def.max_priority = def.default_priority;
    }

    const job_def = this.registry.registerJob(def);
    debug(`Peanar: job('${def.queue}', '${job_def.name}')`);

    const enqueueJob = this._createEnqueuer(job_def);

    this.jobs.set(job_def.name, enqueueJob);
    return enqueueJob;
  }

  public async declareAmqResources() {
    await this.broker.queues(this.registry.queues);
    await this.broker.exchanges(this.registry.exchanges);
    await this.broker.bindings(this.registry.bindings);
  }

  public pauseQueue(queue: string) {
    const consumers = this.consumers.get(queue);
    if (!consumers) return;

    for (const c of consumers) c.pause();
  }

  public resumeQueue(queue: string) {
    const consumers = this.consumers.get(queue);
    if (!consumers) return;

    for (const c of consumers) c.resume();
  }

  protected async _startWorker(queue: string, consumer: Consumer, options?: Omit<IWorkerOptions, 'queues' | 'concurrency'>) {
    const worker = new Worker(this, consumer.channel, queue, {
      logger: options?.logger
    });

    consumer.on('channelChanged', ch => {
      worker.channel = ch;
    });

    this._registerConsumer(queue, consumer);
    this._registerWorker(queue, worker);

    return consumer
      .pipe(worker)
      .pipe(options?.outputWritable ?? new Writable({
        objectMode: true,
        write: (result: IWorkerResult, _encoding: string, cb: TransformCallback) => {
          if (result.status === 'FAILURE') {
            this.log(result.error);
          }

          if (result.job.def.replyTo) {
            this._enqueueJobResponse(result.job, result).then(_ => cb(), ex => cb(ex));
          }
          else {
            return cb();
          }
        }
      }));
  }

  public async worker(options: IWorkerOptions) {
    const { queues, concurrency, prefetch = 1 } = options;

    const worker_queues = (Array.isArray(queues) && queues.length > 0)
      ? queues
      : this.registry.workerQueues;

    const queues_to_start = [...worker_queues].flatMap(q => Array(concurrency).fill(q));

    return Promise.all(this.broker.consumeOver(queues_to_start).map(async p => {
      const { queue, consumer } = await p;

      return this._startWorker(queue, consumer, options);
    }));
  }
}
