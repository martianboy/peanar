import { PeanarInternalError } from './exceptions';
import {
  EExchangeType,
  IBinding,
  IExchange,
  IPeanarJobDefinition,
  IPeanarJobDefinitionInput,
  IQueue,
  IQueueArgs
} from './types';

export default class Registry {
  private _jobs: Map<string, IPeanarJobDefinition> = new Map();

  private _workerQueues: Set<string> = new Set();
  private _queues: Map<string, IQueue> = new Map();
  private _exchanges: Map<string, IExchange> = new Map();
  private _bindings: Set<string> = new Set();

  private _prepareJobDefinition(def: IPeanarJobDefinitionInput): IPeanarJobDefinition {
    return {
      routingKey: def.queue,
      exchange: '',
      ...def,
      name: def.name && def.name.length ? def.name : def.handler.name
    };
  }

  private registerQueue(queue: string, args: IQueueArgs, worker: boolean = false) {
    this._queues.set(queue, {
      name: queue,
      arguments: args,
      auto_delete: false,
      durable: true,
      exclusive: false
    });

    if (worker) {
      this._workerQueues.add(queue);
    }
  }

  private registerExchange(exchange: string, type: EExchangeType = 'direct') {
    if (!this._exchanges.has(exchange)) this._exchanges.set(exchange, {
      name: exchange,
      type,
      durable: true
    });
  }

  private registerBinding(exchange: string, routingKey: string, queue: string) {
    this._bindings.add(`${exchange}:${routingKey}:${queue}`);
  }

  private extractAmqStructure(def: IPeanarJobDefinition) {
    const main_queue_args: IQueueArgs = {};

    if (def.retry_exchange && def.retry_exchange.length > 0) {
      this.registerExchange(def.retry_exchange, 'topic');

      main_queue_args.deadLetterExchange = def.retry_exchange;
      main_queue_args.deadLetterRoutingKey = '#'
    }

    if (def.max_priority) {
      main_queue_args.maxPriority = def.max_priority;
    }

    this.registerQueue(def.queue, main_queue_args, true);

    this.registerExchange(`${def.queue}.retry-requeue`, 'topic');
    this.registerBinding(`${def.queue}.retry-requeue`, '#', def.queue);

    if (Number(def.delayed_run_wait) > 0) {
      this.registerQueue(`${def.queue}.delayed`, {
        messageTtl: def.delayed_run_wait,
        deadLetterExchange: `${def.queue}.retry-requeue`,
        deadLetterRoutingKey: '#'
      }, true);
    }

    if (def.exchange && def.exchange.length > 0) {
      this.registerExchange(def.exchange);
      this.registerBinding(def.exchange, def.routingKey, def.queue);
    }

    if (def.error_exchange && def.error_exchange.length > 0) {
      this.registerExchange(def.error_exchange, 'topic');
      this.registerQueue(`${def.queue}.error`, {}, false);
      this.registerBinding(def.error_exchange, '#', `${def.queue}.error`);
    }
  }

  public registerJob(def: IPeanarJobDefinitionInput): IPeanarJobDefinition {
    const job_def = this._prepareJobDefinition(def);

    if (!job_def.name || job_def.name.length < 1)
      throw new PeanarInternalError('Invalid job name.');

    this._jobs.set(job_def.name, job_def);

    this.extractAmqStructure(job_def);

    return job_def;
  }

  get workerQueues(): Set<string> {
    return this._workerQueues;
  }

  get queues(): IQueue[] {
    return Array.from(this._queues.values());
  }

  get exchanges(): IExchange[] {
    return Array.from(this._exchanges.values());
  }

  get bindings(): IBinding[] {
    return [...this._bindings.values()].map(b => {
      const [exchange, routing_key, queue] = b.split(':');

      return {
        exchange,
        routing_key,
        queue
      };
    });
  }

  public getJobDefinition(name: string): IPeanarJobDefinition {
    const def = this._jobs.get(name);

    if (!def) {
      throw new PeanarInternalError(`Job definition for ${name} not found.`);
    }

    return def;
  }
}
