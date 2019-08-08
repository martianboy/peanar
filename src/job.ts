import debugFn from 'debug';
const debug = debugFn('peanar:job');

import { EventEmitter } from "events";
import ChannelN from "ts-amqp/dist/classes/ChannelN";
import { PeanarAdapterError, PeanarJobError, PeanarJobCancelledError, PeanarInternalError } from "./exceptions";
import PeanarApp, { IPeanarRequest, IPeanarJobDefinition } from "./app";

const fib_seq = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584];

function fib(n: number, m: number = 1): number {
  if (n > fib_seq.length) {
    for (let i = fib_seq.length; i < n; i++) {
      fib_seq[i] = fib_seq[i - 1] + fib_seq[i - 2];
    }
  }

  return m * fib_seq[n - 1];
}

function exponential(n: number, m: number = 1) {
  return Math.round(
    m * 0.5 * (Math.pow(2, n) - 1)
  );
}

export default class PeanarJob extends EventEmitter {
  public id: string;
  public name: string;
  public args: any[];
  public handler: (...args: any[]) => Promise<any>;
  public deliveryTag?: bigint;
  public correlationId?: string;
  public channel: ChannelN;
  public app: PeanarApp;
  public def: IPeanarJobDefinition;

  public attempt: number;
  public max_retries: number;

  public cancelled: boolean = false;

  constructor(
    req: IPeanarRequest,
    def: IPeanarJobDefinition,
    app: PeanarApp,
    channel: ChannelN
  ) {
    super()

    this.def = def;

    this.id = req.id;
    this.name = req.name;
    this.args = req.args;
    this.correlationId = req.correlationId || req.id;
    this.deliveryTag = req.deliveryTag;
    this.attempt = req.attempt;
    this.max_retries = def.max_retries || 0;

    this.handler = def.handler;

    this.channel = channel;
    this.app = app;
  }

  cancel(reason?: string | Error) {
    this.cancelled = true;
    this.emit('cancel', reason);
  }

  ack() {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    if (!this.cancelled) {
      this.channel.basicAck(this.deliveryTag, false);
      debug(`PeanarJob#${this.id}: Acknowledged!`);
    }
  }

  async reject() {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    if (this.max_retries < 0 || this.attempt <= this.max_retries) {
      debug(`PeanarJob#${this.id}: Trying again...`);
      await this._declareRetryQueues();

      debug(`PeanarJob#${this.id}: Rejecting to retry queue...`);
      this.channel.basicReject(this.deliveryTag, false);
    } else {
      if (!this.def.error_exchange || this.def.error_exchange.length < 1) {
        debug(`PeanarJob#${this.id}: No retries left. Discarding...`);
        return;
      }

      // No attempts left. Publish to error exchange for manual investigation.
      debug(`PeanarJob#${this.id}: No retries. Writing to error exchange!`);

      this.channel.json.write({
        routing_key: this.def.routingKey,
        exchange: this.error_name,
        properties: {
          correlationId: this.correlationId,
          replyTo: this.def.replyTo
        },
        body: {
          id: this.id,
          name: this.name,
          args: this.args
        }
      });

      return this.ack();
    }
  }

  enqueue() {
    return this.app.enqueueJobRequest(this.def, {
      id: this.id,
      args: this.args,
      name: this.name,
      attempt: 1,
      correlationId: this.correlationId
    });
  }

  protected get retry_name() {
    return `${this.def.queue}.retry`;
  }

  protected get requeue_name() {
    return `${this.def.queue}.retry-requeue`;
  }

  protected get error_name() {
    return `${this.def.queue}.error`;
  }

  protected async _declareRetryQueues() {
    if (!this.def.retry_exchange) throw new PeanarInternalError('Attempting retry without a retry_exchange specified.');

    const retry_name = this.retry_name;
    const requeue_name = this.requeue_name;

    try {
      debug(`declare retry queue ${retry_name}`);
      await this.channel.declareQueue({
        name: retry_name,
        arguments: {
          expires: 2 * (this.def.retry_delay || 60000),
          messageTtl: this.def.retry_delay || 60000,
          deadLetterExchange: requeue_name
        },
        auto_delete: false,
        durable: true,
        exclusive: false
      });

      debug(`bind retry exchange ${this.def.retry_exchange} to retry queue ${retry_name}`);
      await this.channel.bindQueue({
        exchange: this.def.retry_exchange,
        queue: retry_name,
        routing_key: '#'
      });

      debug('_declareRetryQueues(): done');
    }
    catch (ex) {
      console.error(ex);
      throw ex;
    }
  }

  retry() {
    if (!this.max_retries || this.attempt >= this.max_retries) {
      console.warn('Illegal PeanarJob.retry()!');
    }

    return this.app.enqueueJobRequest(this.def, {
      id: this.id,
      args: this.args,
      name: this.name,
      attempt: this.attempt + 1,
      correlationId: this.correlationId
    });
  }

  pauseQueue() {
    return this.app.pauseQueue(this.def.queue);
  }

  resumeQueue() {
    return this.app.resumeQueue(this.def.queue);
  }

  _perform() {
    return this.handler.apply(this, this.args)
  }

  perform() {
    let already_finished = false;

    return new Promise((resolve, reject) => {
      const callResolve = (result: unknown) => {
        if (!already_finished) {
          already_finished = true;
          resolve(result);
        }
      }

      const callReject = (ex: unknown) => {
        if (!already_finished) {
          already_finished = true;
          reject(ex);
        }
      }

      this._perform().then(callResolve, callReject);
      this.on('cancel', callReject);
    });
  }
}
