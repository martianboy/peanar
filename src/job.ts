import debugFn from 'debug';
const debug = debugFn('peanar:job');

import { EventEmitter } from "events";
import { PeanarAdapterError, PeanarJobError, PeanarJobCancelledError, PeanarInternalError } from "./exceptions";
import PeanarApp from "./app";
import { Channel } from 'amqplib';
import { IPeanarRequest, IPeanarJobDefinition } from "./types";

export default class PeanarJob extends EventEmitter {
  public id: string;
  public name: string;
  public args: any[];
  public handler: (...args: any[]) => Promise<any>;
  public deliveryTag?: bigint;
  public channel: Channel;
  public app: PeanarApp;
  public def: IPeanarJobDefinition;

  public attempt: number;
  public max_retries: number;

  public cancelled: boolean = false;

  constructor(
    req: IPeanarRequest,
    def: IPeanarJobDefinition,
    app: PeanarApp,
    channel: Channel
  ) {
    super()

    this.def = def;

    this.id = req.id;
    this.name = req.name;
    this.args = req.args;
    this.deliveryTag = req.deliveryTag;
    this.attempt = req.attempt;
    this.max_retries = def.max_retries || 0;

    this.handler = def.handler;

    this.channel = channel;
    this.app = app;
  }

  cancel(reason?: Error) {
    this.cancelled = true;
    this.emit('cancel', new PeanarJobCancelledError(reason));
  }

  ack() {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    if (!this.cancelled) {
      // @ts-ignore
      this.channel.ack({ fields: { deliveryTag: Number(this.deliveryTag) } }, false);
      debug(`PeanarJob#${this.id}: Acknowledged!`);
    }
  }

  canRetry() {
    return (
      typeof this.def.retry_exchange === 'string' &&
      (this.max_retries < 0 || this.attempt <= this.max_retries)
    );
  }

  async reject(ex?: Error & { retry?: boolean; }) {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    if (ex?.retry !== false && this.canRetry()) {
      debug(`PeanarJob#${this.id}: Trying again...`);
      await this._declareRetryQueues();

      debug(`PeanarJob#${this.id}: Rejecting to retry queue...`);
      // @ts-ignore
      this.channel.reject({ fields: { deliveryTag: Number(this.deliveryTag) } }, false);
    } else {
      if (!this.def.error_exchange || this.def.error_exchange.length < 1) {
        debug(`PeanarJob#${this.id}: No retries left. Discarding...`);
        // @ts-ignore
        return this.channel.reject({ fields: { deliveryTag: Number(this.deliveryTag) } }, false);
      }

      // No attempts left. Publish to error exchange for manual investigation.
      debug(`PeanarJob#${this.id}: No retries. Writing to error exchange!`);

      this.channel.publish(
        this.def.error_exchange,
        this.def.routingKey,
        Buffer.from(JSON.stringify({
          id: this.id,
          name: this.name,
          error: ex?.message,
          args: this.args
        })),
        {
          contentType: 'application/json'
        }
      );

      return this.ack();
    }
  }

  enqueue() {
    return this.app.enqueueJobRequest(this.def, {
      id: this.id,
      args: this.args,
      name: this.name,
      attempt: 1,
    });
  }

  protected get retry_name() {
    return `${this.def.queue}.retry`;
  }

  protected get requeue_name() {
    return `${this.def.queue}.retry-requeue`;
  }

  protected async _declareRetryQueues() {
    if (!this.def.retry_exchange) throw new PeanarInternalError('Attempting retry without a retry_exchange specified.');

    const retry_name = this.retry_name;
    const requeue_name = this.requeue_name;

    try {
      debug(`declare retry queue ${retry_name}`);
      await this.app.broker.queues([{
        name: retry_name,
        arguments: {
          expires: 2 * (this.def.retry_delay || 60000),
          messageTtl: this.def.retry_delay || 60000,
          deadLetterExchange: requeue_name
        },
        auto_delete: false,
        durable: true,
        exclusive: false
      }]);

      debug(`bind retry exchange ${this.def.retry_exchange} to retry queue ${retry_name}`);
      await this.app.broker.bindings([{
        exchange: this.def.retry_exchange,
        queue: retry_name,
        routing_key: '#'
      }]);

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
          debug(`PeanarJob#${this.id}: (callResolve) not finished. all good. resolving...`);
          resolve(result);
        }
      }

      const callReject = (ex: unknown) => {
        if (!already_finished) {
          already_finished = true;
          debug(`PeanarJob#${this.id}: (callReject) not finished. rejecting.`);
          reject(ex);
        } else {
          debug(`PeanarJob#${this.id}: (callReject) already finished somehow! when did it finish?!`);
        }
      }

      this._perform().then(callResolve, callReject);
      this.once('cancel', callReject);
    });
  }
}
