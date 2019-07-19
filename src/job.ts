import ChannelN from "ts-amqp/dist/classes/ChannelN";
import { PeanarAdapterError, PeanarJobError } from "./exceptions";
import PeanarApp, { IPeanarRequest, IPeanarJobDefinition, IPeanarJob } from "./app";

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

export default class PeanarJob {
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

  constructor(
    req: IPeanarRequest,
    def: IPeanarJobDefinition,
    app: PeanarApp,
    channel: ChannelN
  ) {
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

  ack() {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    this.channel.basicAck(this.deliveryTag, false);
    this.app.log(`PeanarJob#${this.id}: Acknowledged!`);
  }

  async reject() {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    if (this.max_retries < 0 || this.attempt <= this.max_retries) {
      this.app.log(`PeanarJob#${this.id}: Trying again...`);
      await this._declareRetryQueues();

      this.app.log(`PeanarJob#${this.id}: Rejecting to retry queue...`);
      this.channel.basicReject(this.deliveryTag, false);
    } else {
      // No attempts left. Publish to error exchange for manual investigation.
      this.app.log(`PeanarJob#${this.id}: No retries. Writing to error exchange!`);

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
    return this.app.enqueueJob(this.def, {
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

  protected async _declareRetryExchanges() {
    const retry_name = this.retry_name;
    const error_name = this.error_name;
    const requeue_name = this.requeue_name;

    for (const name of [retry_name, error_name, requeue_name]) {
      await this.app.broker.declareExchange(name, 'topic');
    }
  }

  protected _declareRetryQueue() {
    const retry_name = this.retry_name;
    const requeue_name = this.requeue_name;

    return this.app.broker.declareQueue(retry_name, {
      expires: 120000,
      messageTtl: 60000,
      deadLetterExchange: requeue_name
    }, [{
      exchange: retry_name,
      routingKey: '#'
    }]);
  }

  protected _declareErrorQueue() {
    const error_name = this.error_name;

    return this.app.broker.declareQueue(error_name, {}, [{
      exchange: error_name,
      routingKey: '#'
    }]);
  }

  protected _bindToRequeueExchange() {
    const requeue_name = this.requeue_name;

    return this.channel.bindQueue({
      exchange: requeue_name,
      queue: this.def.queue,
      routing_key: '#'
    });
  }

  protected async _declareRetryQueues() {
    await this._declareRetryExchanges();
    await this._declareRetryQueue();
    await this._declareErrorQueue();
    await this._bindToRequeueExchange();
  }

  retry() {
    if (!this.max_retries || this.attempt >= this.max_retries) {
      console.warn('Illegal PeanarJob.retry()!');
    }

    return this.app.enqueueJob(this.def, {
      id: this.id,
      args: this.args,
      name: this.name,
      attempt: this.attempt + 1,
      correlationId: this.correlationId
    });
  }

  async perform() {
    return await this.handler.apply(null, this.args);
  }
}
