import debugFn from 'debug';
const debug = debugFn('peanar:worker');

import util from 'util';
import 'colors';

import { Transform, TransformCallback } from 'stream';
import PeanarApp, { IPeanarRequest, IPeanarJob } from './app';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';
import PeanarJob from './job';
import { PeanarInternalError, PeanarJobCancelledError } from './exceptions';
import CloseReason from 'ts-amqp/dist/utils/CloseReason';
import { Channel } from 'amqplib';

export type IWorkerResult = {
  status: 'SUCCESS';
  job: PeanarJob;
  result: unknown;
} | {
  status: 'FAILURE';
  job: PeanarJob;
  error: unknown;
}

export interface IDeathInfo {
  count: bigint;
  reason: string;
  queue: string;
  time: bigint;
  exchange: string,
  'routing-keys': string[]
}

enum EWorkerState {
  IDLE = 'IDLE',
  WORKING = 'WORKING',
  CLOSING = 'CLOSING',
  CLOSED = 'CLOSED',
}

let counter = 0;
const SHUTDOWN_TIMEOUT = 10000;

const _to_ack = new Set<string>();

export default class PeanarWorker extends Transform {
  private app: PeanarApp;
  private _channel: Channel;
  private queue: string;
  private n: number;
  private state: EWorkerState = EWorkerState.IDLE;
  private activeJob?: PeanarJob;

  private destroy_cb?: (err: Error | null) => void;
  private _destroy_timeout?: NodeJS.Timeout;
  private _shutdown_timeout: number = SHUTDOWN_TIMEOUT;
  private _channel_lost: boolean = false;

  constructor(app: PeanarApp, channel: Channel, queue: string) {
    super({
      objectMode: true
    });

    this.app = app;
    this.queue = queue;
    this.n = counter++;

    this._channel = channel;
    this._channel.once('close', this.onChannelClosed);
  }

  get channel() { return this._channel; }
  set channel(ch) {
    if (this._channel) {
      this._channel.off('close', this.onChannelClosed);
    }

    this._channel_lost = false;

    this._channel = ch;
    this._channel.once('close', this.onChannelClosed);
    this.emit('channelChanged', ch);
    this.log('channel changed!');
  }

  onChannelClosed = (err: CloseReason) => {
    this._channel_lost = true;
    // if (this.activeJob) this.activeJob.cancel(err || 'Channel closed');
  }

  async shutdown(timeout?: number) {
    if (timeout) this._shutdown_timeout = timeout;

    const destroy = util.promisify(this.destroy);
    await destroy.call(this, undefined);
  }

  _destroy(error: Error | null, callback: (error: Error | null) => void) {
    if (this.state === EWorkerState.IDLE) {
      this.state = EWorkerState.CLOSED;
      this.log('Worker state: Closed');
      return callback(null);
    }
    else {
      this.state = EWorkerState.CLOSING;

      this.destroy_cb = (err) => {
        if (this.activeJob) {
          this.log('cancelling active job');
          this.activeJob.cancel();
        }
        this.state = EWorkerState.CLOSED;
        this.log('Worker state: Closed');
        setImmediate(() => callback(err));
      }
      this._destroy_timeout = setTimeout(this.destroy_cb, this._shutdown_timeout);
    }
  }

  public log(msg: string) {
    return debug(`${`PeanarWorker#${this.n}:`.bold} ${msg}`);
  }

  getJobDefinition(name: string) {
    try {
      return this.app.registry.getJobDefinition(name);
    } catch (ex) {
      if (ex instanceof PeanarInternalError) {}
      else throw ex;
    }
  }

  _parseBody(body: Buffer): IPeanarRequest | null {
    try {
      return JSON.parse(body.toString('utf-8'))
    }
    catch (ex) {
      return null
    }
  }

  private _getJob(delivery: IDelivery) {
    if (!delivery.body) {
      console.warn('PeanarWorker#_getJob: Delivery without body!')
      return
    }

    const body = this._parseBody(delivery.body)
    if (!body || !body.name || body.name.length < 1) {
      console.warn('PeanarWorker#_getJob: Invalid message body!')
      return
    }

    debug(`_getJob(${body.name})`);

    const def = this.getJobDefinition(body.name);
    if (!def) {
      console.warn(`PeanarWorker#_getJob: No handler registered for ${this.queue}.${body.name}!`)
      return
    }

    const headers = delivery.properties.headers;
    const deathInfo = headers && Array.isArray(headers['x-death'])
      ? (headers['x-death'] as IDeathInfo[]).find(d => d.queue === def.queue)
      : undefined;

    const req: IPeanarJob = {
      ...def,
      deliveryTag: delivery.envelope.deliveryTag,
      replyTo: delivery.properties.replyTo,
      correlationId: delivery.properties.correlationId,
      queue: this.queue,
      args: body.args,
      id: body.id,
      attempt: deathInfo ? Number(deathInfo.count) + 1 : 1
    };

    if (delivery.properties.replyTo) {
      req.replyTo = delivery.properties.replyTo;
      req.correlationId = delivery.properties.correlationId;
    }

    return new this.app.jobClass(req, def, this.app, this._channel);
  }

  private async run(job: PeanarJob) {
    this.log('run()');
    this.activeJob = job;

    try {
      const result = await job.perform();
      _to_ack.add(job.id);
      this.log(`_to_ack.add(${job.id});`);

      this.push({
        status: 'SUCCESS',
        job,
        result
      });

      this.log(`Job ${job.name}:${job.id} SUCCESS!`);

      job.ack();
      _to_ack.delete(job.id);
      this.log(`_to_ack.delete(${job.id});`);

      this.log(`Job ${job.name}:${job.id} was acked.`);
    } catch (ex) {
      if (ex instanceof PeanarJobCancelledError) {
        this.log(`job ${job.id} was cancelled.`);
        return;
      }

      if (ex.name === 'IllegalOperationError') {
        this.log(`Channel closed on ack for job ${job.id}. It will be acked next time it's delivered.`);
        return;
      }

      this.push({
        status: 'FAILURE',
        job,
        error: ex
      });

      this.log(`Job ${job.name}:${job.id} FAILURE!`);

      await job.reject(ex);
      this.log(`Job ${job.name}:${job.id} was rejected.`);
    } finally {
      this.activeJob = undefined;
    }
  }

  _transform(delivery: IDelivery, _encoding: string, cb: TransformCallback) {
    if (this.app.state === "CLOSING" || this.app.state === "CLOSED") {
      this.log(`Received job ${Number(delivery.envelope.deliveryTag)} while shutting down. Holding on to it...`);
      return cb();
    }

    const done = (ex?: Error | null) => {
      this.log('Worker state: Idle');
      this.state = EWorkerState.IDLE;
      cb(ex);

      if (this.destroy_cb) {
        this.destroy_cb(null);
      }
      if (this._destroy_timeout) {
        clearTimeout(this._destroy_timeout);
        this._destroy_timeout = undefined;
      }
    }

    const startProcessing = () => {
      this.state = EWorkerState.WORKING;

      let job = undefined;
      try {
        job = this._getJob(delivery);
      } catch (ex) {
        // @ts-ignore
        this.channel.reject({ fields: { deliveryTag: Number(delivery.envelope.deliveryTag) } }, false);
        return done(ex);
      }

      if (!job) {
        // @ts-ignore
        this.channel.reject({ fields: { deliveryTag: Number(delivery.envelope.deliveryTag) } }, false);

        return done();
      }

      if (_to_ack.has(job.id)) {
        this.log(`Job ${job.name}:${job.id} will be acked from pending list.`);
        job.ack();
        _to_ack.delete(job.id);
        return done();
      }

      this.run(job).then(_ => done(), done);
    }

    // @ts-ignore
    if (this._channel_lost) {
      this.once("channelChanged", startProcessing);
    } else {
      startProcessing();
    }
  }
}
