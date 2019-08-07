import debugFn from 'debug';
const debug = debugFn('peanar:worker');

import util from 'util';
import 'colors';

import { Transform, TransformCallback } from 'stream'
import PeanarApp, { IPeanarRequest, IPeanarJob } from './app';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';
import ChannelN from 'ts-amqp/dist/classes/ChannelN';
import PeanarJob from './job';
import { PeanarInternalError } from './exceptions';

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

export default class PeanarWorker extends Transform {
  private app: PeanarApp;
  private channel: ChannelN;
  private queue: string;
  private n: number;
  private state: EWorkerState = EWorkerState.IDLE;
  private activeJob?: PeanarJob;

  private destroy_cb?: (err: Error | null) => void;
  private _destroy_timeout?: NodeJS.Timeout;
  private _shutdown_timeout: number = SHUTDOWN_TIMEOUT;

  constructor(app: PeanarApp, channel: ChannelN, queue: string) {
    super({
      objectMode: true
    });

    this.app = app;
    this.channel = channel;
    this.queue = queue;
    this.n = counter++;
  }

  async shutdown(timeout?: number) {
    if (timeout) this._shutdown_timeout = timeout;

    const destroy = util.promisify(this.destroy);
    await destroy.call(this, undefined);
  }

  _destroy(error: Error | null, callback: (error: Error | null) => void) {
    if (this.state === EWorkerState.IDLE) {
      this.state = EWorkerState.CLOSED;
      return callback(null);
    }
    else {
      this.state = EWorkerState.CLOSING;

      this.destroy_cb = (err) => {
        if (this.activeJob) this.activeJob.cancel();
        this.state = EWorkerState.CLOSED;
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

    return new this.app.jobClass(req, def, this.app, this.channel);
  }

  private async run(job: PeanarJob) {
    this.log('run()');
    this.activeJob = job;

    try {
      const result = await job.perform();

      this.push({
        status: 'SUCCESS',
        job,
        result
      });

      this.log('SUCCESS!');

      job.ack();
    } catch (ex) {
      this.push({
        status: 'FAILURE',
        job,
        error: ex
      });

      this.log('FAILURE!');

      await job.reject();
    } finally {
      this.activeJob = undefined;
    }
  }

  _transform(delivery: IDelivery, _encoding: string, cb: TransformCallback) {
    if (this.app.state !== "RUNNING") {
      this.log(`Received job ${Number(delivery.envelope.deliveryTag)} while shutting down. Holding on to it...`);
      return cb();
    }

    const done = (ex?: Error | null) => {
      this.log('Worker state: Idle');
      this.state = EWorkerState.IDLE;
      cb(ex);

      if (this.destroy_cb) {
        this.log('Destroying worker!');
        this.destroy_cb(null);
      }
      if (this._destroy_timeout) {
        clearTimeout(this._destroy_timeout);
        this._destroy_timeout = undefined;
      }
    }

    this.state = EWorkerState.WORKING;

    let job = undefined;
    try {
      job = this._getJob(delivery);
    } catch (ex) {
      return done(ex);
    }

    if (!job) {
      return done();
    }

    this.run(job).then(_ => done(), done);
  }
}
