import { Transform, TransformCallback } from 'stream'
import PeanarApp, { IPeanarRequest, IPeanarJob } from './app';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';
import ChannelN from 'ts-amqp/dist/classes/ChannelN';
import PeanarJob from './job';

export type IWorkerResult = {
  status: 'SUCCESS';
  job: PeanarJob;
  result: unknown;
} | {
  status: 'FAILURE';
  job: PeanarJob;
  error: unknown;
}

let counter = 0;

export default class PeanarWorker extends Transform {
  private app: PeanarApp;
  private channel: ChannelN;
  private queue: string;
  private n: number;

  constructor(app: PeanarApp, channel: ChannelN, queue: string) {
    super({
      objectMode: true
    })

    this.app = app
    this.channel = channel
    this.queue = queue
    this.n = counter++;
  }

  getJobDefinition(name: string) {
    const queue_mapping = this.app.registry.get(this.queue)
    
    if (!queue_mapping) return

    return queue_mapping.get(name)
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

    this.app.log(`PeanarWorker: _getJob(${JSON.stringify(delivery.body.toString('utf-8'), null, 2)})`)

    const body = this._parseBody(delivery.body)

    if (!body || !body.name || body.name.length < 1) {
      console.warn('PeanarWorker#_getJob: Invalid message body!')
      return
    }

    const def = this.getJobDefinition(body.name)

    if (!def) {
      console.warn(`PeanarWorker#_getJob: No handler registered for ${this.queue}.${body.name}!`)
      return
    }

    const req: IPeanarJob = {
      ...def,
      deliveryTag: delivery.envelope.deliveryTag,
      replyTo: delivery.properties.replyTo,
      correlationId: delivery.properties.correlationId,
      queue: this.queue,
      args: body.args,
      id: body.id
    }

    if (delivery.properties.replyTo) {
      req.replyTo = delivery.properties.replyTo;
      req.correlationId = delivery.properties.correlationId;
    }

    return new this.app.jobClass(req, this.channel);
  }

  private async run(job: PeanarJob) {
    this.app.log(`PeanarWorker#${this.n}: run()`);

    try {
      const result = await job.perform()

      this.push({
        status: 'SUCCESS',
        job,
        result
      })
    } catch (ex) {
      this.push({
        status: 'FAILURE',
        job,
        err: ex
      })
    } finally {
      job.ack()
    }
  }

  _transform(delivery: IDelivery, _encoding: string, cb: TransformCallback) {
    const job = this._getJob(delivery)

    if (!job) return cb();

    this.run(job).then(_ => cb(), _ => cb());
  }
}
