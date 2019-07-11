import { Transform, TransformCallback } from 'stream'
import PeanarApp, { IPeanarRequest, IPeanarJob } from './app';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';
import ChannelN from 'ts-amqp/dist/classes/ChannelN';

export default class PeanarConsumer extends Transform {
  private app: PeanarApp;
  private channel: ChannelN;
  private queue: string;

  constructor(app: PeanarApp, channel: ChannelN, queue: string) {
    super({
      objectMode: true
    })

    this.app = app
    this.channel = channel
    this.queue = queue
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

  _transform(delivery: IDelivery, _encoding: string, cb: TransformCallback) {
    if (!delivery.body) {
      console.warn('PeanarConsumer#_transform: Delivery without body!')
      return cb()
    }

    this.app.log(`PeanarConsumer: _transform(${JSON.stringify(delivery.body.toString('utf-8'), null, 2)})`)

    const body = this._parseBody(delivery.body)

    if (!body || !body.name || body.name.length < 1) {
      console.warn('PeanarConsumer#_transform: Invalid message body!')
      return cb()
    }

    const job = this.getJobDefinition(body.name)

    if (!job) {
      console.warn(`PeanarConsumer#_transform: No handler registered for ${this.queue}.${body.name}!`)
      return cb()
    }

    const req: IPeanarJob = {
      ...job,
      deliveryTag: delivery.envelope.deliveryTag,
      replyTo: delivery.properties.replyTo,
      correlationId: delivery.properties.correlationId,
      queue: this.queue,
      args: body.args,
      id: body.id
    }

    if (delivery.properties.replyTo) {
      req.replyTo = delivery.properties.replyTo
      req.correlationId = delivery.properties.correlationId
    }

    this.push(new this.app.jobClass(req, this.channel))

    cb()
  }
}
