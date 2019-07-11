import ChannelN from "ts-amqp/dist/classes/ChannelN";
import { PeanarAdapterError } from "./exceptions";
import { IPeanarJob } from "./app";

export default class PeanarJob {
  public id: string;
  public name: string;
  public args: any[];
  public queue: string;
  public handler: (...args: any[]) => Promise<any>;
  public exchange?: string;
  public deliveryTag: bigint;
  public replyTo?: string;
  public correlationId?: string;
  public channel: ChannelN;

  constructor(req: IPeanarJob, channel: ChannelN) {
    this.id = req.id
    this.name = req.name
    this.args = req.args
    this.correlationId = req.correlationId || req.id
    this.queue = req.queue
    this.replyTo = req.replyTo
    this.deliveryTag = req.deliveryTag

    this.handler = req.handler

    this.channel = channel;
  }

  ack() {
    if (!this.channel) throw new PeanarAdapterError('Worker: AMQP connection lost!')

    this.channel.basicAck(this.deliveryTag, false)
  }

  async perform() {
    return await this.handler.apply(null, this.args)
  }
}
