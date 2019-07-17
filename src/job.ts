import ChannelN from "ts-amqp/dist/classes/ChannelN";
import { PeanarAdapterError, PeanarJobError } from "./exceptions";
import PeanarApp, { IPeanarRequest, IPeanarJobDefinition, IPeanarJob } from "./app";

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
  }

  reject(requeue: boolean = false) {
    if (!this.channel)
      throw new PeanarAdapterError("Worker: AMQP connection lost!");

    if (!this.deliveryTag)
      throw new PeanarJobError("Worker: No deliveryTag set!");

    this.channel.basicReject(this.deliveryTag, requeue);
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
