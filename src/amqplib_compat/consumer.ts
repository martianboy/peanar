import { Readable } from 'stream';
import { Channel, ConsumeMessage } from 'amqplib';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';

export default class Consumer extends Readable {
  public tag?: string;
  public queue: string;
  private _channel: Channel;

  public constructor(channel: Channel, queue: string) {
    super({
      objectMode: true
    });

    this._channel = channel;
    this.queue = queue;
  }

  public get channel() {
    return this._channel;
  }

  public set channel(ch) {
    this._channel = ch;
    this.emit('channelChanged', ch);
  }

  public async cancel() {
    if (!this.tag) {
      throw new Error('Consumer is not yet started or the consumer tag is not yet set!');
    }

    await this.channel.cancel(this.tag);
    this.removeAllListeners('channelChanged');
    this.handleCancel(false);
  }

  public handleDelivery(delivery: ConsumeMessage) {
    const msg: IDelivery = {
      body: delivery.content,
      envelope: {
        deliveryTag: BigInt(delivery.fields.deliveryTag),
        exchange: delivery.fields.exchange,
        routingKey: delivery.fields.routingKey,
        redeliver: delivery.fields.redelivered
      },
      properties: delivery.properties
    };

    if (this.isPaused()) {
      this.once('resume', () => {
        this.push(msg);
      });
    } else {
      this.push(msg);
    }
  }

  public handleCancel(server: boolean) {
    this.emit('cancel', { server });
    this.destroy();
  }

  _read() {}
}
