import { Readable } from 'stream';
import { Channel, ConsumeMessage } from 'amqplib';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';
import { PeanarAdapterError } from '../exceptions';

export default class Consumer extends Readable {
  public tag: string | null = null;
  public queue: string;
  public prefetch: number;
  private _channel: Channel;

  public constructor(channel: Channel, queue: string, prefetch = 1) {
    super({
      objectMode: true
    });

    this._channel = channel;
    this.queue = queue;
    this.prefetch = prefetch;
  }

  async start() {
    const res = await this._channel.consume(
      this.queue,
      (msg: ConsumeMessage | null) => {
        if (msg) {
          this.handleDelivery(msg);
        }
      }
    );

    this.tag = res.consumerTag;
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
      throw new PeanarAdapterError('Cannot cancel a consumer without tag.');
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

export async function createConsumer(
  channel: Channel,
  queue: string,
  prefetch = 1
): Promise<Consumer> {
  const consumer = new Consumer(channel, queue, prefetch);
  await consumer.start();

  return consumer;
}
