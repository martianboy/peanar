import { Readable } from 'stream';
import { Channel, ConsumeMessage } from 'amqplib';
import { IConsumer } from 'ts-amqp/dist/interfaces/Consumer';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';

export default class Consumer extends Readable implements IConsumer<Channel> {
  public tag: string;
  public queue: string;
  public prefetch: number = 1;
  private _channel: Channel;
  private _buffer: IDelivery[] = [];
  private _prefetchTimer?: NodeJS.Timeout;

  public constructor(channel: Channel, consumer_tag: string, queue: string, prefetch = 1, prefetchTimeout?: number) {
    super({
      objectMode: true
    });

    this.tag = consumer_tag;
    this._channel = channel;
    this.queue = queue;
    this.prefetch = prefetch;

    if (prefetchTimeout && prefetchTimeout > 0) {
      this._prefetchTimer = setInterval(() => this.pushBuffer(), prefetchTimeout)
    }
  }

  private pushBuffer() {
    if (this.isPaused()) {
      this.once('resume', () => {
        this.push(this._buffer);
        this._buffer = [];
      });
    } else {
      this.push(this._buffer);
      this._buffer = [];
    }
  }

  public get channel() {
    return this._channel;
  }

  public set channel(ch) {
    this._channel = ch;
    this.emit('channelChanged', ch);
  }

  public async cancel() {
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

    if (this._prefetchTimer) {
      this._buffer.push(msg);
    } else {
      if (this.isPaused()) {
        this.once('resume', () => {
          this.push([msg]);
        });
      } else {
        this.push([msg]);
      }
    }
  }

  public handleCancel(server: boolean) {
    this.emit('cancel', { server });
    if (this._prefetchTimer) {
      clearInterval(this._prefetchTimer);
    }
    this.destroy();
  }

  _read() {}
}
