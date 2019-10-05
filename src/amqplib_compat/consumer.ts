import { Readable } from 'stream';
import { Channel, ConsumeMessage } from 'amqplib';
import { IConsumer } from 'ts-amqp/dist/interfaces/Consumer';
import { IDelivery } from 'ts-amqp/dist/interfaces/Basic';

export default class Consumer extends Readable implements IConsumer<Channel> {
  public tag: string;
  public channel: Channel;

  public constructor(channel: Channel, consumer_tag: string) {
    super({
      objectMode: true
    });

    this.tag = consumer_tag;
    this.channel = channel;
  }

  public async cancel() {
    await this.channel.cancel(this.tag);
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
    this.push(msg);
  }

  public handleCancel(server: boolean) {
    this.emit('cancel', { server });
    this.destroy();
  }

  _read() {}
}
