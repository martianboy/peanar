import PeanarApp from './app';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';
import { Connection } from 'ts-amqp';
import ChannelN from 'ts-amqp/dist/classes/ChannelN';
import { PeanarAdapterError } from './exceptions';
import { EExchangeType } from 'ts-amqp/dist/interfaces/Exchange';
import { IQueueArgs } from 'ts-amqp/dist/interfaces/Queue';

/**
 * Peanar's broker adapter
 */
export default class PeanarBroker {
  private app: PeanarApp;
  private config?: IConnectionParams;
  private conn?: Connection;
  public channel?: ChannelN;

  private declared_exchanges: string[] = [
    '',
    'amq.direct',
    'amq.fanout'
  ];
  private declared_queues: string[] = [];

  constructor(app: PeanarApp, config?: IConnectionParams) {
    this.config = config
    this.app = app
  }

  /**
   * Initializes adapter connection and channel
   */
  connect = async () => {
    this.app.log('PeanarBroker: connect()');

    const conn = (this.conn = new Connection(this.config));
    await conn.start();

    conn.on('close', this.connect);

    this.channel = await conn.channel();
    this.channel.on('channelClose', this.reconnectChannel);

    return this.channel;
  }

  private reconnectChannel = async () => {
    if (this.conn && this.conn.state !== 'closing') {
      this.channel = await this.conn.channel();
    }
  }

  async prefetch(n: number) {
    if (!this.channel) throw new PeanarAdapterError('Prefetch: Strange! No open channels found!')

    this.channel.basicQos(n, false)
  }

  async closeConsumers() {
    if (!this.channel) throw new PeanarAdapterError('Shutdown: Strange! No open channels found!');
  }

  async shutdown() {
    this.app.log('PeanarAdapter: shutdown()')

    if (!this.channel) throw new PeanarAdapterError('Shutdown: Strange! No open channels found!')
    if (!this.conn) throw new PeanarAdapterError('Shutdown: Not connected!')

    this.channel.off('channelClose', this.reconnectChannel);
    this.conn.off('close', this.connect);
    await this.conn.close();
  }

  async declareExchange(exchange: string, type: EExchangeType = 'direct') {
    this.app.log(`PeanarBroker: declareExchange('${exchange}')`)

    if (!this.channel) throw new PeanarAdapterError('Not connected!')
    if (this.declared_exchanges.includes(exchange)) return

    await this.channel.declareExchange({
      name: exchange,
      type,
      durable: true,
      arguments: {}
    }, false)
  }

  async declareQueue(queue: string, args: IQueueArgs = {}, bindings: {exchange: string; routing_key: string}[] = []) {
    this.app.log(`PeanarBroker: declareQueue('${queue}')`)

    if (!this.channel) throw new PeanarAdapterError('Not connected!')
    if (this.declared_queues.includes(queue)) return

    await this.channel.declareQueue({
      name: queue,
      durable: true,
      exclusive: false,
      auto_delete: false,
      arguments: args
    })

    for (const b of bindings || []) {
      await this.channel.bindQueue({
        exchange: b.exchange,
        queue,
        routing_key: b.routing_key
      });
    }
  }
}
