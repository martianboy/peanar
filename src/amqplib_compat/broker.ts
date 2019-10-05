import debugFn from 'debug';
const debug = debugFn('peanar:broker');

import amqplib, { Connection, ConsumeMessage, Replies, Channel } from 'amqplib';

import ChannelPool from './pool';
import { PeanarAdapterError } from '../exceptions';
import { IMessage } from 'ts-amqp/dist/interfaces/Basic';
import { IQueue, IBinding } from 'ts-amqp/dist/interfaces/Queue';
import { IExchange } from 'ts-amqp/dist/interfaces/Exchange';
import Consumer from './consumer';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';

interface IBrokerOptions {
  connection?: IConnectionParams;
  poolSize: number;
  prefetch?: number;
}

/**
 * Peanar's broker adapter
 */
export default class NodeAmqpBroker {
  private config: IBrokerOptions;
  private conn?: Connection;
  private _connectPromise?: Promise<void>;

  public pool?: ChannelPool;

  constructor(config: IBrokerOptions) {
    this.config = config
  }

  private _connect = async () => {
    debug('_connect()');

    const c = this.config || {};

    const conn = (this.conn = await amqplib.connect({
      hostname: c.connection ? c.connection.host : 'localhost',
      port: c.connection ? c.connection.port : 5672,
      username: c.connection ? c.connection.username : 'guest',
      password: c.connection ? c.connection.password : 'guest',
      vhost: c.connection ? c.connection.vhost : '/'
    }));

    this.pool = new ChannelPool(conn, this.config.poolSize, this.config.prefetch);

    await this.pool.open();

    conn.once('close', (err?: any) => {
      this._connectPromise = undefined;
      if (err && err.reply_code >= 400) {
        this.connect()
      }
    });
  }

  /**
   * Initializes adapter connection and channel
   */
  public connect = async () => {
    if (this._connectPromise) return this._connectPromise;

    return (this._connectPromise = this._connect());
  }

  public async shutdown() {
    debug('shutdown()');

    if (!this.pool) throw new PeanarAdapterError('Shutdown: Strange! Channel pool has not been initialized!');
    if (!this.conn) throw new PeanarAdapterError('Shutdown: Not connected!');

    await this.pool.close();
    this.conn.off('close', this.connect);
    await this.conn.close();
  }

  public async queues(queues: IQueue[]) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return await Promise.all(this.pool.mapOver(queues, async (ch, queue) => {
      return ch.assertQueue(queue.name, {
        durable: queue.durable,
        autoDelete: queue.auto_delete,
        exclusive: queue.exclusive,
        ...queue.arguments
      });
    }));
  }

  public async exchanges(exchanges: IExchange[]) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return await Promise.all(this.pool.mapOver(exchanges, async (ch, exchange) => {
      return ch.assertExchange(exchange.name, exchange.type, {
        durable: exchange.durable,
        ...exchange.arguments
      });
    }));
  }

  public async bindings(bindings: IBinding[]) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return await Promise.all(this.pool.mapOver(bindings, async (ch, binding) => {
      return ch.bindQueue(binding.queue, binding.exchange, binding.routing_key);
    }));
  }

  private async _startConsumer(ch: Channel, queue: string): Promise<Consumer> {
    let consumer: Consumer | null;

    return await ch.consume(queue, (msg: ConsumeMessage | null) => {
      if (msg && consumer) {
        consumer.handleDelivery(msg);
      }
    }).then((res: Replies.Consume) => {
      consumer = new Consumer(ch, res.consumerTag);
      return consumer;
    }, ex => Promise.reject(ex));
  }

  public consume(queue: string): Promise<Consumer> {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.acquireAndRun(async ch => this._startConsumer(ch, queue));
  }

  public consumeOver(queues: string[]) {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.mapOver(queues, async (ch, queue) => {
      return {
        queue,
        consumer: await this._startConsumer(ch, queue)
      };
    });
  }

  public async publish(message: IMessage<unknown>) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    const { channel, release } = await this.pool.acquire();
    debug(`publish to channel`);
    if (channel.publish(
      message.exchange || '',
      message.routing_key,
      Buffer.from(JSON.stringify(message.body)),
      {
        contentType: 'application/json',
        mandatory: message.mandatory
      }
    )) {
      release();
      return true;
    } else {
      channel.once('drain', release);
      return false;
    }
  }
}
