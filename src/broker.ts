import debugFn from 'debug';
const debug = debugFn('peanar:broker');

import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';
import { Connection } from 'ts-amqp';
import ChannelPool from 'ts-amqp/dist/classes/ChannelPool';
import { PeanarAdapterError } from './exceptions';
import { IExchange } from 'ts-amqp/dist/interfaces/Exchange';
import { IQueue, IBinding } from 'ts-amqp/dist/interfaces/Queue';
import { ICloseReason } from 'ts-amqp/dist/interfaces/Protocol';
import { IMessage } from 'ts-amqp/dist/interfaces/Basic';

interface IBrokerOptions {
  connection?: IConnectionParams;
  poolSize: number;
  prefetch?: number;
}

/**
 * Peanar's broker adapter
 */
export default class PeanarBroker {
  private config: IBrokerOptions;
  private conn?: Connection;
  private _connectPromise?: Promise<void>;

  public pool?: ChannelPool;

  constructor(config: IBrokerOptions) {
    this.config = config
  }

  private _connect = async () => {
    debug('_connect()');

    const conn = (this.conn = new Connection(this.config.connection));
    this.pool = new ChannelPool(conn, this.config.poolSize, this.config.prefetch);

    await conn.start();
    await this.pool.open();

    conn.once('close', (err?: ICloseReason) => {
      this._connectPromise = undefined;
      if (err && err.reply_code < 400) {
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
      return ch.declareQueue(queue);
    }));
  }

  public async exchanges(exchanges: IExchange[]) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return await Promise.all(this.pool.mapOver(exchanges, async (ch, exchange) => {
      return ch.declareExchange(exchange);
    }));
  }

  public async bindings(bindings: IBinding[]) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return await Promise.all(this.pool.mapOver(bindings, async (ch, binding) => {
      return ch.bindQueue(binding);
    }));
  }

  public consume(queue: string) {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.acquireAndRun(ch => ch.basicConsume(queue));
  }

  public consumeOver(queues: string[]) {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.mapOver(queues, async (ch, queue) => {
      return {
        queue,
        consumer: await ch.basicConsume(queue)
      };
    });
  }

  public async publish(message: IMessage<unknown>) {
    await this.connect();
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    const { channel, release } = await this.pool.acquire();
    debug(`publish to channel ${channel.channelNumber}`);
    channel.json.write(message);
    release();
  }
}
