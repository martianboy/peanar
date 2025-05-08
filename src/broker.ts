import debugFn from 'debug';
const debug = debugFn('peanar:broker');

import amqplib, { ChannelModel, ConsumeMessage, Replies, Channel } from 'amqplib';

import { ChannelPool } from './pool';
import { PeanarAdapterError } from './exceptions';
import {
  IBinding,
  IConnectionParams,
  IExchange,
  IMessage,
  IQueue
} from './types';
import Consumer from './consumer';

interface IBrokerOptions {
  connection?: string | IConnectionParams;
  socketOptions?: any;
  poolSize: number;
  prefetch?: number;
}

function timeout(ms: number) {
  return new Promise(res => {
    setTimeout(res, ms)
  })
}

/**
 * Peanar's broker adapter
 */
export default class NodeAmqpBroker {
  protected config: IBrokerOptions;
  protected conn?: ChannelModel;
  protected _connectPromise?: Promise<ChannelModel>;

  protected _channelConsumers = new Map<Channel, Set<Consumer>>();

  public pool?: ChannelPool;

  constructor(config: IBrokerOptions) {
    this.config = config
  }

  protected async _connectAmqp(retry = 1): Promise<ChannelModel> {
    debug(`_connectAmqp(${retry})`)
    try {
      const c = this.config || {};

      if (typeof c.connection === 'string') {
        this.conn = await amqplib.connect(c.connection, c.socketOptions);
      } else {
        this.conn = await amqplib.connect({
          protocol: c.connection?.protocol ?? 'amqp',
          hostname: c.connection?.host ?? 'localhost',
          port: c.connection?.port ?? 5672,
          username: c.connection?.username ?? 'guest',
          password: c.connection?.password ?? 'guest',
          vhost: c.connection?.vhost ?? '/'
        });
      }

      return this.conn
    } catch (ex: any) {
      if (ex.code === 'ECONNREFUSED') {
        await timeout(700 * retry);
        return this._connectAmqp(retry + 1);
      }

      throw ex;
    }
  }

  /**
   * Initializes adapter connection and channel
   */
  public connect = async () => {
    if (this._connectPromise) return this._connectPromise;

    const doConnect = async () => {
      debug('doConnect()');

      const conn = await this._connectAmqp();

      this.pool = new ChannelPool(conn, this.config.poolSize, this.config.prefetch);
      await this.pool.open();

      if (this._channelConsumers.size > 0) {
        await this.resurrectAllConsumers();
      }

      this.pool.on('channelLost', (ch) => {
        this.pauseConsumersOnChannel(ch);
      });
      this.pool.on('channelReplaced', (ch, newCh) => {
        this.rewireConsumersOnChannel(ch, newCh).catch(ex => {
          // FIXME: don't leave the broker in a broken state
          console.error(ex);
        });
      });
      this.pool.on('error', (ex) => {
        console.error('Pool error:', ex);
      });

      // FIXME: seems unused; remove?
      conn.on('error', ex => {
        debug(`AMQP connection error ${ex.code}!`);
        debug(`Original error message: ${ex.message}`);
      });

      conn.once('close', this.onClose);

      // TODO: consider not exposing connection
      return conn;
    }

    return (this._connectPromise = doConnect());
  }

  onClose = (err?: any) => {
    if (err) {
      debug(err.message);
    } else {
      debug('AMQP connection closed.');
    }

    this._connectPromise = undefined;
    if (err && err.code >= 300) {
      this.connect();
    } else {
      this.pool!.close();
      this.pool = undefined;
    }
  }

  /**
   * Awaits connection establishment
   * @throws {PeanarAdapterError} if not connected or in the process of connecting
   * @todo use a state machine to handle connection state (#68)
   * @todo return a Promise<void> instead of Promise<unknown>
   */
  ready(): Promise<unknown> {
    if (!this._connectPromise) {
      throw new PeanarAdapterError('Not connected!');
    }

    return this._connectPromise;
  }

  /**
   * Awaits pool closure and then closes the connection
   * @todo: use a state machine to handle connection state (#68)
   * @todo: support shutdown timeout (#69)
   */
  public async shutdown(): Promise<void> {
    // FIXME: replace this with a proper state machine (#68)
    if (!this.conn || !this.pool) {
      debug('shutdown() called when not connected');
      return;
    }

    debug('shutdown()');
    this.conn.off('close', this.onClose);

    await this.pool.close();
    this.pool = undefined;
    debug('pool closed.');

    await this.conn.close();
    this._connectPromise = undefined;
    this._channelConsumers.clear();
    this.conn = undefined;
    debug('connection closed.');
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

  private async resurrectAllConsumers() {
    // Expectation here is that the pool is already connected and have
    // the same number of channels as before
    await Promise.all(this.pool!.mapOver([...this._channelConsumers.keys()], (newCh, oldCh) => this.rewireConsumersOnChannel(oldCh, newCh)))
  }

  private pauseConsumersOnChannel(ch: Channel) {
    const set = this._channelConsumers.get(ch);
    if (!set || set.size < 1) return;

    for (const consumer of set) {
      consumer.pause();
    }
  }

  /**
   * Recreates consumers on a new channel in the same way as the old one
   * @todo properly handle errors, e.g. if the channel is closed when we try to consume (#67)
   */
  private async rewireConsumersOnChannel(ch: Channel, newCh: Channel) {
    const set = this._channelConsumers.get(ch);
    if (!set || set.size < 1) return;

    for (const consumer of set) {
      // FIXME (#67): handle potential errors
      const res = await newCh.consume(consumer.queue, (msg: ConsumeMessage | null) => {
        if (msg && consumer) {
          consumer.handleDelivery(msg);
        }
      });

      consumer.tag = res.consumerTag;
      consumer.channel = newCh;
      consumer.resume();
    }

    this._channelConsumers.delete(ch);
    this._channelConsumers.set(newCh, set);
  }

  private async _startConsumer(ch: Channel, queue: string): Promise<Consumer> {
    let consumer = new Consumer(ch, queue);

    return await ch.consume(queue, (msg: ConsumeMessage | null) => {
      if (msg) {
        consumer.handleDelivery(msg);
      }
    }).then((res: Replies.Consume) => {
      consumer.tag = res.consumerTag;

      if (!this._channelConsumers.has(ch)) {
        this._channelConsumers.set(ch, new Set([consumer]));
      } else {
        const set = this._channelConsumers.get(ch);
        set!.add(consumer);
      }

      return consumer;
    });
  }

  public consume(queue: string): PromiseLike<Consumer> {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.acquireAndRun(async ch => this._startConsumer(ch, queue));
  }

  public consumeOver(queues: string[]) {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.mapOver(queues, async (ch, queue) => {
      return {
        queue,
        channel: ch,
        consumer: await this._startConsumer(ch, queue)
      };
    });
  }

  /**
   * This method will always try to make a connection
   * unless `this.pool` is empty. Call with care.
   */
  public async publish(message: IMessage<unknown>) {
    await this.connect();

    const _doAcquire = async (): Promise<{
      release: () => void;
      channel: Channel;
    }> => {
      if (!this.pool) throw new PeanarAdapterError('Not connected!');

      try {
        return await this.pool.acquire();
      } catch (ex: any) {
        await this.connect();
        return _doAcquire();
      }
    }

    const _doPublish = async (): Promise<boolean> => {
      const { channel, release } = await _doAcquire()

      try {
        if (channel.publish(
          message.exchange ?? '',
          message.routing_key,
          Buffer.from(JSON.stringify(message.body)),
          {
            contentType: 'application/json',
            persistent: true,
            priority: message.properties?.priority
          }
        )) {
          release();
          return true;
        } else {
          debug('Channel is full, waiting for drain');
          channel.once('drain', release);
          return false;
        }
      } catch (ex: any) {
        if (ex.message === 'Channel closed') {
          debug('Acquired channel got closed, trying to acquire a new one');
          return _doPublish();
        }

        throw ex;
      }
    };

    return _doPublish();
  }
}
