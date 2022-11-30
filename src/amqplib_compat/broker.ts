import { setTimeout as timeout } from 'timers/promises';

import debugFn from 'debug';
const debug = debugFn('peanar:broker');

import amqplib, { Connection, ConsumeMessage, Channel } from 'amqplib';

import { ChannelPool } from './pool';
import { PeanarAdapterError } from '../exceptions';
import { IMessage } from 'ts-amqp/dist/interfaces/Basic';
import { IQueue, IBinding } from 'ts-amqp/dist/interfaces/Queue';
import { IExchange } from 'ts-amqp/dist/interfaces/Exchange';
import Consumer from './consumer';
import { IConnectionParams } from 'ts-amqp/dist/interfaces/Connection';

export interface IBrokerOptions {
  connection?: IConnectionParams;
  poolSize: number;
  prefetch?: number;
}

export type TBrokerState = 'CLOSED' | 'CLOSING' | 'CONNECTING' | 'CONNECTED';

/**
 * Peanar's broker adapter
 */
export default class NodeAmqpBroker {
  protected config: IBrokerOptions;
  protected conn?: Connection;
  protected _connectPromise?: Promise<Connection>;

  protected _channelConsumers = new Map<Channel, Set<Consumer>>();
  protected _channelPrefetch = new WeakMap<Channel, number>();
  protected _state: TBrokerState = 'CLOSED';

  public pool?: ChannelPool;

  constructor(config: IBrokerOptions) {
    this.config = config
  }

  protected get state() {
    return this._state;
  }

  protected set state(newState) {
    debug(`state: ${newState}`);
    this._state = newState;
  }

  protected async _connectAmqp(maxRetries = 5, retry = 0): Promise<Connection> {
    debug(`_connectAmqp(${maxRetries}, ${retry})`);
    try {
      this.state = 'CONNECTING';

      const c = this.config || {};

      const conn = (this.conn = await amqplib.connect({
        hostname: c.connection?.host ?? 'localhost',
        port: c.connection?.port ?? 5672,
        username: c.connection?.username ?? 'guest',
        password: c.connection?.password ?? 'guest',
        vhost: c.connection?.vhost ?? '/'
      }));

      this.state = 'CONNECTED';
      return conn;
    } catch (ex) {
      this.state = 'CLOSED';
      if ((ex as any).code === 'ECONNREFUSED') {
        if (retry === maxRetries) {
          throw ex;
        }

        const delay = this.config.connection?.retryDelay ?? 700;
        await timeout(delay * retry);
        return this._connectAmqp(maxRetries, retry + 1);
      } else {
        console.error(ex);
        throw ex;
      }
    }
  }

  protected onPoolChannelLost = (ch: Channel) => {
    this.pauseConsumersOnChannel(ch);
  }

  protected onPoolChannelReplaced = (ch: Channel, newCh: Channel) => {
    this.rewireConsumersOnChannel(ch, newCh).catch(ex => {
      console.error(ex);
    });
  }

  protected onConnectionError = (ex: any) => {
    debug(`AMQP connection error ${ex.code}!`);
    debug(`Original error message: ${ex.message}`);
  }

  protected onConnectionClosed = (err?: any) => {
    if (err) {
      debug(err.message);
    } else {
      debug('AMQP connection closed.');
    }

    this.state = 'CLOSED';
    this._connectPromise = undefined;

    // If RabbitMQ has closed the connection for a protocol error, try to
    // restore the connection.
    if (err && err.code >= 300) {
      // this.state will become CONNECTING in the current tick.
      // It's important to make sure there will be no undefined state here.
      this.connect();
    }
  }

  /**
   * Initializes adapter connection and channel
   */
  public connect = () => {
    if (this._connectPromise) return this._connectPromise;

    const doConnect = async () => {
      debug('doConnect()');

      const conn = await this._connectAmqp(this.config.connection?.maxRetries);

      this.pool = new ChannelPool(
        conn,
        this.config.poolSize,
        this.config.prefetch
      );

      await this.pool.open();

      if (this._channelConsumers.size > 0) {
        await this.resurrectAllConsumers();
      }

      this.pool.on('channelLost', this.onPoolChannelLost);
      this.pool.on('channelReplaced', this.onPoolChannelReplaced);

      conn.on('error', this.onConnectionError);
      conn.once('close', this.onConnectionClosed);

      return conn;
    }

    return (this._connectPromise = doConnect());
  }

  public async shutdown() {
    debug('shutdown()');
    this.state = 'CLOSING';

    if (!this.conn) throw new PeanarAdapterError('Shutdown: Not connected!');
    if (!this.pool) throw new PeanarAdapterError('Shutdown: Strange! Channel pool has not been initialized!');

    await this.pool.close();
    this.pool = undefined;

    await this.conn.close();
    this.conn = undefined;
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
    await Promise.all(
      this.pool!.mapOver([...this._channelConsumers.keys()], (newCh, oldCh) =>
        this.rewireConsumersOnChannel(oldCh, newCh)
      )
    );
  }

  private pauseConsumersOnChannel(ch: Channel) {
    const set = this._channelConsumers.get(ch);
    if (!set || set.size < 1) return;

    for (const consumer of set) {
      consumer.pause();
    }
  }

  private async _ensurePrefetch(ch: Channel, prefetch: number) {
    debug(`broker._ensurePrefetch(ch, ${prefetch})`);

    const currentPrefetch = this._channelPrefetch.get(ch) ?? 1;
    if (currentPrefetch !== prefetch) {
      await ch.prefetch(prefetch);
      this._channelPrefetch.set(ch, prefetch);
    }
  }

  private async rewireConsumersOnChannel(ch: Channel, newCh: Channel) {
    const set = this._channelConsumers.get(ch);
    if (!set || set.size < 1) return;

    for (const consumer of set) {
      await this._ensurePrefetch(newCh, consumer.prefetch);

      const res = await newCh.consume(consumer.queue, (msg: ConsumeMessage | null) => {
        if (msg && consumer) {
          consumer.handleDelivery(msg);
        }
      });

      debug(`rewriteConsumersOnChannel on consumer ${consumer.tag}`);

      consumer.tag = res.consumerTag;
      consumer.channel = newCh;
      consumer.resume();
    }

    this._channelConsumers.delete(ch);
    this._channelConsumers.set(ch, set);
  }

  private async _startConsumer(ch: Channel, queue: string, prefetch = 1): Promise<Consumer> {
    const consumer: Consumer = new Consumer(ch, queue, prefetch);

    await this._ensurePrefetch(ch, prefetch);
    await consumer.start();

    consumer.once('cancel', ({ server }: { server: boolean }) => {
      if (server) return;

      const consumers = this._channelConsumers.get(ch);
      if (consumers) {
        consumers.delete(consumer);
      }
    });

    const consumers = this._channelConsumers.get(ch);
    if (consumers) {
      consumers.add(consumer);
    } else {
      this._channelConsumers.set(ch, new Set([consumer]));
    }

    return consumer;
  }

  public consume(queue: string, prefetch = 1): PromiseLike<Consumer> {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.acquireAndRun(async ch => this._startConsumer(ch, queue, prefetch));
  }

  public consumeOver(queues: string[], prefetch = 1) {
    if (!this.pool) throw new PeanarAdapterError('Not connected!');

    return this.pool.mapOver(queues, async (ch, queue) => {
      return {
        queue,
        consumer: await this._startConsumer(ch, queue, prefetch)
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
      } catch (ex) {
        await this.connect();
        return _doAcquire();
      }
    }

    const _doPublish = async (): Promise<boolean> => {
      const { channel, release } = await _doAcquire();

      try {
        if (channel.publish(
          message.exchange || '',
          message.routing_key,
          Buffer.from(JSON.stringify(message.body)),
          {
            contentType: 'application/json',
            mandatory: message.mandatory,
            persistent: true,
          }
        )) {
          release();
          return true;
        } else {
          channel.once('drain', release);
          return false;
        }
      } catch (ex) {
        if ((ex as Error).message === 'Channel closed') {
          return _doPublish();
        }

        throw ex;
      }
    };

    return _doPublish();
  }
}
