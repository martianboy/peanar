import { EventEmitter, once } from 'events';
import * as uuid from 'uuid';
import { Channel } from 'amqplib';
import debugFn from 'debug';
const debug = debugFn('peanar:transact');

import { IPeanarJobDefinition, EAppState } from "./app";
import PeanarApp from './app';
import { PeanarInternalError } from './exceptions';
import { IBasicProperties } from 'ts-amqp/dist/interfaces/Protocol';


function timeout(ms: number) {
  return new Promise((res, rej) => {
    setTimeout(rej, ms);
  });
}

class PeanarTransactor extends EventEmitter {
  protected queue_name: string;
  protected tid: string;
  protected _counter = 0;

  protected _acquireChannelPromise?: Promise<Channel>;
  protected committed = false;
  protected rolledback = false;
  protected began = false;

  public constructor(
    protected def: IPeanarJobDefinition,
    protected app: PeanarApp
  ) {
    super();

    this.queue_name = `${this.def.queue}:${this.def.name}:${uuid.v4()}`;
    this.tid = this.queue_name;
  }

  get queueName() { return this.queue_name; }

  protected acquireChannel() {
    if (this.app.state !== EAppState.RUNNING) {
      throw new PeanarInternalError('Peanar is not running!');
    }

    const _doAcquire = async () => {
      const conn = await this.app.broker.connect();
      const ch = await conn.createChannel();
      await ch.prefetch(0, false);

      ch.once('close', () => {
        this._acquireChannelPromise = undefined;

        if (this.app.state === EAppState.RUNNING) {
          this._acquireChannelPromise = _doAcquire();
        }
      });

      return ch;
    }

    if (this._acquireChannelPromise) return this._acquireChannelPromise;

    return (this._acquireChannelPromise = _doAcquire());
  }

  public async begin(tid?: string) {
    if (tid) {
      this.tid = tid;
    }

    await this.app.broker.queues([{
      name: this.queue_name,
      durable: true,
      exclusive: false,
      auto_delete: false,
      arguments: {
        deadLetterExchange: this.def.exchange,
        deadLetterRoutingKey: this.def.routingKey,
      }
    }]);
  }

  public async call(...args: unknown[]) {
    debug(`PeanarTransactor: call(${this.def.queue}:${this.def.name}})`);

    const req = {
      id: uuid.v4(),
      name: this.def.name,
      args,
      attempt: 1
    };

    if (this.app.state !== EAppState.RUNNING) {
      throw new PeanarInternalError('PeanarTransactor::call() called while app is not in running state.');
    }

    const properties: IBasicProperties = {};

    if (typeof this.def.expires === 'number') {
      properties.expiration = this.def.expires.toString();
    }

    await this.app.broker.publish({
      routing_key: this.queue_name,
      exchange: '',
      properties,
      body: {
        id: req.id,
        name: req.name,
        args: req.args
      }
    });

    this._counter += 1;

    return req.id;
  }

  public async waitUntil(ms = 0): Promise<void> {
    if (this.committed || this.rolledback) return;

    const arr: Promise<unknown>[] = [
      once(this, 'conclude'),
    ];

    if (ms > 0) {
      arr.push(timeout(ms));
    }

    await Promise.race(arr);
  }

  public async commit(): Promise<void> {
    if (this.committed) {
      throw new PeanarInternalError('Transaction already commited.');
    }
    if (this.rolledback) {
      throw new PeanarInternalError('Transaction has been rolled back.');
    }

    let resolveFn: () => void;
    let consumeCounter = 0;

    const consumerPromise = new Promise(res => {
      resolveFn = res;
    });

    const consumerCallback = () => {
      consumeCounter += 1;

      if (consumeCounter === this._counter) {
        if (typeof resolveFn === 'function') {
          resolveFn();
        } else {
          setImmediate(resolveFn);
        }
      }
    }

    const ch = await this.acquireChannel();

    try {
      const consumer = await ch.consume(this.queue_name, consumerCallback);

      await consumerPromise;

      await ch.cancel(consumer.consumerTag);
      ch.nackAll(false);
      await ch.deleteQueue(this.queue_name, { ifEmpty: true });

      debug(`${this.queue_name}: committed`);
      this.committed = true;
      this.emit('commit');
      this.emit('conclude', 'commit');
    } catch(ex) {
      if (ex.message === 'Channel closed' || ex.name === 'IllegalOperationError') {
        this.app.log(`Channel closed while committing the transaction ${this.queue_name}. Retrying...`);
        return this.commit();
      }
      throw ex;
    }
  }

  public async rollback(): Promise<void> {
    if (this.committed) {
      throw new PeanarInternalError('Transaction already commited.');
    }
    if (this.rolledback) {
      throw new PeanarInternalError('Transaction has been rolled back.');
    }
    if (this.app.state !== EAppState.RUNNING || !this.app.broker.pool) {
      throw new PeanarInternalError('Peanar is not running!');
    }

    try {
      await this.app.broker.pool.acquireAndRun(ch => ch.deleteQueue(this.queue_name, { ifEmpty: true }));
      debug(`${this.queue_name}: rolled back`);
      this.rolledback = true;
      this.emit('rollback');
      this.emit('conclude', 'rollback');
    } catch (ex) {
      if (ex.message === 'Channel closed' || ex.name === 'IllegalOperationError') {
        this.app.log(`Channel closed while rolling back the transaction ${this.queue_name}. Retrying...`);
        return this.rollback();
      }
      throw ex;
    }
  }
}

export default PeanarTransactor;
