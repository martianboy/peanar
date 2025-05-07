import { EventEmitter } from 'events';
import debugFn from 'debug';
import type { Channel } from 'amqplib';
import { PeanarPoolError } from './exceptions';
const debug = debugFn('peanar:pool');

type Releaser = () => void;
export interface ChannelWithReleaser {
  release: Releaser;
  channel: Channel;
}

export interface ChannelCreator {
  createChannel(): Promise<Channel>;

  on(event: 'error', listener: () => void): this;
  on(event: 'close', listener: () => void): this;
  once(event: 'error', listener: () => void): this;
  once(event: 'close', listener: () => void): this;
  off(event: 'error', listener: () => void): this;
  off(event: 'close', listener: () => void): this;
}

interface ChannelDispatcher {
  resolve: (value: ChannelWithReleaser) => void;
  reject: (reason?: unknown) => void;
}

export class ChannelPool extends EventEmitter {
  private _conn: ChannelCreator;
  private _capacity: number;
  private _queue: ChannelDispatcher[] = [];

  private _pool: Channel[] = [];
  private _acquisitions = new Map<Channel, Promise<void>>();
  private _releaseResolvers = new Map<Channel, Releaser>();

  private _isOpen = false;

  constructor(connection: ChannelCreator, capacity: number, private prefetch = 1) {
    super();

    this._conn = connection;
    this._capacity = capacity;
    this.prefetch = prefetch;

    this._conn.once('close', this.hardCleanUp);
    this._conn.once('error', this.softCleanUp);
  }

  private softCleanUp = () => {
    debug('soft cleanup');
    this._isOpen = false;

    for (const resolver of this._releaseResolvers.values()) {
      resolver();
    }
  };

  private hardCleanUp = () => {
    debug('hard cleanup');
    this.softCleanUp();

    for (const { reject } of this._queue) {
      reject(new PeanarPoolError('ChannelPool: Connection failed.'));
    }
  };

  async *[Symbol.asyncIterator]() {
    if (!this._isOpen) {
      throw new PeanarPoolError('[Symbol.asyncIterator]() called before pool is open.');
    }

    while (this._isOpen) {
      yield await this.acquire();
    }
  }

  get capacity(): number {
    return this._capacity;
  }

  get size(): number {
    return this._pool.length;
  }

  get queueLength(): number {
    return this._queue.length;
  }

  get isOpen(): boolean {
    return this._isOpen;
  }

  async open(): Promise<void> {
    if (this._isOpen) {
      throw new PeanarPoolError('open() called on an already open pool.');
    }

    debug('opening the pool');
    this._isOpen = true;

    debug('initializing the pool');
    for (let i = 0; i < this._capacity; i++) {
      this._pool.push(await this.openChannel());
    }

    this.emit('open');
  }

  async close(): Promise<void> {
    this.emit('closing');

    this._conn.off('close', this.hardCleanUp);

    debug('awaiting complete pool release');
    await Promise.all(this._acquisitions.values());

    debug('closing all channels');
    if (this._isOpen) {
      this._isOpen = false;
      let ch: Channel | undefined;
      while (ch = this._pool.shift()) {
        ch.removeAllListeners('close');
        ch.removeAllListeners('error');
        await ch.close();
      }
    }

    this.emit('close');
    debug('pool closed successfully');
  }

  acquire(): Promise<ChannelWithReleaser> {
    if (!this._isOpen) {
      throw new PeanarPoolError('acquire() called before pool is open.');
    }

    debug('acquire()');

    const promise: Promise<ChannelWithReleaser> = new Promise((res, rej) =>
      this._queue.push({
        resolve: res,
        reject: rej
      })
    );

    if (this._pool.length > 0) {
      debug(
        `${this._pool.length} channels available in the pool. dispatch immediately`
      );
      this.dispatchChannels();
    } else {
      debug('no channels available in the pool. awaiting...');
    }

    return promise;
  }

  mapOver<T, R>(arr: T[], fn: (ch: Channel, item: T) => Promise<R>): Promise<R>[] {
    return arr.map(item => this.acquireAndRun(ch => fn(ch, item)));
  }

  /**
   * Acquires a channel from the pool and runs the provided function on it.
   * The channel is released back to the pool after the function completes.
   * @param fn function to run on the channel
   * @returns the result of the function
   * @throws if the function throws
   * @throws {PeanarPoolError} if the pool is closed
   */
  async acquireAndRun<R>(fn: (ch: Channel) => Promise<R>): Promise<R> {
    const { channel, release } = await this.acquire();
    return fn(channel).finally(release);
  }

  private async onChannelClose(ch: Channel) {
    this._releaseResolvers.get(ch)?.();
    this._acquisitions.delete(ch);
    this._releaseResolvers.delete(ch);

    const idx = this._pool.indexOf(ch);
    if (this._isOpen) {
      let newCh: Channel | undefined = undefined;
      try {
        newCh = await this.openChannel();
        this.emit('channelReplaced', ch, newCh);
      } catch (ex) {
        debug('failed to open a new channel to replace a lost one. transitioning to closed state!', ex);
        this.emit('error', ex);
        return this.softCleanUp();
      }

      this._pool.splice(idx, 1, newCh);
    } else {
      debug('pool is closing. dropping closed channel from the pool');
      this._pool.splice(idx, 1);
    }
  }

  private onChannelError(ch: Channel, err: unknown) {
    console.error(err);
    this.emit('channelLost', ch, err)
  }

  private async openChannel(): Promise<Channel> {
    const ch = await this._conn.createChannel();
    ch.once('close', this.onChannelClose.bind(this, ch));
    ch.once('error', this.onChannelError.bind(this, ch));

    if (this.prefetch) await ch.prefetch(this.prefetch, false);

    queueMicrotask(() => this.dispatchChannels());

    return ch;
  }

  private releaser(ch: Channel, req: { released: boolean }) {
    if (req.released) {
      throw new PeanarPoolError('Release called for an acquisition request that has already been released.');
    }

    req.released = true;
    this._pool.push(ch);

    if (this._releaseResolvers.has(ch)) {
      const releaseResolver = this._releaseResolvers.get(ch)!;
      releaseResolver();
    }

    this.dispatchChannels();
  }

  private dispatchChannels() {
    while (this._queue.length > 0 && this._pool.length > 0) {
      const dispatcher = this._queue.shift()!;
      const ch = this._pool.shift()!;
      const acquisition = new Promise<void>(res => this._releaseResolvers.set(ch, res));
      this._acquisitions.set(ch, acquisition);

      dispatcher.resolve({
        channel: ch,
        release: this.releaser.bind(this, ch, { released: false }),
      });
    }
  }
}
