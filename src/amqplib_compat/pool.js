const { EventEmitter } = require('events');
const debugFn = require('debug');
const debug = debugFn('peanar:pool');

class ChannelPoolError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ChannelPoolError';
  }
}

class ChannelPool extends EventEmitter {
  _queue = [];

  /** @type {import('amqplib').Channel[]} */
  _pool = [];
  _acquisitions = new Map();
  _releaseResolvers = new Map();

  _isOpen = false;

  constructor(connection, size, prefetch = 1) {
    super();

    /** @type {import('amqplib').ChannelModel} */
    this._conn = connection;
    this._size = size;
    this.prefetch = prefetch;

    this._conn.once('close', this.hardCleanUp);
    this._conn.once('error', this.softCleanUp);
  }

  softCleanUp = () => {
    debug('soft cleanup');
    this._isOpen = false;

    for (const resolver of this._releaseResolvers.values()) {
      resolver();
    }
  };

  hardCleanUp = () => {
    debug('hard cleanup');
    this.softCleanUp();

    for (const { reject } of this._queue) {
      reject(new ChannelPoolError('Connection failed.'));
    }
  };

  async *[Symbol.asyncIterator]() {
    if (!this._isOpen) {
      throw new ChannelPoolError('[Symbol.asyncIterator]() called before pool is open.');
    }

    while (this._isOpen) {
      yield await this.acquire();
    }
  }

  get size() {
    return this._size;
  }

  get isOpen() {
    return this._isOpen;
  }

  async open() {
    this._isOpen = true;

    debug('initializing the pool');
    for (let i = 0; i < this._size; i++) {
      this._pool.push(await this.openChannel());
    }

    this.emit('open');
  }

  async close() {
    this.emit('closing');

    this._conn.off('close', this.hardCleanUp);

    debug('awaiting complete pool release');
    await Promise.all(this._acquisitions.values());

    debug('closing all channels');
    if (this._isOpen) {
      this._isOpen = false;
      for (const ch of this._pool) {
        ch.removeAllListeners('close');
        ch.removeAllListeners('error');
        await ch.close();
      }
    }

    this.emit('close');
    debug('pool closed successfully');
  }

  acquire() {
    if (!this._isOpen) {
      throw new ChannelPoolError('acquire() called before pool is open.');
    }

    const promise = new Promise((res, rej) =>
      this._queue.push({
        resolve: res,
        reject: rej
      })
    );

    if (this._pool.length > 0) {
      this.dispatchChannels();
    } else {
      debug('no channels available in the pool. awaiting...');
    }

    return promise;
  }

  mapOver(arr, fn) {
    return arr.map(item => this.acquireAndRun(ch => fn(ch, item)));
  }

  async acquireAndRun(fn) {
    const { channel, release } = await this.acquire();
    const result = await fn(channel);
    release();

    return result;
  }

  async onChannelClose(ch) {
    // debug(`ChannelPool: channel ${ch.channelNumber} closed`);

    this._acquisitions.delete(ch);
    this._releaseResolvers.delete(ch);

    const idx = this._pool.indexOf(ch);
    if (this._isOpen) {
      // debug(`ChannelPool: replacing closed channel ${ch.channelNumber} with a new one`);
      let newCh = undefined
      try {
        newCh = await this.openChannel();
        this.emit('channelReplaced', ch, newCh);
      } catch (ex) {
        this.softCleanUp();
      }

      this._pool.splice(idx, 1, newCh);
    } else {
      debug('pool is closing. dropping closed channel from the pool');
      this._pool.splice(idx, 1);
    }
  }

  onChannelError(ch, err) {
    console.error(err);
    this.emit('channelLost', ch, err);
  }

  async openChannel() {
    const ch = await this._conn.createChannel();
    ch.once('close', this.onChannelClose.bind(this, ch));
    ch.once('error', this.onChannelError.bind(this, ch));

    if (this.prefetch) await ch.prefetch(this.prefetch, false);

    setImmediate(() => this.dispatchChannels());

    return ch;
  }

  releaser(ch) {
    this._pool.push(ch);

    if (this._releaseResolvers.has(ch)) {
      const releaseResolver = this._releaseResolvers.get(ch);
      releaseResolver();
    }

    this.dispatchChannels();
  }

  dispatchChannels() {
    while (this._queue.length > 0 && this._pool.length > 0) {
      const dispatcher = this._queue.shift();
      const ch = this._pool.shift();
      const acquisition = new Promise(res => this._releaseResolvers.set(ch, res));
      this._acquisitions.set(ch, acquisition);

      dispatcher.resolve({
        channel: ch,
        release: this.releaser.bind(this, ch)
      });
    }
  }
}

module.exports = {
  ChannelPool,
  ChannelPoolError,
};
