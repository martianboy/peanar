const { EventEmitter } = require('events');
const debugFn = require('debug');
const debug = debugFn('amqp:pool');

class ChannelPool extends EventEmitter {
  _queue = [];
  _pool = [];
  _acquisitions = new Map();
  _releaseResolvers = new Map();

  _isOpen = false;

  constructor(connection, size, prefetch = 1) {
    super();

    this._conn = connection;
    this._size = size;
    this.prefetch = prefetch;

    // this._conn.once('closing', this.softCleanUp);
    this._conn.once('close', this.hardCleanUp);
  }

  softCleanUp = () => {
    debug('ChannelPool: soft cleanup');
    this._isOpen = false;

    for (const resolver of this._releaseResolvers.values()) {
      resolver();
    }
  };

  hardCleanUp = () => {
    debug('ChannelPool: hard cleanup');
    this.softCleanUp();

    for (const { reject } of this._queue) {
      reject(new Error('ChannelPool: Connection failed.'));
    }
  };

  async *[Symbol.asyncIterator]() {
    if (!this._isOpen) {
      throw new Error('ChannelPool: [Symbol.asyncIterator]() called before pool is open.');
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

    debug('ChannelPool: initializing the pool');
    for (let i = 0; i < this._size; i++) {
      this._pool.push(await this.openChannel());
    }

    this.emit('open');
  }

  async close() {
    this.emit('closing');

    this._isOpen = false;

    // this._conn.off('closing', this.softCleanUp);
    this._conn.off('close', this.hardCleanUp);

    debug('ChannelPool: awaiting complete pool release');
    await Promise.all(this._acquisitions.values());

    debug('ChannelPool: closing all channels');
    for (const ch of this._pool) {
      ch.off('close');
      ch.off('error');
      await ch.close();
    }

    this.emit('close');
    debug('ChannelPool: pool closed successfully');
  }

  acquire() {
    if (!this._isOpen) {
      throw new Error('ChannelPool: acquire() called before pool is open.');
    }

    debug('ChannelPool: acquire()');

    const promise = new Promise((res, rej) =>
      this._queue.push({
        resolve: res,
        reject: rej
      })
    );

    if (this._pool.length > 0) {
      debug(
        `ChannelPool: ${this._pool.length} channels available in the pool. dispatch immediately`
      );
      this.dispatchChannels();
    } else {
      debug('ChannelPool: no channels available in the pool. awaiting...');
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
      this._pool.splice(idx, 1, await this.openChannel());
    } else {
      debug('ChannelPool: pool is closing. dropping closed channel from the pool');
      this._pool.splice(idx, 1);
    }
  }

  onChannelError(ch, err) {
    this.emit('error', ch, err);
  }

  async openChannel() {
    const ch = await this._conn.createChannel();
    ch.once('channelClose', this.onChannelClose.bind(this, ch));
    ch.on('error', this.onChannelError.bind(this, ch));

    if (this.prefetch) await ch.prefetch(this.prefetch, false);

    setImmediate(() => this.dispatchChannels());

    return ch;
  }

  releaser(ch) {
    // debug(`ChannelPool: channel ${ch.channelNumber} released`);
    this._pool.push(ch);

    const releaseResolver = this._releaseResolvers.get(ch);
    releaseResolver();

    debug('ChannelPool: dispatch released channel to new requests');
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

      // debug(`ChannelPool: channel ${ch.channelNumber} acquired`);
    }
  }
}

module.exports = ChannelPool;
