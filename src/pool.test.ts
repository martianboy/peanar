import { rejects } from 'assert';
import { expect } from 'chai';
import sinon, { SinonStub } from 'sinon';
import { EventEmitter } from 'events';
import { ChannelPool, ChannelCreator } from './pool';
import { controllablePromise, Try } from '../test/utils';

/**
 * A minimal fake Channel implementation.
 * Only the members used by ChannelPool are provided.
 */
class FakeChannel extends EventEmitter {
  public prefetch: SinonStub;
  public close: SinonStub;

  constructor (public id: number) {
    super();
    this.prefetch = sinon.stub().resolves();
    this.close = sinon.stub().callsFake(async () => {
      this.emit('close');
    });
  }
}

/** Helper that builds a fake ChannelCreator with a fresh FakeChannel per call */
function createFakeConn (): {
  conn: ChannelCreator & EventEmitter;
  createChannelSpy: SinonStub;
} {
  const conn = new EventEmitter() as ChannelCreator & EventEmitter;
  let channelCount = 0;
  const createChannelSpy = sinon.stub().callsFake(async () => new FakeChannel(channelCount++));
  // @ts-ignore – we are adding the method dynamically
  conn.createChannel = createChannelSpy;
  return { conn, createChannelSpy };
}

describe('ChannelPool', () => {
  describe('life-cycle (open / close)', () => {
    it('starts closed; open() initializes the pool and emits "open"', async () => {
      const { conn, createChannelSpy } = createFakeConn();
      const pool = new ChannelPool(conn, 3, 5);

      const openSpy = sinon.spy();
      pool.on('open', openSpy);

      // pool is closed by default
      expect(pool.isOpen).to.be.false;
      expect(pool.size).to.equal(0);

      await pool.open();

      expect(pool.isOpen).to.be.true;
      expect(pool.size).to.equal(3);
      expect(createChannelSpy.callCount).to.equal(3);
      // each channel had prefetch called with 5
      for (const call of createChannelSpy.getCalls()) {
        const ch = await call.returnValue;
        expect(ch.prefetch.calledWithExactly(5, false)).to.be.true;
      }
      expect(openSpy.calledOnce).to.be.true;
    });

    it('close() waits for in-flight releases and then closes every channel', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const acquired = await pool.acquire(); // take the only channel
      const closingSpy = sinon.spy();
      const closeSpy   = sinon.spy();
      pool.on('closing', closingSpy);
      pool.on('close',   closeSpy);

      const closePromise = pool.close(); // should block until released
      await new Promise(r => setTimeout(r, 10)); // give event-loop a tick
      // Promise should still be pending because channel not released yet
      expect(pool.isOpen).to.be.true;

      acquired.release();             // now release the channel
      await closePromise;             // close should finish

      expect(pool.isOpen).to.be.false;
      expect(closingSpy.calledOnce).to.be.true;
      expect(closeSpy.calledOnce).to.be.true;
    });
  });

  describe('acquire() / release()', () => {
    it('throws if called before open()', () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      expect(() => pool.acquire()).to.throw('acquire() called before pool is open.');
    });

    it('returns a channel immediately when one is available', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 2);
      await pool.open();

      const { channel, release } = await pool.acquire();
      expect(channel).to.be.instanceOf(EventEmitter);
      expect(pool.size).to.equal(1);  // one removed from pool

      release();                      // put it back
      expect(pool.size).to.equal(2);
    });

    it('queues requests when pool is empty and resolves them in FIFO order', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const first = await pool.acquire();          // pool empty afterwards
      const p2 = pool.acquire();                   // queued
      const p3 = pool.acquire();                   // queued

      first.release();                             // triggers dispatch

      const second = await p2;
      expect(second).to.not.equal(first);          // may be same object, but released
      second.release();

      const third = await p3;
      third.release();

      expect(pool.size).to.equal(1);
    });
  });

  describe('channel event handling', () => {
    it('replaces a channel that closes unexpectedly while pool is open', async () => {
      const { conn, createChannelSpy } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const oldCh = (await createChannelSpy.getCall(0).returnValue) as FakeChannel;
      createChannelSpy.resetHistory(); // reset call count

      const replaceSpy = sinon.spy();
      pool.on('channelReplaced', replaceSpy);

      oldCh.emit('close');            // simulate broker closing channel
      await new Promise(r => setImmediate(r));   // give dispatch loop a tick

      expect(createChannelSpy.calledOnce).to.be.true;
      expect(replaceSpy.calledOnce).to.be.true;
      const [, newCh] = replaceSpy.firstCall.args;
      expect(newCh).to.be.instanceOf(EventEmitter);
      expect(newCh).to.not.equal(oldCh); // new channel
      expect(pool.size).to.equal(1);
    });

    it('emits "channelLost" on channel error', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const lostSpy = sinon.spy();
      pool.on('channelLost', lostSpy);

      // grab the existing channel from the pool directly
      const ch = await (pool as any)._pool[0];
      const err = new Error('boom');
      ch.emit('error', err);

      expect(lostSpy.calledOnceWithExactly(ch, err)).to.be.true;
    });

    it('replaces dead channels on error', async () => {
      const { conn, createChannelSpy } = createFakeConn();
      const pool = new ChannelPool(conn, 2);
      await pool.open();
      const oldChannels: FakeChannel[] = await Promise.all(createChannelSpy.getCalls().map(c => c.returnValue));
      createChannelSpy.resetHistory(); // reset call count

      // create two promises (both resolving a new FakeChannel instance) that will resolve in reverse order
      const p1 = controllablePromise<FakeChannel>();
      const p2 = controllablePromise<FakeChannel>();
      createChannelSpy.onFirstCall().callsFake(() => p1.promise);
      createChannelSpy.onSecondCall().callsFake(() => p2.promise);

      // close old channels
      oldChannels.forEach(ch => ch.close());

      expect(createChannelSpy.calledTwice).to.be.true;

      // resolve the promises in reverse order
      const newChannels = [new FakeChannel(1001), new FakeChannel(1002)];

      p2.resolve(newChannels[1]);
      await new Promise(r => setImmediate(r));
      expect((pool as any)._pool).to.have.members([oldChannels[0], newChannels[1]]);

      p1.resolve(newChannels[0]);
      await new Promise(r => setImmediate(r));
      expect((pool as any)._pool).to.have.members(newChannels);
    });

    it('release() does not throw if called on a closed channel', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const { channel, release } = await pool.acquire();
      expect(pool.size).to.equal(0);

      // simulate channel close
      channel.close();
      const [ _, err ] = await Try.catch(async () => release());
      expect(err).to.be.null;
    });

    it('does not replace lost channels if pool is closed', async () => {
      const { conn, createChannelSpy } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const oldCh = (await createChannelSpy.getCall(0).returnValue) as FakeChannel;
      createChannelSpy.resetHistory(); // reset call count

      const lostSpy = sinon.spy();
      pool.on('channelLost', lostSpy);

      const closePromise = pool.close();                     // close the pool
      oldCh.emit('error', new Error('boom')); // simulate channel error

      await new Promise(r => setImmediate(r));   // give dispatch loop a tick

      expect(createChannelSpy.called).to.be.false;
      expect(lostSpy.calledOnce).to.be.true;

      // wait for close to finish
      await closePromise;
    });
  });

  describe('clean-up helpers', () => {
    it('softCleanUp() resolves all release promises and marks pool closed', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 1);
      await pool.open();

      const { release } = await pool.acquire();
      // soft cleanup (simulates connection error)
      conn.emit('error', new Error('boom'));
      expect(pool.isOpen).to.be.false;

      // releasing now should still succeed without throwing
      expect(release).to.not.throw;
    });

    it('hardCleanUp() rejects queued acquisitions', async () => {
      const { conn } = createFakeConn();
      const pool = new ChannelPool(conn, 0); // start with empty pool
      await pool.open();

      const p = pool.acquire();   // queued because no channel yet
      pool.hardCleanUp();

      await rejects(p, 'Connection failed.');
      expect(pool.isOpen).to.be.false;
    });
  });
});
