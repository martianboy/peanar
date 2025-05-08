import { expect } from 'chai';
import amqplib from 'amqplib';

import { ChannelPool, ChannelWithReleaser } from '../src/pool';

import { brokerOptions } from './config';
import { randomName, retry, Try } from './utils';
import { getVhostConnections, getChannelsOnConnection, createVhost, deleteVhost } from './rabbitmq-http/client';
import { PeanarPoolError } from '../src/exceptions';
import { once } from 'events';

describe('Pool', () => {
  const vhost = randomName('test');
  let conn: amqplib.ChannelModel;
  let pool: ChannelPool;
  let rabbitmqConnectionInfo: any = undefined;

  before(async () => {
    await createVhost(vhost);
    conn = await amqplib.connect({
      ...brokerOptions.connection,
      vhost,
    });

    await retry(15, 500, async () => {
      const conns = await getVhostConnections(vhost);
      expect(conns.length).to.equal(1);
      rabbitmqConnectionInfo = conns[0];
    });
  });
  after(async () => {
    await conn.close();
    await deleteVhost(vhost);
  });

  describe('Pool-Lifecycle & Initialization', () => {
    describe('constructor', () => {
      before(() => {
        pool = new ChannelPool(conn, 5, 1);
      });
      it('should be initially closed', () => { expect(pool.isOpen).to.be.false; });
      it('should have a size of 0', () => { expect(pool.size).to.equal(0); });
    });
    describe('open()', () => {
      context('on a freshly created pool', () => {
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
        });
        after(async () => {
          await pool.close();
        });

        it('should be open', () => { expect(pool.isOpen).to.be.true; });
        it('should have a size of 5', () => { expect(pool.size).to.equal(5); });
        it('should have 5 channels', async () => {
          await retry(15, 500, async () => {
            const channels = await getChannelsOnConnection(rabbitmqConnectionInfo.name);
            expect(channels.length).to.equal(5);
          });
        });
      });
      context('on a closed pool', () => {
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
          await pool.close();
          await pool.open();
        });
        after(async () => {
          await pool.close();
        });

        it('should be open', () => { expect(pool.isOpen).to.be.true; });
        it('should have a size of 5', () => { expect(pool.size).to.equal(5); });
        it('should have 5 channels', async () => {
          await retry(15, 500, async () => {
            const channels = await getChannelsOnConnection(rabbitmqConnectionInfo.name);
            expect(channels.length).to.equal(5);
          });
        });
      });
      context('on an already open pool', () => {
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
        });
        after(async () => {
          await pool.close();
        });

        it('should throw an error', async () => {
          try {
            await pool.open();
          } catch (err) {
            expect(err).to.be.instanceOf(PeanarPoolError);
            expect(err.message).to.equal('open() called on an already open pool.');
          }
        });
      });
    });
    describe('close()', () => {
      context('on a closed pool', () => {
        let emitClosePromise: Promise<any[]>;
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
          emitClosePromise = once(pool, 'close');
          await pool.close();
        });

        it('should be closed', () => { expect(pool.isOpen).to.be.false; });
        it('should have a size of 0', () => { expect(pool.size).to.equal(0); });
        it('should have 0 channels', async () => {
          await retry(15, 500, async () => {
            const channels = await getChannelsOnConnection(rabbitmqConnectionInfo.name);
            expect(channels.length).to.equal(0);
          });
        });
        it('should emit close event', async () => {
          await emitClosePromise;
        });
        it('should consider a second close a no-op', async () => {
          await pool.close();
          expect(pool.isOpen).to.be.false;
          expect(pool.size).to.equal(0);
        });
      });
      context('on an open pool', () => {
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
        });
        after(async () => {
          await pool.close();
        });

        it('should be closed', async () => {
          const emitClosePromise = once(pool, 'close');
          await pool.close();
          await emitClosePromise;
          expect(pool.isOpen).to.be.false;
          expect(pool.size).to.equal(0);
        });
      });
      context('on a pool with acquired channels', () => {
        let releaser: () => void;
        let ch: amqplib.Channel;
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
          const chWithReleaser = await pool.acquire();
          expect(pool.size).to.equal(4);
          expect(chWithReleaser).to.be.ok;
          ch = chWithReleaser.channel;
          releaser = chWithReleaser.release;
        });

        it('should await for all channels to be released', async () => {
          const closePromise = pool.close();
          expect(pool.size).to.equal(4);
          expect(pool.isOpen).to.be.true;
          releaser();
          await closePromise;
          expect(pool.size).to.equal(0);
        });
      });
    });
  });

  describe('Channel Acquisition & Release Semantics', () => {
    describe('acquire()', () => {
      context('on a closed pool', () => {
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
        });
        it('should throw an error in initial state', async () => {
          const [_, err] = await Try.catch(() => pool.acquire());

          expect(err).to.be.instanceOf(PeanarPoolError);
          expect(err?.message).to.equal('acquire() called before pool is open.');
        });
        it('should throw an error after an open and close', async () => {
          await pool.open();
          await pool.close();

          expect(() => pool.acquire()).to.throw('acquire() called before pool is open.');
        });
      });
      context('on an open pool', () => {
        before(async () => {
          pool = new ChannelPool(conn, 5, 1);
          await pool.open();
        });
        after(async () => {
          await pool.close();
        });

        it('should return a channel with a releaser', async () => {
          const { release, channel: ch } = await pool.acquire();
          try {
            expect(ch).to.be.ok;
            expect(release).to.be.ok;
            expect(pool.size).to.equal(4);
          } finally {
            release?.();
          }
        });
        it('should wait for the channel to be released when full', async () => {
          const acquirePromises: Promise<ChannelWithReleaser>[] = [];
          for (let i = 0; i < 5; i++) {
            acquirePromises.push(pool.acquire());
          }
          expect(pool.size).to.equal(0);
          expect(pool.queueLength).to.equal(0);

          const acquirePromise = pool.acquire();
          expect(pool.queueLength).to.equal(1);

          const acquisitions = await Promise.all(acquirePromises);
          acquisitions[0].release();
          expect(pool.queueLength).to.equal(0);
          expect(pool.size).to.equal(0);
          acquisitions.shift();

          acquisitions.unshift(await acquirePromise);
          expect(pool.size).to.equal(0);

          for (const { release } of acquisitions) {
            release();
          }
          expect(pool.size).to.equal(5);
        });
        it('should provide an idempotent releaser', async () => {
          const acquirePromises: Promise<ChannelWithReleaser>[] = [];
          for (let i = 0; i < 7; i++) {
            acquirePromises.push(pool.acquire());
          }
          expect(pool.size).to.equal(0);
          expect(pool.queueLength).to.equal(2);
          const { release } = await acquirePromises.shift()!;

          release();
          expect(pool.size).to.equal(0);
          expect(pool.queueLength).to.equal(1);
          expect(() => release()).to.throw('Release called for an acquisition request that has already been released.');
          expect(pool.size).to.equal(0);
          expect(pool.queueLength).to.equal(1);

          for await (const { release } of acquirePromises) {
            release();
          }
        });
      });
    });
    describe('acquireAndRun()', () => {
      before(async () => {
        pool = new ChannelPool(conn, 5, 1);
        await pool.open();
      });
      after(async () => {
        await pool.close();
      });

      it('should acquire a channel and run the function, then release the channel if function succeeds', async () => {
        const result = await pool.acquireAndRun(async (ch) => {
          expect(ch).to.be.ok;
          return 'test';
        });
        expect(result).to.equal('test');
        expect(pool.size).to.equal(5);
      });

      it('should acquire a channel and run the function, then release the channel if function fails', async () => {
        const [_, err] = await Try.catch(() => pool.acquireAndRun(async (ch) => {
          expect(ch).to.be.ok;
          throw new Error('test');
        }));
        expect(err).to.instanceOf(Error);
        expect(err?.message).to.equal('test');
        expect(pool.size).to.equal(5);
      });

      it('should handle hundreds of concurrent acquireAndRun() calls without unhandled rejections', async () => {
        const getAllChannels = async () => {
          const channels: amqplib.Channel[] = [];
          for (let i = 0; i < pool.capacity; i++) {
            await pool.acquireAndRun(async (ch) => {
              channels.push(ch);
            });
          }
          return channels;
        }

        const channelsBefore = await getAllChannels();
        const numCalls = 1000;
        const promises: Promise<any>[] = [];
        for (let i = 0; i < numCalls; i++) {
          promises.push(pool.acquireAndRun(async (ch) => {
            expect(ch).to.be.ok;
            return 'test';
          }));
        }
        await Promise.all(promises);

        const channelsAfter = await getAllChannels();
        expect(channelsAfter).to.have.members(channelsBefore);

        await retry(15, 500, async () => {
          const channels = await getChannelsOnConnection(rabbitmqConnectionInfo.name);
          expect(channels.length).to.equal(pool.capacity);
        });
      });
    });
  });

  describe('Event Emission & Observability', () => {
    before(async () => {
      pool = new ChannelPool(conn, 5, 1);
    });
    after(async () => {
      await pool.close().catch(() => {});
    });

    it('should emit an open event when the pool is opened', async () => {
      const emitOpenPromise = once(pool, 'open');
      await pool.open();
      await emitOpenPromise;
    });

    it('should emit a close event when the pool is closed', async () => {
      const emitClosePromise = once(pool, 'close');
      await pool.close();
      await emitClosePromise;
    });

    it('should emit a closing event when the pool is closing', async () => {
      const emitClosingPromise = once(pool, 'closing');
      await pool.close();
      await emitClosingPromise;
    });

    it('should emit a channelLost and a channelReplaced event when a channel is closed by server', async () => {
      await pool.open();

      const emitChannelLostPromise = once(pool, 'channelLost');
      const emitChannelReplacedPromise = once(pool, 'channelReplaced');

      // Acquire a channel and cause a channel-level close with an invalid operation
      let dyingChannel: amqplib.Channel | undefined;
      const [_, err] = await Try.catch(() => pool.acquireAndRun(async (ch) => {
        dyingChannel = ch;
        await ch.consume('non-existent-queue', () => {}, { noAck: true });
      }));
      expect(err).to.be.instanceOf(Error);
      expect(err?.message).to.include('NOT_FOUND - no queue');

      // Wait for the channelLost event to be emitted
      const [lostCh, lostErr] = await emitChannelLostPromise;
      expect(lostCh).to.be.equal(dyingChannel);
      expect(lostErr).to.be.instanceOf(Error);
      expect(lostErr?.message).to.include('NOT_FOUND - no queue');

      // Wait for the channelReplaced event to be emitted
      const [oldCh, newCh] = await emitChannelReplacedPromise;
      expect(oldCh).to.be.ok;
      expect(newCh).to.be.ok;
      expect(oldCh).to.not.equal(newCh);
      expect(pool.size).to.equal(5);
    });
  });

  describe('Channel Failure & Recovery', () => {
    before(async () => {
      pool = new ChannelPool(conn, 5, 1);
    });
    after(async () => {
      await pool.close().catch(() => {});
    });

    // Channel closes while pool is closing: Replacement is not attempted (size shrinks); close() still resolves cleanly.
    it('should not replace a channel that closes while the pool is closing', async () => {
      /**
       * 1. Open the pool
       * 2. Acquire a channel
       * 3. Close the pool, don't await for it to finish
       * 4. Cause a channel-level close with an invalid operation: consume on a non-existent queue
       * 5.
       */
    });
  });
});
