import EventEmitter, { once } from 'events';
import { expect } from 'chai';
import { rejects } from 'assert';
import sinon from 'sinon';
import amqplib, { Replies } from 'amqplib';

import { Try } from '../test/utils';

import NodeAmqpBroker from './broker';
import { ChannelPool } from './pool';

class ServerShutdownError extends Error {
  name = 'ServerShutdownError';
  code = 500;
  constructor() {
    super('Server shutdown');
  }
}

// Mock amqplib Channel
const createMockChannel = (id: number) => ({
  channelNumber: id, // Add an identifier for easier debugging
  isDead: false,
  assertQueue: sinon.stub().resolves(),
  assertExchange: sinon.stub().resolves(),
  bindQueue: sinon.stub().resolves(),
  consume: sinon.stub().resolves({ consumerTag: `test-consumer-tag-${id}` } as Replies.Consume),
  publish: sinon.stub().returns(true),
  close: sinon.stub().resolves(),
  on: sinon.stub(),
  once: sinon.stub(),
  prefetch: sinon.stub().resolves(),
  ack: sinon.stub(),
  nack: sinon.stub(),
  cancel: sinon.stub().resolves(), // Add cancel stub
  removeAllListeners: sinon.stub(), // Add removeAllListeners stub
});

type MockChannel = ReturnType<typeof createMockChannel>;

// Mock amqplib Connection
const connectionEmitter = new EventEmitter();

const mockConnection = {
  channelCount: 1,
  createChannel: sinon.stub().callsFake(() => Promise.resolve(createMockChannel(++mockConnection.channelCount))),
  close: sinon.stub().resolves(),
  on: connectionEmitter.on.bind(connectionEmitter),
  off: connectionEmitter.off.bind(connectionEmitter),
  once: connectionEmitter.once.bind(connectionEmitter),
};

describe('NodeAmqpBroker', () => {
  let broker: NodeAmqpBroker;
  let sandbox: sinon.SinonSandbox;
  const config = { poolSize: 5, prefetch: 1 };
  let connectStub: sinon.SinonStub;

  let mockChannel: MockChannel;

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    connectStub = sandbox.stub(amqplib, 'connect');
    // Create a fresh mock channel for each test
    mockChannel = createMockChannel(1);

    // Restore stubs and mocks
    connectStub.resetHistory();
    connectStub.resetBehavior();
    connectStub.resolves(mockConnection);

    broker = new NodeAmqpBroker(config);

    // Reset mockConnection stubs
    Object.values(mockConnection).forEach(stub => (stub as sinon.SinonStub).resetHistory?.());
    connectionEmitter.removeAllListeners();
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('connect', () => {
    it('should connect to the broker and create channels', async () => {
      await broker.connect();

      sinon.assert.calledOnce(connectStub);
      sinon.assert.called(mockConnection.createChannel);
      expect(broker.pool?.size).to.equal(config.poolSize);
    });

    it('should throw on non-retryable errors', async () => {
      const error = new Error('Connection error');
      connectStub.rejects(error);

      await rejects(broker.connect(), error);
    });

    it('should retry connection on ECONNREFUSED', async () => {
      class ConnectionRefusedError extends Error {
        code = 'ECONNREFUSED';
        name = 'ConnectionRefusedError';
        constructor() {
          super('Connection refused');
        }
      }
      const error = new ConnectionRefusedError();
      connectStub.onCall(0).rejects(error);
      connectStub.onCall(1).resolves(mockConnection as any);

      sandbox.stub(global, 'setTimeout').callsFake((cb: () => void) => cb() as any);
      await broker.connect();

      sinon.assert.calledTwice(connectStub);
      sinon.assert.called(mockConnection.createChannel);
      expect(broker.pool?.size).to.equal(config.poolSize);
    });

    it('should reconnect on connection error after initial connection', async () => {
      await broker.connect();
      connectStub.resetHistory();

      // Simulate a connection error
      const error = new ServerShutdownError();
      connectionEmitter.emit('close', error);

      // Wait for the broker to reconnect
      await broker.ready();

      sinon.assert.calledOnce(connectStub);
      sinon.assert.called(mockConnection.createChannel);
      expect(broker.pool?.size).to.equal(config.poolSize);
    });
  });

  describe('shutdown', () => {
    let poolCloseSpy: sinon.SinonSpy;
    beforeEach(() => {
      poolCloseSpy = sinon.spy(ChannelPool.prototype, 'close');
      poolCloseSpy.resetHistory();
    });
    afterEach(() => {
      poolCloseSpy.restore();
    });

    it('should close the connection and channels', async () => {
      await broker.connect();
      await broker.shutdown();

      sinon.assert.calledOnce(mockConnection.close);
      sinon.assert.calledOnce(poolCloseSpy);
      expect(() => broker.ready()).to.throw('Not connected!');
      expect(broker.pool).to.be.undefined;
    });

    it('should consider shutdown a noop if not connected', async () => {
      const [_, err] = await Try.catch(() => broker.shutdown());
      expect(err).to.be.null;

      sinon.assert.notCalled(mockConnection.close);
      sinon.assert.notCalled(poolCloseSpy);
    });

    it('should not reconnect halfway through shutdown', async () => {
      await broker.connect();
      connectStub.resetHistory();

      // Acquire a channel to hold for a while
      const { release } = await broker.pool!.acquire();

      // Begin shutdown
      const shutdownPromise = broker.shutdown();

      // Simulate a server-initiated connection close
      connectionEmitter.emit('close', new ServerShutdownError());

      // Ensure that no new connection attempt was made
      sinon.assert.notCalled(connectStub);
      sinon.assert.notCalled(mockConnection.close);

      // Release the channel
      release();

      // Wait for the shutdown to complete
      await shutdownPromise;

      // Ensure that the pool is closed
      expect(broker.pool).to.be.undefined;
      sinon.assert.calledOnce(poolCloseSpy);
      sinon.assert.calledOnce(mockConnection.close);
      expect(() => broker.ready()).to.throw('Not connected!');
    });
  });

  describe('connection error handling', () => {
    class ConnectionError extends Error {
      name = 'ConnectionError';
      constructor(public code: number) {
        super('Connection refused');
      }
    }

    it('should reconnect on connection error', async () => {
      await broker.connect();
      connectStub.resetHistory();

      connectionEmitter.emit('close', new ConnectionError(500));
      await broker.ready();

      sinon.assert.calledOnce(connectStub);
      sinon.assert.called(mockConnection.createChannel);
      expect(broker.pool?.size).to.equal(config.poolSize);
    });

    for (const error of [undefined, new ConnectionError(200)]) {
      it(`should not reconnect on ${error ? 'graceful server shutdown' : 'no'} error`, async () => {
        await broker.connect();
        connectStub.resetHistory();

        connectionEmitter.emit('close', error);
        expect(() => broker.ready()).to.throw('Not connected!');
        sinon.assert.notCalled(connectStub);
        expect(broker.pool).to.be.undefined;
        expect(connectionEmitter.listenerCount('close')).to.equal(0);
      });
    }
  });

  describe('rewireConsumersOnChannel', () => {
    it('should rewire consumers to a new channel', async () => {
      await broker.connect();
      const consumer = await broker.consume('test-queue');

      const newChannel = createMockChannel(6);
      const oldChannel = consumer.channel;

      broker.pool!.emit('channelReplaced', oldChannel, newChannel);
      await once(consumer, 'resume');

      sinon.assert.calledOnce(newChannel.consume);
      expect((broker as any)._channelConsumers.get(newChannel)).to.include(consumer);
      expect((broker as any)._channelConsumers.get(mockChannel)).to.be.undefined;
    });

    it('should skip rewiring if the channel does not start a consumer', async () => {
      await broker.connect();
      const consumer = await broker.consume('test-queue');
      const newChannel = createMockChannel(6);
      const oldChannel = consumer.channel;

      // Simulate a channel that does not start a consumer
      newChannel.consume.rejects(new Error('Consumer not started'));

      broker.pool!.emit('channelReplaced', oldChannel, newChannel);
      await new Promise(r => setImmediate(r));   // give dispatch loop a tick

      sinon.assert.calledOnce(newChannel.consume);
      expect((broker as any)._channelConsumers.get(newChannel)).to.be.empty;
    });
  });
});
