import EventEmitter from 'events';
import { expect } from 'chai';
import { rejects } from 'assert';
import sinon from 'sinon';
import amqplib, { Replies } from 'amqplib';

import NodeAmqpBroker from './broker';
import { ChannelPool } from './pool';
import Consumer from './consumer';

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

let mockChannel: MockChannel;

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


// Stub amqplib.connect before importing the broker
const connectStub = sinon.stub(amqplib, 'connect');

describe('NodeAmqpBroker', () => {
  let broker: NodeAmqpBroker;
  let sandbox: sinon.SinonSandbox;
  const config = { poolSize: 5, prefetch: 1 };

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    // Create a fresh mock channel for each test
    mockChannel = createMockChannel(1);

    // Restore stubs and mocks
    connectStub.resetHistory();
    connectStub.resetBehavior();
    connectStub.resolves(mockConnection as any);

    broker = new NodeAmqpBroker(config);

    // Reset mockConnection stubs
    Object.values(mockConnection).forEach(stub => (stub as sinon.SinonStub).resetHistory?.());
    connectionEmitter.removeAllListeners();
  });

  afterEach(() => {
    sandbox.restore();
  });

  after(() => {
    // Explicitly restore the original amqplib.connect if necessary
    connectStub.restore();
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

    it.skip('should consider shutdown a noop if not connected', async () => {
      await broker.shutdown();

      sinon.assert.notCalled(mockConnection.close);
      sinon.assert.notCalled(poolCloseSpy);
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
      const newChannel = createMockChannel(2);
      const consumer = new Consumer(mockChannel, 'test-queue');
      (broker as any)._channelConsumers.set(mockChannel, new Set([consumer]));

      await (broker as any).rewireConsumersOnChannel(mockChannel, newChannel);

      sinon.assert.calledOnce(newChannel.consume);
      expect((broker as any)._channelConsumers.get(newChannel)).to.include(consumer);
      expect((broker as any)._channelConsumers.get(mockChannel)).to.be.undefined;
    });
  });
});
