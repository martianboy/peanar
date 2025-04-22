import { expect } from 'chai';
import sinon from 'sinon';
import amqplib, { Replies } from 'amqplib';

import NodeAmqpBroker from './broker';

// Mock amqplib Channel
const createMockChannel = (id: number) => ({
  channelNumber: id, // Add an identifier for easier debugging
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
const mockConnection = {
  channelCount: 1,
  createChannel: sinon.stub().callsFake(() => Promise.resolve(createMockChannel(++mockConnection.channelCount))),
  close: sinon.stub().resolves(),
  on: sinon.stub(),
  once: sinon.stub(),
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
    connectStub.resolves(mockConnection as any);

    broker = new NodeAmqpBroker(config);

    // Reset mockConnection stubs
    Object.values(mockConnection).forEach(stub => (stub as sinon.SinonStub).resetHistory?.());
  });

  afterEach(() => {
    sandbox.restore();
    // Explicitly restore the original amqplib.connect if necessary
    connectStub.restore();
  });

  it('works', function() {
    expect(broker).to.be.instanceOf(NodeAmqpBroker);
  });
});
