import { expect } from 'chai';
import amqplib from 'amqplib';
import { brokerOptions } from './config';
import Broker from '../src/amqplib_compat/broker';

describe('Broker', () => {
  it('can access RabbitMQ', async () => {
    expect(brokerOptions.connection!.host, 'RABBITMQ_HOST').not.to.be.undefined;
    const broker = new Broker(brokerOptions);
    await broker.connect();
    await broker.shutdown();
  });

  it('initializes the channel pool', async () => {
    const broker = new Broker(brokerOptions);
    await broker.connect();
    expect(broker.pool).not.to.be.undefined;
    expect(broker.pool?.isOpen).to.be.true;
    expect(broker.pool?.numFreeChannels).to.be.equal(brokerOptions.poolSize);
    await broker.shutdown();
    expect(broker.pool).to.be.undefined;
  });

  it('does not connect again using the same connection', async () => {
    const broker = new Broker(brokerOptions);
    const p1 = broker.connect();
    const p2 = broker.connect();

    expect(p1 === p2).to.be.true;
    await p1;
    await broker.shutdown();
  });

  it('throws an error if RabbitMQ unavailable', async () => {
    const broker = new Broker({
      connection: {
        ...brokerOptions.connection!,
        port: 1234,
        maxRetries: 0
      },
      poolSize: 1
    });
    return broker.connect().then(() => {
      throw new Error('Unexpected connection to invalid AMQP server!');
    }, ex => {
      expect(ex.code).to.be.equal('ECONNREFUSED');
    });
  });

  it('tries the specified number of times to connect', async () => {
    const orig = amqplib.connect;
    let num_called = 0;
    amqplib.connect = function(url, socketOptions?: any) {
      num_called += 1;
      return orig(url, socketOptions);
    };

    const broker = new Broker({
      connection: {
        ...brokerOptions.connection!,
        port: 1234,
        maxRetries: 3,
        retryDelay: 5, // ms
      },
      poolSize: 1
    });

    try {
      await broker.connect();
      throw new Error('Unexpected connection to invalid AMQP server!');
    } catch {
      expect(num_called).to.be.equal(4);
    } finally {
      amqplib.connect = orig;
    }
  });
});
