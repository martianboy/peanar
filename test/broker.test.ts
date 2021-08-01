import { expect } from 'chai';
import amqplib from 'amqplib';
import { brokerOptions } from './config';
import Broker from '../src/amqplib_compat/broker';

describe('Broker', () => {
  it('can access rabbitmq', async () => {
    expect(brokerOptions.connection!.host, 'RABBITMQ_HOST').not.to.be.undefined;
    const broker = new Broker(brokerOptions);
    await broker.connect();
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
