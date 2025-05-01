import crypto from 'crypto';
import { setTimeout as timeout } from 'timers/promises';

import { expect } from 'chai';
import amqplib, { ChannelModel } from 'amqplib';

import { brokerOptions } from './config';
import Broker from '../src/broker';
import { createVhost, deleteVhost } from './rabbitmq-http/client';
import { IMessage } from 'ts-amqp/dist/interfaces/Basic';
import { once } from 'events';

class TestBroker extends Broker {
  createConnection() {
    return this._connectAmqp();
  }

  get connection(): ChannelModel | undefined {
    return this.conn;
  }

  set connection(conn: ChannelModel | undefined) {
    this.conn = conn;
  }

  get channelConsumers() {
    return this._channelConsumers;
  }

  set port(port: number) {
    this.config!.connection!.port = port;
  }
  set retryDelay(retryDelay: number) {
    this.config!.connection!.retryDelay = retryDelay;
  }
  set maxRetries(maxRetries: number) {
    this.config!.connection!.maxRetries = maxRetries;
  }
}

describe('Broker', () => {
  describe('Connection', function() {
    let broker: TestBroker;
    beforeEach(function() {
      broker = new TestBroker(brokerOptions);
    });
    afterEach(async function() {
      await broker.shutdown().catch(() => {});
    });
    it('can access RabbitMQ', async () => {
      console.log(brokerOptions);
      expect(brokerOptions.connection!.host, 'RABBITMQ_HOST').not.to.be.undefined;
      const broker = new Broker(brokerOptions);
      await broker.connect();
    });

    it('initializes the channel pool', async () => {
      await broker.connect();
      expect(broker.pool).not.to.be.undefined;
      expect(broker.pool?.isOpen).to.be.true;
      expect(broker.pool?.size).to.be.equal(brokerOptions.poolSize);
    });

    it.skip('does not connect again using the same connection', async () => {
      const p1 = broker.connect();
      const p2 = broker.connect();

      expect(p1 === p2).to.be.true;
      await Promise.allSettled([p1, p2]);
    });

    it.skip('throws an error if RabbitMQ unavailable', async () => {
      broker.port = 1234;
      broker.retryDelay = 5; // 5ms
      broker.maxRetries = 3;

      return broker.connect().then(() => {
        throw new Error('Unexpected connection to invalid AMQP server!');
      }, ex => {
        expect(ex.code).to.be.equal('ECONNREFUSED');
      });
    });

    it.skip('tries the specified number of times to connect', async () => {
      const orig = amqplib.connect;
      let num_called = 0;
      amqplib.connect = (url, socketOptions?: any) => {
        num_called += 1;
        return orig.call(amqplib, url, socketOptions);
      };

      broker.port = 1234;
      broker.retryDelay = 5; // 5ms
      broker.maxRetries = 3;

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

  describe('Functionality', function() {
    const vhost = crypto.randomBytes(5).toString('hex');

    const broker = new TestBroker({
      poolSize: 1,
      connection: {
        ...brokerOptions.connection!,
        vhost
      }
    });

    before(async function() {
      await createVhost(vhost);

      await broker.connect();
      return broker;
    });

    after(async function() {
      await broker.shutdown();
      await deleteVhost(vhost);
    });

    it('can declare queues', async () => {
      await broker.queues([{
        name: 'q1',
        auto_delete: false,
        durable: false,
        exclusive: false
      }]);

      await broker.pool!.acquireAndRun(ch => ch.checkQueue('q1'));
    });

    it('can declare exchanges', async () => {
      await broker.exchanges([{
        name: 'e1',
        durable: false,
        type: 'direct',
      }]);

      await broker.pool!.acquireAndRun(ch => ch.checkExchange('e1'));
    });

    it('can bind exchanges to queues', async () => {
      await broker.bindings([{
        exchange: 'e1',
        queue: 'q1',
        routing_key: '#'
      }]);

      await broker.pool!.acquireAndRun(ch => ch.checkExchange('e1'));
    });

    async function publish(args: IMessage<unknown>) {
      const payload = { username: 'martianboy' };
      await broker.publish({
        body: payload,
        ...args
      });

      await broker.pool!.acquireAndRun(async ch => {
        let resolveFn: (msg: amqplib.ConsumeMessage | null) => void;
        const promise = new Promise<amqplib.ConsumeMessage | null>(resolve => {
          resolveFn = resolve;
        });

        const consumer = await ch.consume('q1', msg => resolveFn(msg));

        const delivery = await promise;
        await ch.cancel(consumer.consumerTag);
        ch.ackAll();

        if (!delivery) throw new Error('Empty delivery after publish!');
        const body = JSON.parse(delivery.content.toString('utf-8'));
        expect(body).to.include(payload);
      });
    }

    it('can publish a message to an exchange', async function() {
      await publish({
        routing_key: '#',
        exchange: 'e1',
      });
    });
    it('can publish a message to the default exchange', async function() {
      await publish({
        routing_key: 'q1',
      });
    });

    it.skip('can consume from a queue', async function() {
      const consumer = await broker.consume('q1');
      const { consumerCount } = await broker.pool!.acquireAndRun(async ch => {
        return await ch.checkQueue('q1');
      });

      expect(consumerCount).to.be.eq(1);
      await consumer.cancel();

      expect(broker.channelConsumers.size).to.eq(1);
      expect([...broker.channelConsumers.values()][0].size).to.be.eq(0);
    });

    it('doesn\'t rewire if no consumers are registered', async function() {
      // cause a channel error
      await broker.pool!.acquireAndRun(async ch => {
        return ch.assertQueue('q1', { exclusive: true });
      }).then(() => {
        throw new Error('Expected assertQueue to fail!');
      }, () => {});
    });

    it('can rewire consumers to a new channel when one is lost', async function() {
      const consumers = await Promise.all(broker.consumeOver(['q1', 'q1', 'q1']));
      expect(consumers).to.have.length(3);

      // cause a channel error
      await broker.pool!.acquireAndRun(async ch => {
        return ch.assertQueue('q1', { exclusive: true });
      }).then(() => {
        throw new Error('Expected assertQueue to fail!');
      }, () => {});

      // Await the resume event on each consumer which is the signal that it is
      // back on a new channel and receiving messages again.
      await Promise.all(consumers.map(c => once(c.consumer, 'resume')));

      // Cancel all consumers
      await Promise.all(consumers.map(c => c.consumer.cancel()));
    });

    it('can consume from multiple queues', async function() {
      const consumers = await Promise.all(broker.consumeOver(['q1', 'q1', 'q1']));
      const { consumerCount } = await broker.pool!.acquireAndRun(async ch => {
        return await ch.checkQueue('q1');
      });

      expect(consumerCount).to.be.eq(3);
      await Promise.all(consumers.map(c => c.consumer.cancel()));
    });

    it('can publish multiple messages without overloading a channel', async function() {
      await broker.queues([{
        name: 'q2',
        auto_delete: false,
        durable: false,
        exclusive: false
      }]);

      let returnedFalseYet = false;
      for (let i = 0; i < 2500; i++) {
        const ret = await broker.publish({
          routing_key: 'q2',
          body: { message: 'Hello, World!' }
        });

        returnedFalseYet ||= !ret;
      }

      expect(returnedFalseYet).to.be.true;

      await broker.pool?.acquireAndRun(async ch => {
        let all_received = false;
        for (let i = 0; i < 5; i++) {
          const { messageCount } = await ch.checkQueue('q2');
          if (messageCount === 2500) {
            all_received = true;
            break;
          }
          await timeout(5);
        }
        expect(all_received, 'not all messages were received after 5 trials').to.be.true;
        await ch.deleteQueue('q2');
      });
    });
  });

  describe('Error handling', function() {
    describe('#consume()', function() {
      it('throws when not connected', async function() {
        const broker = new Broker(brokerOptions);
        try {
          await broker.consume('q1');
          throw new Error('Expected an error but none was thrown.');
        } catch (ex) {
          return;
        }
      });
    });
    describe('#shutdown()', function() {
    });
  });
});
