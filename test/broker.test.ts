import { expect } from 'chai';
import { brokerOptions } from './config';
import Broker from '../src/amqplib_compat/broker';
// import { getChannels, getConnections } from './rabbitmq-http/client';

describe('Broker', () => {
  it('can access rabbitmq', async () => {
    expect(brokerOptions.connection!.host, 'RABBITMQ_HOST').not.to.be.undefined;
    const broker = new Broker(brokerOptions);
    await broker.connect();
    // const connections = await getConnections();
    // const channels = await getChannels();
    await broker.shutdown();
  })
});
