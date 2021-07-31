import { expect } from 'chai';
import { brokerOptions } from './config';

describe('Broker', () => {
  it('can access rabbitmq', () => {
    expect(brokerOptions.connection!.host, 'RABBITMQ_HOST').not.to.be.undefined;
  })
});
