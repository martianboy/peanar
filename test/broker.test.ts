import { expect } from 'chai';

describe('Broker', () => {
  it('can access rabbitmq', () => {
    console.log(process.env.RABBITMQ_HOST);
    expect(process.env.RABBITMQ_HOST).not.to.be.empty;
  })
});
