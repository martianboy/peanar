import { IBrokerOptions } from '../src/amqplib_compat/broker';

export const brokerOptions: IBrokerOptions = {
  connection: {
    host: process.env.RABBITMQ_HOST ?? '',
    port: parseInt(process.env.RABBITMQ_PORT ?? ''),
    username: 'guest',
    password: 'guest',
    vhost: '/',
    timeout: 10_000,
    maxRetries: 10,
    retryDelay: 1000,
    locale: 'en-US'
  },
  poolSize: 1
};
