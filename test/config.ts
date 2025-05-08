import { IConnectionParams } from "../src/types";

export const brokerOptions = {
  connection: {
    host: process.env.RABBITMQ_HOST ?? '127.0.0.1',
    port: parseInt(process.env.RABBITMQ_PORT ?? '5672'),
    protocol: process.env.RABBITMQ_SSL_MODE === 'true' ? 'amqps' : 'amqp',
    username: 'guest',
    password: 'guest',
    vhost: '/',
    timeout: 10_000,
    maxRetries: 10,
    retryDelay: 1000,
    locale: 'en-US'
  } as IConnectionParams,
  poolSize: 1
};
