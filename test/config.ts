export const brokerOptions = {
  connection: {
    host: process.env.RABBITMQ_HOST ?? '127.0.0.1',
    port: parseInt(process.env.RABBITMQ_PORT ?? '5672'),
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
