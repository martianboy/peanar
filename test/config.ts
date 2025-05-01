const fs = require('fs');

const useTLS = process.env.RABBITMQ_TLS === 'true';
const useTLSVerify = process.env.RABBITMQ_TLS_VERIFY === 'true';
const caPath = process.env.RABBITMQ_CA_PATH;
const certPath = process.env.RABBITMQ_CLIENT_CERT_PATH;
const keyPath = process.env.RABBITMQ_CLIENT_KEY_PATH;

const ca = caPath ? fs.readFileSync(caPath) : undefined;
const cert = certPath ? fs.readFileSync(certPath) : undefined;
const key = keyPath ? fs.readFileSync(keyPath) : undefined;
const tlsOptions = useTLS ?{
  rejectUnauthorized: useTLSVerify,
  ca: [ca],
  cert,
  key,
} : undefined;

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
  poolSize: 1,
  socketOptions: {
    ...tlsOptions,
  },
};
