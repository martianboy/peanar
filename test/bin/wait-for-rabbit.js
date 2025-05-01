const net = require('net');
const debug = require('debug')('docker');
const { setTimeout: timeout } = require('timers/promises');

const RABBITMQ_HOST = process.env.RABBITMQ_HOST
const RABBITMQ_PORT = process.env.RABBITMQ_PORT
const acShutdown = new AbortController();

/**
 * @param {{ signal: AbortSignal }} options
 */
function checkPorts({ signal }) {
  return new Promise((resolve) => {
    try {
      debug(`Connecting to ${RABBITMQ_HOST} on port ${RABBITMQ_PORT}...`);
      const socket = net.connect({
        host: RABBITMQ_HOST,
        port: RABBITMQ_PORT,
        timeout: 500,
        signal
      }, () => {
        debug('Connection successful.');
        socket.destroy();
        acShutdown.abort();
        resolve();
      });
    } catch (ex) {
      console.error(ex);
      if (signal.aborted) {
        return resolve()
      }

      return timeout(500, undefined, { signal })
        .catch(() => {
          debug('Failed to connect to RabbitMQ instance after 500ms timeout. Retrying...');
        })
        .then(() => checkPorts({ signal }));
    }
  });
}

process.on('SIGINT', () => acShutdown.abort());
process.on('SIGTERM', () => acShutdown.abort());

async function main() {
  const t = setTimeout(() => acShutdown.abort(), 5000);
  acShutdown.signal.addEventListener('abort', () => clearTimeout(t));
  await checkPorts({ signal: acShutdown.signal })
}

main().catch(ex => console.error(ex))
