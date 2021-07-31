'use strict';
const child_process = require('child_process');
const net = require('net');
const { Docker } = require('node-docker-api');

const docker = new Docker({ socketPath: '/var/run/docker.sock' });

const HOSTNAME = 'peanar-rabbitmq-test';
const IMAGE_NAME = 'rabbitmq:management';

const acShutdown = new AbortController();

/**
 * @param {import('node-docker-api/src/image').Image} image
 */
async function createRabbitContainer() {
  await docker.container.create({
    name: 'peanar-rabbitmq-test',
    hostname: HOSTNAME,
    image: IMAGE_NAME
  });
}

/**
 *
 * @param {import('node-docker-api/src/container').Container} container
 * @param {{ signal: AbortSignal }} options
 * @returns
 */
function checkPorts(container, { signal }) {
  const ip = container?.data?.NetworkSettings?.Networks?.bridge?.IPAddress
  if (!ip) {
    throw new Error('Could not determine the IP address of the container.');
  }

  return new Promise((resolve, reject) => {
    try {
      const socket = net.connect({ host: ip, port: '5672', timeout: 500 }, () => {
        socket.destroy();
        resolve();
      });
    } catch (ex) {
      if (signal.aborted) {
        return reject(ex);
      }

      return checkPorts(container);
    }
  });
}

process.on('SIGINT', () => acShutdown.abort());
process.on('SIGTERM', () => acShutdown.abort());

async function main() {
  const [image] = await docker.image.list({
    filters: { reference: [IMAGE_NAME] }
  });
  if (!image) {
    throw new Error(`rabbitmq:management image not found.
    run docker pull rabbitmq:management`);
  }

  let [container] = await docker.container.list({
    all: true,
    filters: { ancestor: [IMAGE_NAME] }
  });

  if (!container) {
    await createRabbitContainer();
  }

  if (container.data.State !== 'running') {
    await container.start();
    container = docker.container.get(container.id)
  }

  await checkPorts(container, { signal: acShutdown.signal });

  console.log('RabbitMQ is up and running!');
  const child = child_process.spawn('yarn', ['mocha'], {
    env: {
      RABBITMQ_HOST: container?.data?.NetworkSettings?.Networks?.bridge?.IPAddress,
      RABBITMQ_PORT: '5672'
    }
  });
  child.stdout.pipe( process.stdout );
  console.log(child.pid);
}

main().catch(ex => console.error(ex))
