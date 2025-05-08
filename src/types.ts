import type PeanarJob from "./job";

export enum EAppState {
  RUNNING = 'RUNNING',
  CLOSING = 'CLOSING',
  CLOSED = 'CLOSED'
}

export interface IBasicProperties {
  contentType?: string;
  contentEncoding?: string;
  headers?: Record<string, unknown>;
  deliveryMode?: number;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
  expiration?: string;
  messageId?: string;
  timestamp?: Date;
  type?: string;
  userId?: string;
  appId?: string;
  clusterId?: string;
}
export interface IEnvelope {
  deliveryTag: bigint;
  redeliver: boolean;
  exchange: string;
  routingKey: string;
}
export interface IDelivery {
  envelope: IEnvelope;
  properties: IBasicProperties;
  body?: Buffer;
}
export interface IMessage<B = Buffer> {
  exchange?: string;
  routing_key: string;
  mandatory?: boolean;
  immediate?: boolean;
  properties?: IBasicProperties;
  body?: B;
}

export interface IPeanarJobDefinitionInput {
  queue: string;
  name?: string;
  routingKey?: string;
  exchange?: string;
  handler: (...args: any[]) => Promise<any>;

  jobClass?: typeof PeanarJob;

  expires?: number;
  retry_exchange?: string;
  error_exchange?: string;
  max_retries?: number;
  retry_delay?: number;
  delayed_run_wait?: number;

  max_priority?: number;
  default_priority?: number;
}

export interface IPeanarJobDefinition {
  name: string;
  queue: string;
  handler: (...args: any[]) => Promise<any>;

  routingKey: string;
  exchange?: string;

  jobClass?: typeof PeanarJob;

  expires?: number;
  retry_exchange?: string;
  error_exchange?: string;
  max_retries?: number;
  retry_delay?: number;
  delayed_run_wait?: number;

  max_priority?: number;
  default_priority?: number;
}

export interface IPeanarRequest {
  id: string;
  name: string;
  args: any[];
  attempt: number;
  deliveryTag?: bigint;
  priority?: number;
}

export interface IPeanarJob extends IPeanarJobDefinition, IPeanarRequest {
  deliveryTag: bigint;
}

export interface IPeanarResponse {
  id: string;
  name: string;
  status: 'SUCCESS' | 'FAILURE';
  error?: unknown;
  result?: unknown;
}

export interface IPeanarOptions {
  connection?: IConnectionParams;
  poolSize?: number;
  prefetch?: number;
  jobClass?: typeof PeanarJob;
  logger?(...args: any[]): any;
}

export interface IConnectionParams {
  maxRetries: number;
  retryDelay: number;
  protocol?: 'amqp' | 'amqps';
  host: string;
  port: number;
  username: string;
  password: string;
  locale: string;
  keepAlive?: boolean;
  keepAliveDelay?: number;
  timeout?: number;
  vhost: string;
}

export declare type EExchangeType = 'direct' | 'fanout' | 'topic' | 'headers';
export interface IExchangeArgs {
    alternateExchange?: string;
}
export interface IExchange {
    name: string;
    type: EExchangeType;
    durable: boolean;
    arguments?: IExchangeArgs;
}
export interface IQueueArgs {
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
  expires?: number;
  lazy?: boolean;
  maxLength?: number;
  maxLengthBytes?: number;
  maxPriority?: number;
  messageTtl?: number;
  overflow?: 'drop-head' | 'reject-publish';
  queueMasterLocator?: boolean;
}
export interface IQueue {
  name: string;
  durable: boolean;
  exclusive: boolean;
  auto_delete: boolean;
  arguments?: IQueueArgs;
}
export interface IQueueDeclareResponse {
  queue: string;
  message_count: number;
  consumer_count: number;
}
export interface IBinding {
  queue: string;
  exchange: string;
  routing_key: string;
}
export interface IQueuePurgeResponse {
  message_count: number;
}
