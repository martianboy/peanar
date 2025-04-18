import { IConnectionParams } from "ts-amqp/dist/interfaces/Connection";
import type PeanarJob from "./job";

export interface IPeanarJobDefinition {
  name: string;
  queue: string;
  handler: (...args: any[]) => Promise<any>;

  routingKey: string;
  exchange?: string;
  replyTo?: string;

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

export interface IPeanarJobDefinitionInput {
  queue: string;
  name?: string;
  routingKey?: string;
  exchange?: string;
  replyTo?: string;
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

export interface IPeanarRequest {
  id: string;
  name: string;
  args: any[];
  attempt: number;
  correlationId?: string;
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

export type JobDefinitionGetter = (name: string) => IPeanarJobDefinition;
