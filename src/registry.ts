import { IPeanarJobDefinition, IPeanarJobDefinitionInput } from './app';
import { PeanarInternalError } from './exceptions';

export default class Registry {
  private _jobs: Map<string, IPeanarJobDefinition> = new Map();
  private _queues: Map<string, Map<string, IPeanarJobDefinition>> = new Map();

  private _prepareJobDefinition(def: IPeanarJobDefinitionInput): IPeanarJobDefinition {
    return {
      routingKey: def.queue,
      exchange: '',
      ...def,
      name: def.name && def.name.length ? def.name : def.handler.name
    };
  }

  public registerQueue(queue: string): void {
    if (this._queues.has(queue)) throw new PeanarInternalError('Queue is already registered.');

    this._queues.set(queue, new Map());
  }

  public registerJob(def: IPeanarJobDefinitionInput): IPeanarJobDefinition {
    const job_def = this._prepareJobDefinition(def);

    if (!job_def.name || job_def.name.length < 1) throw new PeanarInternalError('Invalid job name.');

    this._jobs.set(job_def.name, job_def);

    const queue_defs = this._queues.get(def.queue);
    if (!queue_defs) {
      this._queues.set(job_def.queue, new Map([[job_def.name, job_def]]));
    } else {
      queue_defs.set(job_def.name, job_def);
    }

    return job_def;
  }

  get queues(): string[] {
    return [...this._queues.keys()];
  }

  public getQueueDefs(queue: string): Map<string, IPeanarJobDefinition> {
    const queue_defs = this._queues.get(queue);
    if (!queue_defs) {
      throw new PeanarInternalError(`Queue ${queue} is not registered.`);
    }

    return queue_defs;
  }

  public getJobDefinition(queue: string, name: string): IPeanarJobDefinition {
    const queue_defs = this.getQueueDefs(queue);
    const def = queue_defs.get(name);

    if (!def) {
      throw new PeanarInternalError(`Job definition for ${name} in queue ${queue} not found.`);
    }

    return def;
  }
}
