import { Transform, TransformCallback } from "stream";
import PeanarApp from "./app";
import PeanarJob from "./job";

let counter = 0;

export default class PeanarWorker extends Transform {
  private app: PeanarApp;
  private n: number;

  constructor(app: PeanarApp) {
    super({
      objectMode: true
    });

    this.app = app;
    this.n = counter++;
  }

  async run(job: PeanarJob) {
    this.app.log(`PeanarWorker#${this.n}: run()`);


    try {
      const result = await job.perform()

      this.push({
        status: 'SUCCESS',
        result
      })
    } catch (ex) {
      this.push({
        status: 'FAILURE',
        ex
      })
    } finally {
      job.ack()
    }
  }

  /**
   * @param {import('./job')} job
   * @param {string} _encoding
   * @param {import('stream').TransformCallback} cb
   */
  _transform(job: PeanarJob, _encoding: string, cb: TransformCallback) {
    this.run(job).then(_ => cb(), _ => cb());
  }
}
