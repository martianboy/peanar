export type RetryOpts = {
  // do not defer process exit by waiting on resulting Timeout
  unref?: boolean
  // backoffStrategy step function to calculate next delay, e.g. `t => 2*t`, defaults to linear
  backoffStrategy?: (previousDelay:number) => number
  // stop retries prematurely based on the error thrown from it
  stopCondition?: (err:Error) => boolean
}

/**
 * retry a function until it does not reject
 *
 * @param {number} count maximum no. of times to retry
 * @param {number} delay time b/w two consecutive retries
 * @param {Function} fn function to execute everytime
 * @param {RetryOpts} opts RetryOpts
 * @returns {Promise<any>}
 */
// eslint-disable-next-line @typescript-eslint/ban-types
export function retry(count:number, delay:number, fn:Function, opts:RetryOpts={}): Promise<any> {
  return new Promise((resolve, reject) => {
    const res = fn();
    if (res && res.then)
      res.then(resolve, reject)
    else
      resolve(res);
  })
  .catch(err => {
    if (count-- === 0 || opts.stopCondition?.(err))
      throw err;
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        retry(count, opts.backoffStrategy?.(delay) || delay, fn, opts).then(resolve, reject);
      }, delay)
      if (opts.unref) t.unref()
    });
  });
}

export class Try {
  public static async catch<T>(fn: (...args: unknown[]) => Promise<T>): Promise<[T | null, Error | null]> {
    try {
      const result = await fn();
      return [result, null];
    } catch (error) {
      if (error instanceof Error) {
        return [null, error];
      }
      return [null, new Error('Unknown error')];
    }
  }
}
