export class PeanarInternalError extends Error {}
export class PeanarAdapterError extends Error {}
export class PeanarJobError extends Error {}
export class PeanarJobCancelledError extends PeanarJobError {
  public underlyingError?: Error;

  constructor(underlyingError?: Error) {
    super(underlyingError?.message);
    this.underlyingError = underlyingError;
  }
}
export class PeanarPoolError extends Error {}
