/// <reference types="node" />
import { EventEmitter } from 'events';
import { Channel } from 'amqplib';
declare type Releaser = () => void;
export interface ChannelWithReleaser {
  release: Releaser;
  channel: Channel;
}

export interface ChannelCreator {
  createChannel(): Promise<Channel>;

  on(event: 'error', listener: () => void): this;
  on(event: 'close', listener: () => void): this;
  once(event: 'error', listener: () => void): this;
  once(event: 'close', listener: () => void): this;
}

export class ChannelPool extends EventEmitter {
  private _queue;
  protected _pool: Channel[];
  private _acquisitions;
  private _releaseResolvers;
  private _conn;
  private _size;
  private _isOpen;
  private prefetch?;
  constructor(connection: ChannelCreator, size: number, prefetch?: number);
  private softCleanUp;
  private hardCleanUp;
  [Symbol.asyncIterator](): AsyncIterableIterator<ChannelWithReleaser>;
  readonly size: number;
  readonly isOpen: boolean;
  open(): Promise<void>;
  close(): Promise<void>;
  acquire(): Promise<ChannelWithReleaser>;
  mapOver<T, R>(arr: T[], fn: (ch: Channel, item: T) => Promise<R>): Promise<R>[];
  acquireAndRun<R>(fn: (ch: Channel) => Promise<R>): Promise<R>;
  private onChannelClose;
  private openChannel;
  private releaser;
  private dispatchChannels;

  on(event: 'channelLost', listener: (ch: Channel, err: unknown) => void): this;
  once(event: 'channelLost', listener: (ch: Channel, err: unknown) => void): this;

  on(event: 'channelReplaced', listener: (ch: Channel, newCh: Channel) => void): this;
  once(event: 'channelReplaced', listener: (ch: Channel, newCh: Channel) => void): this;
}
