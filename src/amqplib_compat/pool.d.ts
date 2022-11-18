/// <reference types="node" />
import { EventEmitter } from 'events';
import { Channel, Connection } from 'amqplib'
declare type Releaser = () => void;
interface ChannelWithReleaser {
    release: Releaser;
    channel: Channel;
}
export class ChannelPool extends EventEmitter {
    private _queue;
    private _pool;
    private _acquisitions;
    private _releaseResolvers;
    private _conn;
    private _size;
    private _isOpen;
    private prefetch?;
    constructor(connection: Connection, size: number, prefetch?: number);
    private softCleanUp;
    private hardCleanUp;
    [Symbol.asyncIterator](): AsyncIterableIterator<ChannelWithReleaser>;
    readonly size: number;
    readonly numFreeChannels: number;
    readonly isOpen: boolean;
    open(): Promise<void>;
    close(): Promise<void>;
    acquire(): Promise<ChannelWithReleaser>;
    mapOver<T, R>(arr: T[], fn: (ch: Channel, item: T) => PromiseLike<R>): PromiseLike<R>[];
    acquireAndRun<R>(fn: (ch: Channel) => PromiseLike<R>): PromiseLike<R>;
    private onChannelClose;
    private openChannel;
    private releaser;
    private dispatchChannels;

    on(event: 'channelLost', listener: (ch: Channel, err: unknown) => void): this;
    once(event: 'channelLost', listener: (ch: Channel, err: unknown) => void): this;

    on(event: 'channelReplaced', listener: (ch: Channel, newCh: Channel) => void): this;
    once(event: 'channelReplaced', listener: (ch: Channel, newCh: Channel) => void): this;
}
