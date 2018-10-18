import { RedisOptions, Redis as RedisClient } from 'ioredis';
import { PubSubEngine } from 'graphql-subscriptions';
import { PubSubAsyncIterator } from './pubsub-async-iterator';

export interface PubSubRedisOptions {
  connection?: RedisOptions;
  triggerTransform?: TriggerTransform;
  connectionListener?: (err: Error) => void;
  publisher?: RedisClient;
  subscriber?: RedisClient;
  reviver?: Reviver;
}

type Subscription = {
  triggerName: string,
  onMessage: Function,
  binary: boolean,
};

export class RedisPubSub implements PubSubEngine {
  constructor(options: PubSubRedisOptions = {}) {
    const {
      triggerTransform,
      connection,
      connectionListener,
      subscriber,
      publisher,
      reviver,
    } = options;

    this.triggerTransform = triggerTransform || (trigger => trigger as string);
    this.reviver = reviver;

    if (subscriber && publisher) {
      this.redisPublisher = publisher;
      this.redisSubscriber = subscriber;
    } else {
      try {
        const IORedis = require('ioredis');
        this.redisPublisher = new IORedis(connection);
        this.redisSubscriber = new IORedis(connection);

        if (connectionListener) {
          this.redisPublisher.on('connect', connectionListener);
          this.redisPublisher.on('error', connectionListener);
          this.redisSubscriber.on('connect', connectionListener);
          this.redisSubscriber.on('error', connectionListener);
        } else {
          this.redisPublisher.on('error', console.error);
          this.redisSubscriber.on('error', console.error);
        }
      } catch (error) {
        console.error(
          `No publisher or subscriber instances were provided and the package 'ioredis' wasn't found. Couldn't create Redis clients.`,
        );
      }
    }

    // TODO support for pattern based message
    this.redisSubscriber.on('messageBuffer', this.onMessage.bind(this));

    this.subscriptionMap = {};
    this.subsRefsMap = {};
    this.currentSubscriptionId = 0;
  }

  public async publish(trigger: string, payload: any, options: {binary?: boolean} = {}): Promise<void> {
    await this.redisPublisher.publish(trigger, options.binary ? payload : JSON.stringify(payload));
  }

  public subscribe(
    trigger: string,
    onMessage: Function,
    options: {binary?: boolean, repoName?: string} = {},
  ): Promise<number> {
    const triggerName: string = this.triggerTransform(trigger, options);
    const id = this.currentSubscriptionId++;
    this.subscriptionMap[id] = {triggerName, onMessage, binary: !!options.binary};

    const refs = this.subsRefsMap[triggerName];
    if (refs && refs.length > 0) {
      const newRefs = [...refs, id];
      this.subsRefsMap[triggerName] = newRefs;
      return Promise.resolve(id);
    } else {
      return new Promise<number>((resolve, reject) => {
        // TODO Support for pattern subs
        this.redisSubscriber.subscribe(triggerName, err => {
          if (err) {
            reject(err);
          } else {
            this.subsRefsMap[triggerName] = [
              ...(this.subsRefsMap[triggerName] || []),
              id,
            ];
            resolve(id);
          }
        });
      });
    }
  }

  public unsubscribe(subId: number) {
    const {triggerName = null} = this.subscriptionMap[subId] || {};
    const refs = this.subsRefsMap[triggerName];

    if (!refs) throw new Error(`There is no subscription of id "${subId}"`);

    if (refs.length === 1) {
      this.redisSubscriber.unsubscribe(triggerName);
      delete this.subsRefsMap[triggerName];
    } else {
      const index = refs.indexOf(subId);
      const newRefs =
        index === -1
          ? refs
          : [...refs.slice(0, index), ...refs.slice(index + 1)];
      this.subsRefsMap[triggerName] = newRefs;
    }
    delete this.subscriptionMap[subId];
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers);
  }

  public getSubscriber(): RedisClient {
    return this.redisSubscriber;
  }

  public getPublisher(): RedisClient {
    return this.redisPublisher;
  }

  public close(): void {
    this.redisPublisher.quit();
    this.redisSubscriber.quit();
  }

  private onMessage(channel: string, message: Buffer) {
    let parsedMessage: any = message;
    let messageParseAttempted = false;

    for (const subId of this.subsRefsMap[channel] || []) {
      const {onMessage, binary} = this.subscriptionMap[subId];
      if (!binary && !messageParseAttempted) {
        messageParseAttempted = true;
        try {
          parsedMessage = parsedMessage.toString();
          parsedMessage = JSON.parse(parsedMessage, this.reviver);
        } catch (e) {
          // swallow parse error and return raw string
        }
      }
      onMessage(binary ? message : parsedMessage);
    }
  }

  private triggerTransform: TriggerTransform;
  private redisSubscriber: RedisClient;
  private redisPublisher: RedisClient;
  private reviver: Reviver;

  private subscriptionMap: { [subId: number]: Subscription };
  private subsRefsMap: { [trigger: string]: Array<number> };
  private currentSubscriptionId: number;
}

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (
  trigger: Trigger,
  channelOptions?: Object,
) => string;
export type Reviver = (key: any, value: any) => any;
