import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { spy, restore } from 'simple-mock';
import { isAsyncIterable } from 'iterall';
import { RedisPubSub } from '../redis-pubsub';
import * as IORedis from 'ioredis';

chai.use(chaiAsPromised);
const expect = chai.expect;

// -------------- Mocking Redis Client ------------------

let listener;

const publishSpy = spy((channel, message) => listener && listener(channel, message));
const subscribeSpy = spy((channel, cb) => cb && cb(null, channel));
const unsubscribeSpy = spy((channel, cb) => cb && cb(channel));
const psubscribeSpy = spy((channel, cb) => cb && cb(null, channel));
const punsubscribeSpy = spy((channel, cb) => cb && cb(channel));

const quitSpy = spy(cb => cb);
const mockRedisClient = {
  publish: publishSpy,
  subscribe: subscribeSpy,
  unsubscribe: unsubscribeSpy,
  psubscribe: psubscribeSpy,
  punsubscribe: punsubscribeSpy,
  on: (event, cb) => {
    if (event === 'messageBuffer') {
      listener = cb;
    }
  },
  quit: quitSpy,
};
const mockOptions = {
  publisher: (mockRedisClient as any),
  subscriber: (mockRedisClient as any),
};



// -------------- Mocking Redis Client ------------------

describe('RedisPubSub', () => {

  it('should create default ioredis clients if none were provided', done => {
    const pubSub = new RedisPubSub();
    expect(pubSub.getSubscriber()).to.be.an.instanceOf(IORedis);
    expect(pubSub.getPublisher()).to.be.an.instanceOf(IORedis);
    pubSub.close();
    done();
  });

  it('should verify close calls pub and sub quit methods', done => {
    const pubSub = new RedisPubSub(mockOptions);
    pubSub.close();
    expect(quitSpy.callCount).to.equal(2);
    done();
  });

  it('can subscribe to specific redis channel and called when a message is published on it', done => {
    const pubSub = new RedisPubSub(mockOptions);
    pubSub.subscribe('Posts', message => {
      try {
        expect(message).to.equals('test');
        done();
      } catch (e) {
        done(e);
      }

    }).then(async subId => {
      expect(subId).to.be.a('number');
      await pubSub.publish('Posts', 'test');
      pubSub.unsubscribe(subId);
    });
  });

  it('can subscribe to a redis channel pattern and called when a message is published on it', done => {
    const pubSub = new RedisPubSub(mockOptions);

    pubSub.subscribe('Posts*', message => {
      try {
        expect(psubscribeSpy.callCount).to.equal(1);
        expect(message).to.equals('test');
        done();
      } catch (e) {
        done(e);
      }
    }, { pattern: true }).then(async subId => {
      expect(subId).to.be.a('number');
      await pubSub.publish('Posts*', 'test');
      pubSub.unsubscribe(subId);
    });
  });

  it('can unsubscribe from specific redis channel', done => {
    const pubSub = new RedisPubSub(mockOptions);
    pubSub.subscribe('Posts', () => null).then(subId => {
      pubSub.unsubscribe(subId);

      try {

        expect(unsubscribeSpy.callCount).to.equals(1);
        expect(unsubscribeSpy.lastCall.args).to.have.members(['Posts']);

        expect(punsubscribeSpy.callCount).to.equals(1);
        expect(punsubscribeSpy.lastCall.args).to.have.members(['Posts']);

        done();

      } catch (e) {
        done(e);
      }
    });
  });

  it('cleans up correctly the memory when unsubscribing', done => {
    const pubSub = new RedisPubSub(mockOptions);
    Promise.all([
      pubSub.subscribe('Posts', () => null),
      pubSub.subscribe('Posts', () => null),
    ])
      .then(([subId, secondSubId]) => {
        try {
          // This assertion is done against a private member, if you change the internals, you may want to change that
          expect((pubSub as any).subscriptionMap[subId]).not.to.be.an('undefined');
          pubSub.unsubscribe(subId);
          // This assertion is done against a private member, if you change the internals, you may want to change that
          expect((pubSub as any).subscriptionMap[subId]).to.be.an('undefined');
          expect(() => pubSub.unsubscribe(subId)).to.throw(`There is no subscription of id "${subId}"`);
          pubSub.unsubscribe(secondSubId);
          done();

        } catch (e) {
          done(e);
        }
      });
  });

  it('will not unsubscribe from the redis channel if there is another subscriber on it\'s subscriber list', done => {
    const pubSub = new RedisPubSub(mockOptions);
    const subscriptionPromises = [
      pubSub.subscribe('Posts', () => {
        done('Not supposed to be triggered');
      }),
      pubSub.subscribe('Posts', (msg) => {
        try {
          expect(msg).to.equals('test');
          done();
        } catch (e) {
          done(e);
        }
      }),
    ];

    Promise.all(subscriptionPromises).then(async subIds => {
      try {
        expect(subIds.length).to.equals(2);

        pubSub.unsubscribe(subIds[0]);
        expect(unsubscribeSpy.callCount).to.equals(0);

        await pubSub.publish('Posts', 'test');
        pubSub.unsubscribe(subIds[1]);
        expect(unsubscribeSpy.callCount).to.equals(1);
      } catch (e) {
        done(e);
      }
    });
  });

  it('will subscribe to redis channel only once', done => {
    const pubSub = new RedisPubSub(mockOptions);
    const onMessage = () => null;
    const subscriptionPromises = [
      pubSub.subscribe('Posts', onMessage),
      pubSub.subscribe('Posts', onMessage),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);
        expect(subscribeSpy.callCount).to.equals(1);

        pubSub.unsubscribe(subIds[0]);
        pubSub.unsubscribe(subIds[1]);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('can have multiple subscribers and all will be called when a message is published to this channel', done => {
    const pubSub = new RedisPubSub(mockOptions);
    const onMessageSpy = spy(() => null);
    const subscriptionPromises = [
      pubSub.subscribe('Posts', onMessageSpy as Function),
      pubSub.subscribe('Posts', onMessageSpy as Function),
    ];

    Promise.all(subscriptionPromises).then(async subIds => {
      try {
        expect(subIds.length).to.equals(2);

        await pubSub.publish('Posts', 'test');

        expect(onMessageSpy.callCount).to.equals(2);
        onMessageSpy.calls.forEach(call => {
          expect(call.args).to.have.members(['test']);
        });

        pubSub.unsubscribe(subIds[0]);
        pubSub.unsubscribe(subIds[1]);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('can publish objects as well', done => {
    const pubSub = new RedisPubSub(mockOptions);
    pubSub.subscribe('Posts', message => {
      try {
        expect(message).to.have.property('comment', 'This is amazing');
        done();
      } catch (e) {
        done(e);
      }
    }).then(async subId => {
      try {
        await pubSub.publish('Posts', { comment: 'This is amazing' });
        pubSub.unsubscribe(subId);
      } catch (e) {
        done(e);
      }
    });
  });

  it('can accept custom reviver option (eg. for Javascript Dates)', done => {
    const dateReviver = (key, value) => {
      const isISO8601Z = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)Z$/;
      if (typeof value === 'string' && isISO8601Z.test(value)) {
        const tempDateNumber = Date.parse(value);
        if (!isNaN(tempDateNumber)) {
          return new Date(tempDateNumber);
        }
      }
      return value;
    };

    const pubSub = new RedisPubSub({...mockOptions, reviver: dateReviver});
    const validTime = new Date();
    const invalidTime = '2018-13-01T12:00:00Z';
    pubSub.subscribe('Times', message => {
      try {
        expect(message).to.have.property('invalidTime', invalidTime);
        expect(message).to.have.property('validTime');
        expect(message.validTime.getTime()).to.equals(validTime.getTime());
        done();
      } catch (e) {
        done(e);
      }
    }).then(async subId => {
      try {
        await pubSub.publish('Times', { validTime, invalidTime });
        pubSub.unsubscribe(subId);
      } catch (e) {
        done(e);
      }
    });
  });

  it('throws if you try to unsubscribe with an unknown id', () => {
    const pubSub = new RedisPubSub(mockOptions);
    return expect(() => pubSub.unsubscribe(123))
      .to.throw('There is no subscription of id "123"');
  });

  it('can use transform function to convert the trigger name given into more explicit channel name', done => {
    const triggerTransform = (trigger, { repoName }) => `${trigger}.${repoName}`;
    const pubSub = new RedisPubSub({
      triggerTransform,
      publisher: (mockRedisClient as any),
      subscriber: (mockRedisClient as any),
    });

    const validateMessage = message => {
      try {
        expect(message).to.equals('test');
        done();
      } catch (e) {
        done(e);
      }
    };

    pubSub.subscribe('comments', validateMessage, { repoName: 'graphql-redis-subscriptions' }).then(async subId => {
      await pubSub.publish('comments.graphql-redis-subscriptions', 'test');
      pubSub.unsubscribe(subId);
    });

  });

  // TODO pattern subs

  afterEach('Reset spy count', () => {
    publishSpy.reset();
    subscribeSpy.reset();
    unsubscribeSpy.reset();
    psubscribeSpy.reset();
    punsubscribeSpy.reset();
  });

  after('Restore redis client', () => {
    restore();
  });

});

describe('PubSubAsyncIterator', () => {

  it('should expose valid asyncItrator for a specific event', () => {
    const pubSub = new RedisPubSub(mockOptions);
    const eventName = 'test';
    const iterator = pubSub.asyncIterator(eventName);
    // tslint:disable-next-line:no-unused-expression
    expect(iterator).to.exist;
    // tslint:disable-next-line:no-unused-expression
    expect(isAsyncIterable(iterator)).to.be.true;
  });

  it('should trigger event on asyncIterator when published', async () => {
    const pubSub = new RedisPubSub(mockOptions);
    const eventName = 'test';
    const iterator = pubSub.asyncIterator(eventName);

    const promise = iterator.next();

    pubSub.publish(eventName, { test: true });
    await promise.then(result => {
      // tslint:disable-next-line:no-unused-expression
      expect(result).to.exist;
      // tslint:disable-next-line:no-unused-expression
      expect(result.value).to.exist;
      // tslint:disable-next-line:no-unused-expression
      expect(result.done).to.exist;
    });
  });

  it('should not trigger event on asyncIterator when publishing other event', async () => {
    const pubSub = new RedisPubSub(mockOptions);
    const eventName = 'test2';
    const iterator = pubSub.asyncIterator('test');
    const triggerSpy = spy(() => undefined);

    iterator.next().then(triggerSpy);
    await pubSub.publish(eventName, { test: true });
    expect(triggerSpy.callCount).to.equal(0);
  });

  it('register to multiple events', async () => {
    const pubSub = new RedisPubSub(mockOptions);
    const eventName = 'test2';
    const iterator = pubSub.asyncIterator(['test', 'test2']);
    const triggerSpy = spy(() => undefined);

    const promise = iterator.next();
    pubSub.publish(eventName, { test: true });
    await promise.then(() => {
      triggerSpy();
      expect(triggerSpy.callCount).to.be.gte(1);
    });
  });

  it('should not trigger event on asyncIterator already returned', async () => {
    const pubSub = new RedisPubSub(mockOptions);
    const eventName = 'test';
    const iterator = pubSub.asyncIterator<any>(eventName);

    const p1 = iterator.next();

    await pubSub.publish(eventName, { test: 'word' });
    const p2 = iterator.next();

    const p3 =  iterator.return();
    pubSub.publish(eventName, { test: true });

    await p1.then(result => {
      // tslint:disable-next-line:no-unused-expression
      expect(result).to.exist;
      // tslint:disable-next-line:no-unused-expression
      expect(result.value).to.exist;
      expect(result.value.test).to.equal('word');
      // tslint:disable-next-line:no-unused-expression
      expect(result.done).to.be.false;
    });
    await p2.then(result => {
      // tslint:disable-next-line:no-unused-expression
      expect(result).to.exist;
      // tslint:disable-next-line:no-unused-expression
      expect(result.value).not.to.exist;
      // tslint:disable-next-line:no-unused-expression
      expect(result.done).to.be.true;
    });
    await p3;
  });

});
