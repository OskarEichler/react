/**
 * @jest-environment node
 */
'use strict';

// The Flight async debug tracking in ReactFlightServerConfigDebugNode is being
// moved from async_hooks (which reduces every Promise to an integer id) to
// v8.promiseHooks (which passes the Promise instances themselves). Node
// implements async_hooks' Promise events on top of the same V8 promise hooks,
// so the two event streams should correspond 1:1. This test runs both systems
// side by side over an adversarial workload and asserts the invariants the
// rewrite depends on:
//
//   A. Both systems see the same set of Promises.
//   B. v8 parent !== undefined <=> async_hooks trigger !== execution id, and
//      parent is exactly the Promise whose asyncId === triggerAsyncId.
//   C. A stack maintained from v8 before/after (push / scan-pop) reproduces
//      executionAsyncId() whenever the execution context is a Promise
//      continuation, and is empty when it isn't.
//   D. settled parity with promiseResolve.
//   E. after fires (the stack unwinds) even when a handler throws.

const async_hooks = require('async_hooks');
const {promiseHooks} = require('v8');

const describeIfPromiseHooks = promiseHooks != null ? describe : describe.skip;

describeIfPromiseHooks('v8.promiseHooks / async_hooks equivalence', () => {
  beforeEach(() => {
    // The workload needs real timers and setImmediate so that continuations
    // run in genuine macrotask/microtask interleavings.
    jest.useRealTimers();
  });

  it('delivers equivalent Promise events through both APIs', async () => {
    const tags = new WeakMap(); // promise -> tag
    let nextTag = 1;
    function tagOf(p) {
      let t = tags.get(p);
      if (t === undefined) {
        t = nextTag++;
        tags.set(p, t);
      }
      return t;
    }

    const events = [];
    const asyncIdOfTag = new Map(); // tag -> asyncId (from async_hooks init)
    const promiseAsyncIds = new Set();

    // --- async_hooks side (the old implementation's event source) ---
    const ahHook = async_hooks.createHook({
      init(asyncId, type, triggerAsyncId, resource) {
        if (type !== 'PROMISE') return;
        const tag = tagOf(resource);
        asyncIdOfTag.set(tag, asyncId);
        promiseAsyncIds.add(asyncId);
        events.push({
          sys: 'ah',
          kind: 'init',
          tag,
          asyncId,
          trigger: triggerAsyncId,
          exec: async_hooks.executionAsyncId(),
        });
      },
      promiseResolve(asyncId) {
        events.push({sys: 'ah', kind: 'settled', asyncId});
      },
    });
    ahHook.enable();

    // --- v8.promiseHooks side (the new implementation's event source) ---
    // Maintain the stack exactly like the rewrite's executingPromises.
    const stack = [];
    const stop = promiseHooks.createHook({
      init(promise, parent) {
        events.push({
          sys: 'v8',
          kind: 'init',
          tag: tagOf(promise),
          parentTag: parent === undefined ? null : tagOf(parent),
          exec: async_hooks.executionAsyncId(),
          stackTop: stack.length > 0 ? stack[stack.length - 1] : null,
          stackLen: stack.length,
        });
      },
      before(promise) {
        stack.push(tagOf(promise));
      },
      after(promise) {
        const tag = tagOf(promise);
        events.push({
          sys: 'v8',
          kind: 'after',
          tag,
        });
        for (let i = stack.length - 1; i >= 0; i--) {
          if (stack[i] === tag) {
            stack.length = i;
            return;
          }
        }
        events.push({sys: 'v8', kind: 'after-miss', tag});
      },
      settled(promise) {
        events.push({sys: 'v8', kind: 'settled', tag: tagOf(promise)});
      },
    });

    // ---------- adversarial workload ----------
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    let thrownContinuationTag = null;
    const cached = sleep(2).then(() => 'cached');

    async function workload() {
      // plain await chain
      await Promise.resolve(1);
      await sleep(1);

      // throwing then-handler, caught
      const throwing = Promise.resolve(2).then(() => {
        throw new Error('boom');
      });
      thrownContinuationTag = tagOf(throwing);
      await throwing.catch(() => {});

      // throwing async function + await of the rejection
      const rejecting = (async () => {
        await sleep(1);
        throw new Error('boom2');
      })();
      try {
        await rejecting;
      } catch (x) {}

      // userland thenable (PromiseResolveThenableJob)
      const thenable = {
        then(resolve) {
          setTimeout(() => resolve('thenable'), 1);
        },
      };
      await thenable;

      // async function returning a thenable
      await (async () => thenable)();

      // combinators
      await Promise.all([sleep(1), Promise.resolve(3), (async () => 4)()]);
      await Promise.race([sleep(1), sleep(2)]);
      await Promise.allSettled([
        (async () => {
          throw new Error('x');
        })(),
        sleep(1),
      ]);

      // async generator consumed like a for await loop
      async function* gen() {
        for (let i = 0; i < 3; i++) {
          await sleep(1);
          yield i;
        }
      }
      const iterator = gen();
      while (true) {
        const step = await iterator.next();
        if (step.done) {
          break;
        }
      }

      // microtask/macrotask interleaving inside continuations
      await Promise.resolve().then(
        () =>
          new Promise(resolve => {
            queueMicrotask(() => {
              Promise.resolve().then(() => {});
              process.nextTick(() => {
                Promise.resolve().then(() => setImmediate(resolve));
              });
            });
          }),
      );

      // deeply nested sync resolution
      await Promise.resolve(Promise.resolve(Promise.resolve(5)));

      // await of a cached promise created before this frame
      await cached;
    }

    // Sampled from a macrotask so no promise continuation (including this
    // test's own awaits) is currently executing.
    let stackDepthAtIdle = -1;
    try {
      await workload();
      await new Promise(resolve => {
        setImmediate(() => {
          stackDepthAtIdle = stack.length;
          resolve();
        });
      });
    } finally {
      stop();
      ahHook.disable();
    }

    // ---------- analysis ----------
    const failures = [];
    const ahInits = new Map();
    const v8Inits = new Map();
    const v8SettledTags = new Map();
    const ahSettledIds = new Map();
    events.forEach(e => {
      if (e.sys === 'ah' && e.kind === 'init') {
        ahInits.set(e.tag, e);
      }
      if (e.sys === 'v8' && e.kind === 'init') {
        v8Inits.set(e.tag, e);
      }
      if (e.sys === 'ah' && e.kind === 'settled') {
        ahSettledIds.set(e.asyncId, (ahSettledIds.get(e.asyncId) || 0) + 1);
      }
      if (e.sys === 'v8' && e.kind === 'settled') {
        v8SettledTags.set(e.tag, (v8SettledTags.get(e.tag) || 0) + 1);
      }
      if (e.kind === 'after-miss') {
        failures.push(`v8 after with no stack entry for tag ${e.tag}`);
      }
    });

    // A: init parity
    v8Inits.forEach((v8e, tag) => {
      if (!ahInits.has(tag)) {
        failures.push(`v8 saw promise ${tag}, async_hooks did not`);
      }
    });
    ahInits.forEach((ahe, tag) => {
      if (!v8Inits.has(tag)) {
        failures.push(`async_hooks saw promise ${tag}, v8 did not`);
      }
    });

    // B: parent <-> trigger equivalence
    v8Inits.forEach((v8e, tag) => {
      const ahe = ahInits.get(tag);
      if (!ahe) {
        return;
      }
      const chained = v8e.parentTag !== null;
      const ahChained = ahe.trigger !== ahe.exec;
      if (chained !== ahChained) {
        failures.push(
          `tag ${tag}: v8 parent=${v8e.parentTag} but ah trigger=${ahe.trigger} exec=${ahe.exec}`,
        );
      }
      if (
        chained &&
        // Parents created before the hooks enabled (e.g. by the test runner)
        // were never inited by either system, so their id mapping is unknown.
        asyncIdOfTag.has(v8e.parentTag) &&
        asyncIdOfTag.get(v8e.parentTag) !== ahe.trigger
      ) {
        failures.push(
          `tag ${tag}: parent tag ${v8e.parentTag} has asyncId ` +
            `${asyncIdOfTag.get(v8e.parentTag)}, ah trigger is ${ahe.trigger}`,
        );
      }
    });

    // C: stack top reproduces executionAsyncId at every promise creation
    v8Inits.forEach((v8e, tag) => {
      if (promiseAsyncIds.has(v8e.exec)) {
        const topAsyncId =
          v8e.stackTop === null ? null : asyncIdOfTag.get(v8e.stackTop);
        if (topAsyncId !== v8e.exec) {
          failures.push(
            `tag ${tag}: created with exec id ${v8e.exec} (a promise) but ` +
              `simulated stack top is tag ${v8e.stackTop} (asyncId ${topAsyncId})`,
          );
        }
      } else if (v8e.stackLen !== 0) {
        failures.push(
          `tag ${tag}: created outside promise context (exec ${v8e.exec}) ` +
            `but stack depth is ${v8e.stackLen}`,
        );
      }
    });

    // D: settled parity. Only meaningful for promises both systems inited:
    // v8 settled fires even for promises created before the hook was
    // registered (e.g. by the test runner), while async_hooks promiseResolve
    // only fires for promises it assigned an id to. Both the old and the new
    // implementation ignore settles of promises they never tracked, so the
    // boundary difference is unobservable.
    v8SettledTags.forEach((n, tag) => {
      if (!v8Inits.has(tag) || !ahInits.has(tag)) {
        return;
      }
      const m = ahSettledIds.get(asyncIdOfTag.get(tag)) || 0;
      if (n !== m) {
        failures.push(`tag ${tag}: v8 settled ${n}x, ah promiseResolve ${m}x`);
      }
    });

    // E: the throwing continuation still unwound the stack
    const sawAfterForThrown = events.some(
      e =>
        e.sys === 'v8' && e.kind === 'after' && e.tag === thrownContinuationTag,
    );
    if (!sawAfterForThrown) {
      failures.push(
        'no v8 after event for the continuation whose handler threw',
      );
    }
    if (stackDepthAtIdle !== 0) {
      failures.push(`stack not empty at idle: depth ${stackDepthAtIdle}`);
    }

    expect(failures).toEqual([]);
    // Sanity that the workload actually exercised the interesting shapes.
    expect(v8Inits.size).toBeGreaterThan(50);
    expect(
      Array.from(v8Inits.values()).filter(e => e.parentTag !== null).length,
    ).toBeGreaterThan(20);
  });
});
