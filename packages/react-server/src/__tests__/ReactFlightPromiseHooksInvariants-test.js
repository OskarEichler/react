/**
 * @jest-environment node
 */
'use strict';

// The async debug tracking in ReactFlightServerConfigDebugNode relies on a
// few properties of v8.promiseHooks that are not spelled out in its
// documentation. This asserts them directly against the runtime so a Node or
// V8 upgrade that changes them fails loudly instead of silently corrupting
// async debug info. (An earlier version of this test also verified the
// events against async_hooks, which the previous implementation was built
// on; see the commit that introduced it.)
//
//   A. init receives the created Promise and the Promise it chains from.
//   B. A stack maintained from before/after (push / scan-pop) tracks the
//      currently executing continuation, unwinds even when a handler
//      throws, and is empty outside promise continuations.
//   C. settled fires for both fulfilled and rejected Promises.

const {promiseHooks} = require('v8');

const describeIfPromiseHooks = promiseHooks != null ? describe : describe.skip;

describeIfPromiseHooks('v8.promiseHooks invariants', () => {
  beforeEach(() => {
    // The workload needs real timers and setImmediate so that continuations
    // run in genuine macrotask/microtask interleavings.
    jest.useRealTimers();
  });

  it('provides the events the async debug tracking depends on', async () => {
    const stack = [];
    const parents = new WeakMap(); // promise -> parent promise
    const settled = new WeakSet();
    const failures = [];

    const stop = promiseHooks.createHook({
      init(promise, parent) {
        // The hooks are realm-wide, so promises from other contexts (e.g.
        // the test runner's) arrive here too and are not instanceof this
        // realm's Promise. The tracking only needs object identity.
        if (typeof promise !== 'object' || promise === null) {
          failures.push('init did not receive a Promise object');
        }
        if (parent !== undefined) {
          parents.set(promise, parent);
        }
      },
      before(promise) {
        stack.push(promise);
      },
      after(promise) {
        for (let i = stack.length - 1; i >= 0; i--) {
          if (stack[i] === promise) {
            stack.length = i;
            return;
          }
        }
        failures.push('after fired with no matching stack entry');
      },
      settled(promise) {
        settled.add(promise);
      },
    });

    const sleep = ms => new Promise(r => setTimeout(r, ms));

    // A: parent identity for .then chains and awaits.
    const base = sleep(1);
    let insideTop = null;
    const cont = base.then(() => {
      insideTop = stack.length > 0 ? stack[stack.length - 1] : null;
    });
    await cont;
    if (parents.get(cont) !== base) {
      failures.push('parent of a .then continuation is not the base promise');
    }
    // B: while the handler ran, the executing continuation was on top.
    if (insideTop !== cont) {
      failures.push('stack top during a handler is not the continuation');
    }

    // B: the stack unwinds when a handler throws.
    const throwing = sleep(1).then(() => {
      throw new Error('boom');
    });
    try {
      await throwing;
    } catch (x) {}

    // C: settled fires for rejections too, including rejected async
    // functions awaited across a macrotask boundary.
    const rejecting = (async () => {
      await sleep(1);
      throw new Error('boom2');
    })();
    try {
      await rejecting;
    } catch (x) {}
    if (!settled.has(throwing) || !settled.has(rejecting)) {
      failures.push('settled did not fire for a rejected promise');
    }
    if (!settled.has(cont)) {
      failures.push('settled did not fire for a fulfilled promise');
    }

    // Exercise interleavings that historically produced surprising orders.
    await Promise.all([sleep(1), Promise.resolve(1), (async () => 2)()]);
    await Promise.resolve().then(
      () =>
        new Promise(resolve => {
          queueMicrotask(() => {
            process.nextTick(() => {
              Promise.resolve().then(() => setImmediate(resolve));
            });
          });
        }),
    );
    await {
      then(resolve) {
        setTimeout(() => resolve('thenable'), 1);
      },
    };

    // B: sampled from a macrotask so no promise continuation (including this
    // test's own awaits) is currently executing.
    let stackDepthAtIdle = -1;
    await new Promise(resolve => {
      setImmediate(() => {
        stackDepthAtIdle = stack.length;
        resolve();
      });
    });
    stop();
    if (stackDepthAtIdle !== 0) {
      failures.push(`stack not empty at idle: depth ${stackDepthAtIdle}`);
    }

    expect(failures).toEqual([]);
  });
});
