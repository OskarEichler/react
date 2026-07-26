/**
 * @jest-environment node
 */
'use strict';

// Generative test for the async debug info tracking. Each seed builds a
// random Flight render shaped like a real app: components fetch their own
// data, share a cache()-deduped data layer, waterfall through helpers, fan
// out with Promise.all, pass promises down as props, and await data that was
// preloaded before the request. Some seeds abort mid-render, throw in a
// subtree, or run two renders interleaved. A small share of the data layer
// is userland thenables and Promise subclasses.
//
// The runs are deterministic. Every random decision is drawn while the seed
// is being built, never while it runs. Async leaves park their resolvers
// with a driver; a seeded loop runs each leaf's real I/O to completion (one
// at a time, so its latency can't reorder anything), then settles batches of
// leaves back to back in the same tick, with seeded microtask and macrotask
// hops in between. Times are normalized and stacks are compared by function
// name only, so the snapshots are stable across machines and runs.
//
// To debug or explore a single seed: FUZZ_TEST_SEED=<n> yarn test ReactFlightAsyncDebugInfoFuzz

import {patchSetImmediate} from '../../../../scripts/jest/patchSetImmediate';

import fs from 'fs';
import path from 'path';

let ReactServer;
let ReactServerDOMServer;
let ReactServerDOMClient;
let Stream;
let Random;

const NUM_SEEDS = 20;
const ONLY_SEED =
  process.env.FUZZ_TEST_SEED != null ? +process.env.FUZZ_TEST_SEED : null;

const streamOptions = {
  objectMode: true,
};

function filterStackFrame(filename, functionName) {
  return (
    !filename.startsWith('node:') &&
    !filename.includes('node_modules') &&
    // Filter out our own internal source code since it'll typically be in node_modules
    (!filename.includes('/packages/') || filename.includes('/__tests__/')) &&
    !filename.includes('/build/') &&
    !functionName.includes('internal_')
  );
}

class FuzzPromise extends Promise {}

function once(source) {
  let memo = null;
  return function memoizedSource() {
    if (memo === null) {
      memo = source();
    }
    return memo;
  };
}

describe('ReactFlightAsyncDebugInfoFuzz', () => {
  beforeEach(() => {
    jest.resetModules();
    jest.useRealTimers();
    patchSetImmediate();
    global.console = require('console');

    jest.mock('react', () => require('react/react.react-server'));
    jest.mock('react-server-dom-webpack/server', () =>
      jest.requireActual('react-server-dom-webpack/server.node'),
    );
    ReactServer = require('react');
    ReactServerDOMServer = require('react-server-dom-webpack/server');

    jest.resetModules();
    jest.useRealTimers();
    patchSetImmediate();

    __unmockReact();
    jest.unmock('react-server-dom-webpack/server');
    jest.mock('react-server-dom-webpack/client', () =>
      jest.requireActual('react-server-dom-webpack/client.node'),
    );

    ReactServerDOMClient = require('react-server-dom-webpack/client');
    Stream = require('stream');
    Random = require('random-seed');
  });

  jest.setTimeout(60000);

  function finishLoadingStream(readable) {
    return new Promise(resolve => {
      if (readable.readableEnded) {
        resolve();
      } else {
        readable.on('end', () => resolve());
      }
    });
  }

  function createProgram(rand) {
    // Everything the program allocates is retained until the test ends so
    // that GC can never make a WeakRef-dependent field disappear from the
    // output of one run but not another.
    const retained = [];
    // Which io kind (timer or fs read) backs each leaf id, so checks can
    // compare the emitted attribution against what actually happened.
    const leafKinds = new Map();
    // Parked leaves: {io, settle}. The driver runs io() to completion and
    // later calls settle(), possibly in the same tick as other settles.
    const pending = [];
    let nextValue = 0;

    function retain(x) {
      retained.push(x);
      return x;
    }

    // The two kinds of real I/O behind every leaf. Their names are what the
    // io debug entries get attributed to.
    function readFromFile() {
      return new Promise(function readFixture(resolve, reject) {
        fs.readFile(
          path.join(__dirname, '../ReactFlightAsyncSequence.js'),
          function onFileRead(error) {
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          },
        );
      });
    }
    function waitForTimer() {
      return new Promise(function sleep(resolve) {
        setTimeout(resolve, 1);
      });
    }

    // A promise backed by one unit of real I/O whose resolution is parked
    // with the driver. The io kind is decided when the seed is built, never
    // when the leaf is created.
    function leaf(opts) {
      const id = 'v' + nextValue++;
      const useTimer = opts.useTimer === true;
      leafKinds.set(id, useTimer);
      const value = opts.big
        ? // Over the 1MB limit, so the serialized debug value gets omitted.
          id + ':' + 'x'.repeat(1100000)
        : opts.object
          ? {id: id, items: [1, 2, 3]}
          : id;
      const promise = new Promise((resolve, reject) => {
        pending.push({
          io: useTimer ? waitForTimer : readFromFile,
          settle() {
            if (opts.rejects) {
              reject(new Error('rejected ' + id));
            } else {
              resolve(value);
            }
          },
        });
      });
      if (opts.rejects) {
        // The driver may settle this before a consumer has attached its
        // handler; mark it handled so that can't fail the test run. This
        // adds one extra tracked await around every rejecting leaf, which
        // is visible in the snapshots.
        retain(promise.catch(function ignoreUnhandled() {}));
      }
      return retain(promise);
    }

    // ---- Sources ----
    // A source is a thunk. Components (and their helpers) invoke sources
    // while they run, so most promises are created during the render with
    // an owner, like data fetching in a real app. All random decisions are
    // made here at build time; running a thunk never draws from the seed.

    // Module-scope data: created before the render starts. A seeded subset
    // resolves before the request begins, so some awaits point at I/O that
    // finished before the request's time origin.
    const pool = [];
    for (let i = 0; i < 4; i++) {
      pool.push(leaf({useTimer: rand.intBetween(0, 1) === 0}));
    }

    // Request-deduped data layer. The fetch happens inside the cache scope,
    // so components using the same key share one fetch per request.
    const cachedIOKinds = [];
    for (let i = 0; i < 4; i++) {
      cachedIOKinds.push(rand.intBetween(0, 1) === 0);
    }
    const cachedFetch = ReactServer.cache(async function cachedFetch(key) {
      const data = await leaf({useTimer: cachedIOKinds[key]});
      return 'cached:' + key + ':' + data;
    });

    function genSource(depth) {
      const roll = rand.intBetween(1, 100);
      if (roll <= 25 || depth >= 3) {
        // A component fetching its own data.
        const useTimer = rand.intBetween(0, 1) === 0;
        if (rand.intBetween(0, 1) === 0) {
          return function fetchData() {
            return leaf({useTimer: useTimer});
          };
        }
        return async function fetchAndTransform() {
          const data = await leaf({useTimer: useTimer});
          return String(data).toUpperCase();
        };
      }
      if (roll <= 40) {
        const key = rand.intBetween(0, 3);
        return function fetchCached() {
          return cachedFetch(key);
        };
      }
      if (roll <= 50) {
        const preloaded = pool[rand.intBetween(0, pool.length - 1)];
        return function usePreloaded() {
          return preloaded;
        };
      }
      if (roll <= 65) {
        // A waterfall: each step awaits and depends on the previous step.
        const steps = [];
        const n = rand.intBetween(2, 4);
        for (let i = 0; i < n; i++) {
          steps.push(genSource(depth + 1));
        }
        return async function loadWaterfall() {
          let acc = '';
          for (let i = 0; i < steps.length; i++) {
            acc += String(await steps[i]()) + '>';
          }
          return acc;
        };
      }
      if (roll <= 75) {
        // A fan-out.
        const parts = [];
        const n = rand.intBetween(2, 3);
        for (let i = 0; i < n; i++) {
          parts.push(genSource(depth + 1));
        }
        return function loadAll() {
          return Promise.all(parts.map(part => part()));
        };
      }
      if (roll <= 81) {
        // A derived promise. Sometimes the callback itself blocks on more
        // data, which forks the await chain.
        const inner = genSource(depth + 1);
        const next = rand.intBetween(0, 1) === 0 ? genSource(depth + 1) : null;
        return function loadDerived() {
          return Promise.resolve(inner()).then(function transformResult(x) {
            return next !== null ? next() : 'transformed:' + x;
          });
        };
      }
      if (roll <= 85) {
        // Data that fails to load, handled by the data layer.
        const useTimer = rand.intBetween(0, 1) === 0;
        return function fetchWithFallback() {
          return leaf({rejects: true, useTimer: useTimer}).catch(
            function useFallback(x) {
              return 'fallback:' + x.message;
            },
          );
        };
      }
      if (roll <= 89) {
        const first = genSource(depth + 1);
        const second = genSource(depth + 1);
        return function loadFastest() {
          return Promise.race([first(), second()]);
        };
      }
      if (roll <= 92) {
        // Paging through an async iterator.
        const pages = [genSource(depth + 1), genSource(depth + 1)];
        const take = rand.intBetween(1, 2);
        return async function readPages() {
          const paginate = async function* paginate() {
            for (let i = 0; i < pages.length; i++) {
              yield await pages[i]();
            }
          };
          const iterator = paginate();
          let acc = '';
          for (let i = 0; i < take; i++) {
            const step = await iterator.next();
            if (step.done) {
              break;
            }
            acc += String(step.value) + '|';
          }
          if (iterator.return) {
            await iterator.return();
          }
          return acc;
        };
      }
      if (roll <= 94) {
        return function loadSync() {
          return Promise.resolve(
            retain(Promise.resolve(retain(Promise.resolve('sync')))),
          );
        };
      }
      if (roll <= 96) {
        // The serialized debug value of this one gets omitted for size.
        const useTimer = rand.intBetween(0, 1) === 0;
        return function fetchLargeValue() {
          return leaf({big: true, useTimer: useTimer});
        };
      }
      if (roll <= 98) {
        const useTimer = rand.intBetween(0, 1) === 0;
        return function fetchObject() {
          return leaf({object: true, useTimer: useTimer});
        };
      }
      if (roll <= 99) {
        // A userland thenable, like an ORM query object. Parked with the
        // driver like other leaves; resolves to more data.
        const inner = genSource(depth + 1);
        return function queryThenable() {
          return {
            then(resolve) {
              pending.push({
                io: async function noIO() {},
                settle() {
                  resolve(retain(Promise.resolve(inner())));
                },
              });
            },
          };
        };
      }
      // A Promise subclass, like a polyfill or instrumented promise.
      return function subclassedFetch() {
        return new FuzzPromise(resolve => {
          pending.push({
            io: async function noIO() {},
            settle() {
              resolve('sub' + nextValue++);
            },
          });
        });
      };
    }

    return {retained, pending, pool, genSource, leaf, retain, leafKinds};
  }

  function buildComponents(rand, program, componentRoots, componentCreators) {
    let componentId = 0;

    // use() must observe the same thenable when the component replays, so
    // the source is memoized.
    function makeSyncUseComponent(options) {
      const name = 'FuzzUse' + componentId++;
      componentRoots.set(name, options.rootIndex);
      const source = once(program.genSource(2));
      const Component = {
        [name]: function () {
          const value = ReactServer.use(
            program.retain(Promise.resolve(source())),
          );
          return name + ':' + String(value);
        },
      }[name];
      return Component;
    }

    function makeComponent(depth, options) {
      const name = 'Fuzz' + componentId++;
      componentRoots.set(name, options.rootIndex);
      const sources = [];
      const nAwaits = rand.intBetween(1, 2);
      for (let i = 0; i < nAwaits; i++) {
        sources.push(program.genSource(0));
      }
      // Awaits happen one after the other or all at once.
      const parallel = rand.intBetween(0, 2) === 0;
      // Sometimes the component's own data fails and it renders a fallback.
      const catches = rand.intBetween(0, 5) === 0;
      const catchesIOKind = catches ? rand.intBetween(0, 1) === 0 : false;
      // Sometimes the component has a bug and throws. The subtree errors
      // but the rest of the render completes.
      const throws = options.canThrow && rand.intBetween(0, 19) === 0;

      const children = [];
      if (depth < 3) {
        const nChildren = rand.intBetween(0, depth === 0 ? 3 : 2);
        for (let i = 0; i < nChildren; i++) {
          children.push(
            rand.intBetween(0, 4) === 0
              ? makeSyncUseComponent(options)
              : makeComponent(depth + 1, options),
          );
        }
      }
      // A slot: the parent creates an element during its own render and the
      // first child renders it, so the element's owner is not its parent in
      // the tree. Sometimes the parent also renders the same element itself,
      // which dedupes it across two locations.
      const SlotComponent =
        children.length > 0 && rand.intBetween(0, 2) === 0
          ? makeComponent(depth + 1, options)
          : null;
      // This component's body creates the child and slot elements, so it is
      // their owner regardless of where they render.
      children.forEach(Child => componentCreators.set(Child.name, name));
      if (SlotComponent !== null) {
        componentCreators.set(SlotComponent.name, name);
      }
      const dedupesSlot = SlotComponent !== null && rand.intBetween(0, 1) === 0;
      // Most components return [text, children]; some return a single child
      // element or just text.
      const returnShape =
        children.length > 0 && rand.intBetween(0, 3) === 0
          ? 'element'
          : rand.intBetween(0, 5) === 0
            ? 'text'
            : 'array';

      // A promise started by the parent and awaited by the first child.
      // TODO: Pass the source's raw result once debug info serialization
      // survives props that throw on unexpected property access (like our
      // own ClientReference proxies do). JSON.stringify's toJSON probe
      // throws while the component's debug info is serialized, the server
      // degrades the debug info to a string, and the client then corrupts
      // the component's data chunk while failing to initialize it.
      const propSource = children.length > 0 && rand.intBetween(0, 2) === 0;

      const Component = {
        [name]: async function (props) {
          let out = name + ':';
          if (parallel) {
            const values = await Promise.all(sources.map(source => source()));
            out += values.map(String).join(';');
          } else {
            for (let i = 0; i < sources.length; i++) {
              out += String(await sources[i]()) + ';';
            }
          }
          if (catches) {
            try {
              out += String(
                await program.leaf({rejects: true, useTimer: catchesIOKind}),
              );
            } catch (x) {
              out += 'recovered';
            }
          }
          if (throws) {
            throw new Error('bug in ' + name);
          }
          if (props != null && props.data != null) {
            out += '/data:' + String(await props.data);
          }
          const slot = props != null && props.slot != null ? props.slot : null;
          const slotElement =
            SlotComponent !== null
              ? ReactServer.createElement(SlotComponent, {key: 'slot'})
              : null;
          const childElements = children.map((Child, i) =>
            ReactServer.createElement(Child, {
              key: i,
              data:
                i === 0 && propSource
                  ? program.retain(Promise.resolve(sources[0]()))
                  : null,
              slot: i === 0 ? slotElement : null,
            }),
          );
          if (slot !== null) {
            childElements.push(slot);
          }
          if (dedupesSlot) {
            childElements.push(slotElement);
          }
          if (returnShape === 'element' && childElements.length > 0) {
            return childElements[0];
          }
          if (returnShape === 'text' && childElements.length === 0) {
            return out;
          }
          return [out, childElements];
        },
      }[name];
      return Component;
    }

    return {makeComponent};
  }

  async function drive(rand, program, doneRef, onSettle) {
    // Fire parked leaves in seeded-random order. Real I/O runs one at a
    // time so its latency can never reorder events, but settles are batched
    // back to back in one tick so continuations of different leaves
    // interleave within the same microtask flush.
    let iterations = 0;
    while (!doneRef.done || program.pending.length > 0) {
      if (iterations++ > 10000) {
        throw new Error('driver did not converge');
      }
      if (program.pending.length > 0) {
        const batchSize = Math.min(
          program.pending.length,
          rand.intBetween(1, 3),
        );
        const batch = [];
        for (let i = 0; i < batchSize; i++) {
          const idx = rand.intBetween(0, program.pending.length - 1);
          batch.push(program.pending.splice(idx, 1)[0]);
        }
        // Drain everything the render scheduled (including chains of it)
        // before touching real I/O, so that nothing is pending while the
        // I/O runs and its real-world latency can't reorder anything.
        for (let i = 0; i < 6; i++) {
          await new Promise(resolve => {
            setTimeout(resolve, 0);
          });
        }
        for (let i = 0; i < batch.length; i++) {
          await batch[i].io();
        }
        for (let i = 0; i < batch.length; i++) {
          batch[i].settle();
          onSettle();
        }
      }
      const hops = rand.intBetween(1, 3);
      for (let i = 0; i < hops; i++) {
        switch (rand.intBetween(0, 2)) {
          case 0:
            await null;
            break;
          case 1:
            await new Promise(resolve => setImmediate(resolve));
            break;
          default:
            await new Promise(resolve => process.nextTick(resolve));
            break;
        }
      }
    }
  }

  // Collect debug info from the resolved client tree, in traversal order.
  function collectDebugInfo(root) {
    const out = [];
    const visited = new Set();
    const queue = [root];
    let budget = 2000;
    while (queue.length > 0) {
      if (budget-- === 0) {
        throw new Error('debug info walker exceeded its budget');
      }
      const value = queue.shift();
      if (value === null || typeof value !== 'object' || visited.has(value)) {
        continue;
      }
      visited.add(value);
      if (value._debugInfo) {
        out.push(value._debugInfo);
      }
      if (Array.isArray(value)) {
        value.forEach(entry => queue.push(entry));
      } else if (value.$$typeof === Symbol.for('react.transitional.element')) {
        queue.push(value.props.children);
        if (value.props.data != null) {
          queue.push(value.props.data);
        }
        if (value.props.slot != null) {
          queue.push(value.props.slot);
        }
      } else if (value.$$typeof === Symbol.for('react.lazy')) {
        // An aborted or errored subtree stays a lazy wrapper around its
        // chunk.
        queue.push(value._payload);
      } else if (typeof value.then === 'function') {
        if (value.status === 'fulfilled') {
          queue.push(value.value);
        }
      }
    }
    return out;
  }

  function summarize(value) {
    if (value === null || value === undefined) {
      return String(value);
    }
    if (typeof value === 'string') {
      return value.length > 40 ? value.slice(0, 40) + '…' : value;
    }
    if (typeof value === 'number') {
      return String(value);
    }
    if (Array.isArray(value)) {
      return '[' + value.map(summarize).join(',') + ']';
    }
    if (value.$$typeof === Symbol.for('react.transitional.element')) {
      const name =
        typeof value.type === 'string'
          ? value.type
          : value.type.name || 'anonymous';
      return (
        '<' + name + '>' + summarize(value.props.children) + '</' + name + '>'
      );
    }
    if (value.$$typeof === Symbol.for('react.lazy')) {
      const payload = value._payload;
      if (payload.status === 'fulfilled') {
        return 'lazy(' + summarize(payload.value) + ')';
      }
      if (payload.status === 'rejected') {
        return 'lazy(rejected: ' + payload.reason.message + ')';
      }
      return 'lazy(' + payload.status + ')';
    }
    if (typeof value.then === 'function') {
      if (value.status === 'fulfilled') {
        return 'promise(' + summarize(value.value) + ')';
      }
      if (value.status === 'rejected') {
        return 'promise(rejected: ' + value.reason.message + ')';
      }
      return 'promise(' + value.status + ')';
    }
    return JSON.stringify(value);
  }

  async function runSeed(seed) {
    const rand = Random.create('flight-fuzz-' + seed);
    const program = createProgram(rand);
    const componentRoots = new Map();
    const componentCreators = new Map();
    const {makeComponent} = buildComponents(
      rand,
      program,
      componentRoots,
      componentCreators,
    );

    // Resolve a seeded subset of the module-scope data before rendering
    // starts, so some awaits hit promises that settled before the request
    // existed.
    const preResolve = rand.intBetween(0, program.pool.length - 1);
    for (let i = 0; i < preResolve; i++) {
      const entry = program.pending.splice(0, 1)[0];
      await entry.io();
      entry.settle();
      await program.pool[i];
    }

    // Most seeds render one root; some render two interleaved. Some abort
    // the first render partway through.
    const concurrentRenders = rand.intBetween(0, 3) === 0 ? 2 : 1;
    const abortAfter = rand.intBetween(0, 5) === 0 ? rand.intBetween(2, 8) : -1;
    const roots = [];
    for (let i = 0; i < concurrentRenders; i++) {
      const Root = makeComponent(0, {
        canThrow: abortAfter === -1,
        rootIndex: i,
      });
      // Root elements are created outside any component.
      componentCreators.set(Root.name, null);
      roots.push(Root);
    }

    const results = [];
    const readables = [];
    const aborts = [];
    roots.forEach(Root => {
      const stream = ReactServerDOMServer.renderToPipeableStream(
        ReactServer.createElement(Root, null),
        {},
        {
          filterStackFrame,
          onError(error) {
            // Component bugs and aborts are part of the workload.
            return 'digest:' + error.message;
          },
        },
      );
      aborts.push(reason => stream.abort(reason));
      const readable = new Stream.PassThrough(streamOptions);
      results.push(
        ReactServerDOMClient.createFromNodeStream(
          readable,
          {
            moduleMap: {},
            moduleLoading: {},
          },
          // Turns on the client debug handling that feeds the performance
          // track.
          {replayConsoleLogs: true},
        ),
      );
      readables.push(readable);
      stream.pipe(readable);
    });

    const doneRef = {done: false};
    const allDone = Promise.all([
      // Roots may reject (aborts, a throwing root component); the driver
      // still needs to terminate so the rejection can surface as a value.
      Promise.all(
        results.map(result =>
          result.then(
            v => v,
            x => x,
          ),
        ),
      ),
      Promise.all(readables.map(finishLoadingStream)),
    ]).then(() => {
      doneRef.done = true;
    });

    let settles = 0;
    await drive(rand, program, doneRef, function onSettle() {
      settles++;
      if (settles === abortAfter) {
        aborts[0](new Error('fuzz aborted'));
      }
    });
    await allDone;

    const output = [];
    for (let i = 0; i < results.length; i++) {
      let value;
      try {
        value = await results[i];
      } catch (x) {
        output.push({value: 'root rejected: ' + x.message, debugInfo: []});
        continue;
      }
      output.push({
        value: summarize(value),
        debugInfo: collectDebugInfo(value),
      });
    }
    return output;
  }

  // FUZZ_TEST_SEED reruns or explores any seed: every check is computed from
  // what the seed's program actually did, so novel seeds verify like
  // committed ones.
  const seeds = [];
  if (ONLY_SEED !== null) {
    seeds.push(ONLY_SEED);
  } else {
    for (let seed = 0; seed < NUM_SEEDS; seed++) {
      seeds.push(seed);
    }
  }
  seeds.forEach(seed => {
    it(`produces stable async debug info (seed ${seed})`, async () => {
      // Render unconditionally so we catch any production crashes
      await runSeed(seed);
    });
  });
});
