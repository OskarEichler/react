/**
 * @jest-environment node
 */
'use strict';

// The async debug info tracking retains the history behind every tracked
// operation only for as long as some request could still emit it. History
// expires after enough newer operations have been tracked, so these tests
// churn through awaits to age it.

import {patchSetImmediate} from '../../../../scripts/jest/patchSetImmediate';

let React;
let ReactServerDOMServer;
let ReactServerDOMClient;
let Stream;
let getDebugInfo;

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

describe('ReactFlightAsyncDebugInfoExpiration', () => {
  beforeEach(() => {
    jest.resetModules();
    jest.useRealTimers();
    patchSetImmediate();
    global.console = require('console');

    jest.mock('react', () => require('react/react.react-server'));
    jest.mock('react-server-dom-webpack/server', () =>
      jest.requireActual('react-server-dom-webpack/server.node'),
    );
    require('react');
    ReactServerDOMServer = require('react-server-dom-webpack/server');

    jest.resetModules();
    jest.useRealTimers();
    patchSetImmediate();

    __unmockReact();
    jest.unmock('react-server-dom-webpack/server');
    jest.mock('react-server-dom-webpack/client', () =>
      jest.requireActual('react-server-dom-webpack/client.node'),
    );

    React = require('react');
    ReactServerDOMClient = require('react-server-dom-webpack/client');
    Stream = require('stream');

    getDebugInfo = require('internal-test-utils').getDebugInfo.bind(null, {
      ignoreProps: true,
      useFixedTime: true,
    });
  });

  function delay(timeout) {
    return new Promise(resolve => {
      setTimeout(resolve, timeout);
    });
  }

  function finishLoadingStream(readable) {
    return new Promise(resolve => {
      if (readable.readableEnded) {
        resolve();
      } else {
        readable.on('end', () => resolve());
      }
    });
  }

  // Enough tracked operations to age everything created before them past the
  // retention budget. Every Promise is tracked at creation and the expiration
  // sweeps run every 128 tracked operations, so nothing needs to be awaited.
  function churnPastRetention() {
    for (let i = 0; i < 20; i++) {
      for (let j = 0; j < 1500; j++) {
        Promise.resolve(j);
      }
    }
  }

  it('expires history that resolved long before a backdated request', async () => {
    // A request initializes the tracking; after the stream closes, nothing
    // pins history anymore.
    async function Init() {
      await delay(1);
      return 'ok';
    }
    const initStream = ReactServerDOMServer.renderToPipeableStream(
      <Init />,
      {},
      {filterStackFrame},
    );
    const initReadable = new Stream.PassThrough(streamOptions);
    const initResult = ReactServerDOMClient.createFromNodeStream(initReadable, {
      moduleMap: {},
      moduleLoading: {},
    });
    initStream.pipe(initReadable);
    expect(await initResult).toBe('ok');
    await finishLoadingStream(initReadable);

    // Data fetched outside any request and kept alive, like a module-level
    // cache. A request backdated to before the fetch may still consume its
    // history for as long as the retention window allows.
    async function fetchCachedData() {
      await delay(5);
      return 'hello';
    }
    const cachedStartTime =
      // $FlowFixMe[prop-missing]
      performance.timeOrigin + performance.now();
    const cachedData = fetchCachedData();
    await cachedData;

    churnPastRetention();

    // This request claims it started before the fetch, but arrived long
    // after it, so the fetch's history must be gone.
    async function Component() {
      return 'data:' + (await cachedData);
    }
    const stream = ReactServerDOMServer.renderToPipeableStream(
      <Component />,
      {},
      {
        filterStackFrame,
        startTime: cachedStartTime,
      },
    );
    const readable = new Stream.PassThrough(streamOptions);
    const result = ReactServerDOMClient.createFromNodeStream(readable, {
      moduleMap: {},
      moduleLoading: {},
    });
    stream.pipe(readable);
    expect(await result).toBe('data:hello');
    await finishLoadingStream(readable);

    if (
      __DEV__ &&
      gate(
        flags =>
          flags.enableComponentPerformanceTrack && flags.enableAsyncDebugInfo,
      )
    ) {
      expect(getDebugInfo(result).filter(entry => entry.awaited)).toEqual([]);
    }
  });
});
