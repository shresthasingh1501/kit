#!/usr/bin/env node
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import createLogger, { LogLevel } from '@openfn/logger';

import createRTE from '@openfn/engine-multi';
import createMockRTE from './mock/runtime-engine';
import createWorker, { ServerOptions } from './server';

type Args = {
  _: string[];
  port?: number;
  lightning?: string;
  repoDir?: string;
  secret?: string;
  loop?: boolean;
  log: LogLevel;
  lightningPublicKey?: string;
  mock: boolean;
  backoff: string;
  capacity?: number;
  runMemory?: number;
  statePropsToRemove?: string[];
  maxRunDurationSeconds: number;
};

const {
  WORKER_BACKOFF,
  WORKER_CAPACITY,
  WORKER_LIGHTNING_PUBLIC_KEY,
  WORKER_LIGHTNING_SERVICE_URL,
  WORKER_LOG_LEVEL,
  WORKER_MAX_RUN_DURATION_SECONDS,
  WORKER_MAX_RUN_MEMORY_MB,
  WORKER_PORT,
  WORKER_REPO_DIR,
  WORKER_SECRET,
  WORKER_STATE_PROPS_TO_REMOVE,
} = process.env;

const setArg = (cliValue: any, envValue: any, defaultValue: any) => {
  return cliValue !== undefined ? cliValue : envValue !== undefined ? envValue : defaultValue;
};

const parser = yargs(hideBin(process.argv))
  .command('server', 'Start a ws-worker server')
  .option('port', {
    alias: 'p',
    description: 'Port to run the server on.',
    type: 'number',
  })
  .option('lightning', {
    alias: ['l', 'lightning-service-url'],
    description: 'Base url to Lightning websocket endpoint, eg, ws://localhost:4000/worker. Set to "mock" to use the default mock server.',
    type: 'string',
  })
  .option('repo-dir', {
    alias: 'd',
    description: 'Path to the runtime repo (where modules will be installed).',
    type: 'string',
  })
  .option('secret', {
    alias: 's',
    description: 'Worker secret.',
    type: 'string',

  })
  .option('lightning-public-key', {
    description: 'Base64-encoded public key. Used to verify run tokens.',
    type: 'string',
  })
  .option('log', {
    description: 'Set the log level for stdout (default to info, set to debug for verbose output).',
    type: 'string',
  })
  .option('loop', {
    description: 'Disable the claims loop',
    type: 'boolean',
  })
  .option('mock', {
    description: 'Use a mock runtime engine',
    type: 'boolean',
  })
  .option('backoff', {
    description: 'Claim backoff rules: min/max (in seconds).',
    type: 'string',
  })
  .option('capacity', {
    description: 'Max concurrent workers.',
    type: 'number',
  })
  .option('state-props-to-remove', {
    description: 'A list of properties to remove from the final state returned by a job.',
    type: 'array',
  })
  .option('run-memory', {
    description: 'Maximum memory allocated to a single run, in mb.',
    type: 'number',
  })
  .option('max-run-duration-seconds', {
    alias: 't',
    description: 'Default run timeout for the server, in seconds.',
    type: 'number',
  })
  .parse() as Args;

const args = {
  ...parser,
  port: setArg(parser.port, WORKER_PORT ? parseInt(WORKER_PORT) : undefined, 2222),
  lightning: setArg(parser.lightning, WORKER_LIGHTNING_SERVICE_URL, 'ws://localhost:4000/worker'),
  repoDir: setArg(parser.repoDir, WORKER_REPO_DIR, undefined),
  secret: setArg(parser.secret, WORKER_SECRET, undefined),
  lightningPublicKey: setArg(parser.lightningPublicKey, WORKER_LIGHTNING_PUBLIC_KEY, undefined),
  log: setArg(parser.log, WORKER_LOG_LEVEL, 'debug') as LogLevel,
  loop: parser.loop ?? true,
  mock: parser.mock ?? false,
  backoff: setArg(parser.backoff, WORKER_BACKOFF, '1/10'),
  capacity: setArg(parser.capacity, WORKER_CAPACITY ? parseInt(WORKER_CAPACITY) : undefined, 5),
  statePropsToRemove: setArg(parser.statePropsToRemove, WORKER_STATE_PROPS_TO_REMOVE ? WORKER_STATE_PROPS_TO_REMOVE.split(',') : undefined, ['configuration', 'response']),
  runMemory: setArg(parser.runMemory, WORKER_MAX_RUN_MEMORY_MB ? parseInt(WORKER_MAX_RUN_MEMORY_MB) : undefined, 500),
  maxRunDurationSeconds: setArg(parser.maxRunDurationSeconds, WORKER_MAX_RUN_DURATION_SECONDS ? parseInt(WORKER_MAX_RUN_DURATION_SECONDS) : undefined, 300),
};

const logger = createLogger('SRV', { level: args.log });

if (args.lightning === 'mock') {
  args.lightning = 'ws://localhost:8888/worker';
  if (!args.secret) {
    args.secret = 'abdefg';
  }
} else if (!args.secret) {
  logger.error('WORKER_SECRET is not set');
  process.exit(1);
}

const [minBackoff, maxBackoff] = args.backoff.split('/').map((n: string) => parseInt(n, 10) * 1000);

function engineReady(engine: any) {
  logger.debug('Creating worker server...');

  const workerOptions: ServerOptions = {
    port: args.port,
    lightning: args.lightning,
    logger,
    secret: args.secret,
    noLoop: !args.loop,
    backoff: {
      min: minBackoff,
      max: maxBackoff,
    },
    maxWorkflows: args.capacity,
  };

  if (args.lightningPublicKey) {
    logger.info('Lightning public key found: run tokens from Lightning will be verified by this worker');
    workerOptions.runPublicKey = Buffer.from(args.lightningPublicKey, 'base64').toString();
  }

  const { logger: _l, secret: _s, runPublicKey, ...humanOptions } = workerOptions;
  logger.debug('Worker options:', humanOptions);

  createWorker(engine, workerOptions);
}

if (args.mock) {
  createMockRTE().then((engine) => {
    logger.debug('Mock engine created');
    engineReady(engine);
  });
} else {
  const engineOptions = {
    repoDir: args.repoDir,
    memoryLimitMb: args.runMemory,
    maxWorkers: args.capacity,
    statePropsToRemove: args.statePropsToRemove,
    runTimeoutMs: args.maxRunDurationSeconds * 1000,
  };
  logger.debug('Creating runtime engine...');
  logger.debug('Engine options:', engineOptions);

  createRTE(engineOptions).then((engine) => {
    logger.debug('Engine created!');
    engineReady(engine);
  });
}
