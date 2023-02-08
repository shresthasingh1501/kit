import { createNullLogger, Logger } from '../util/logger';
import compile from '../compile/compile';
import loadState from '../execute/load-state';
import execute from '../execute/execute';
import { SafeOpts } from '../commands';

const sillyMessage = 'Calculating the answer to life, the universe, and eveything...';

const testHandler = async (options: SafeOpts, logger: Logger) => {
  logger.log('Running test job...');

  // This is a bit weird but it'll actually work!
  options.jobPath = `const fn = () => state => { console.log('${sillyMessage}'); return state * 2; } ; fn()`;

  if (!options.stateStdin) {
    logger.debug('No state provided: use -S <number> to provide some state');
    options.stateStdin = '21';
  }

  const silentLogger = createNullLogger();

  const state = await loadState(options, silentLogger);
  const code = await compile(options, logger);
  logger.break();
  logger.info('Compiled job:', '\n', code); // TODO there's an ugly intend here
  logger.break();
  logger.info('Running job...');
  const result = await execute(code, state, options);
  logger.success(`Result: ${result}`);
  return result;
};

export default testHandler;
