import type {
  CompiledExecutionPlan,
  CompiledJobEdge,
  CompiledJobNode,
  ExecutionPlan,
  JobEdge,
} from '../types';

import compileFunction from '../modules/compile-function';
import { conditionContext, Context } from './context';

const compileEdges = (
  from: string,
  edges: string | Record<string, boolean | JobEdge>,
  context: Context
) => {
  if (typeof edges === 'string') {
    return { [edges]: true };
  }
  const errs = [];

  const result = {} as Record<string, boolean | CompiledJobEdge>;
  for (const edgeId in edges) {
    try {
      const edge = edges[edgeId];
      if (typeof edge === 'boolean') {
        result[edgeId] = edge;
      } else if (typeof edge === 'string') {
        result[edgeId] = { condition: compileFunction(edge, context) };
      } else {
        const newEdge = {
          ...edge,
        };
        if (typeof edge.condition === 'string') {
          (newEdge as any).condition = compileFunction(edge.condition, context);
        }
        result[edgeId] = newEdge as CompiledJobEdge;
      }
    } catch (e: any) {
      errs.push(
        new Error(
          `Failed to compile edge condition ${from}->${edgeId} (${e.message})`
        )
      );
    }
  }

  if (errs.length) {
    throw errs;
  }

  return result;
};

// find the upstream job for a given job
// Inefficient but fine for now (note that validation does something similar)
// Note that right now we only support one upstream job
const findUpstream = (plan: ExecutionPlan, id: string) => {
  for (const job of plan.jobs) {
    if (job.next)
      if (typeof job.next === 'string') {
        if (job.next === id) {
          return job.next;
        }
      } else if (job.next[id]) {
        return job.id;
      }
  }
};

export default (plan: ExecutionPlan) => {
  let autoJobId = 0;
  const generateJobId = () => `job-${++autoJobId}`;
  const context = conditionContext();

  const errs: Error[] = [];

  const trapErrors = (fn: Function) => {
    try {
      fn();
    } catch (e: any | any[]) {
      if (Array.isArray(e)) {
        // If we've been thrown an array of errors, just add them to the collection
        errs.push(...e);
      } else {
        // Otherwise something else went wrong so we'll panic I guess
        throw e;
      }
    }
  };

  // ensure ids before we start
  for (const job of plan.jobs) {
    if (!job.id) {
      job.id = generateJobId();
    }
  }

  const newPlan = {
    jobs: {},
    start: plan.start,
  } as Pick<CompiledExecutionPlan, 'jobs' | 'start'>;

  for (const job of plan.jobs) {
    const jobId = job.id!;
    if (!newPlan.start) {
      // Default the start job to the first
      newPlan.start = jobId;
    }
    const newJob: CompiledJobNode = {
      id: jobId,
      expression: job.expression, // TODO we should compile this here
    };
    if (job.state) {
      newJob.state = job.state;
    }
    if (job.configuration) {
      newJob.configuration = job.configuration;
    }
    if (job.next) {
      trapErrors(() => {
        newJob.next = compileEdges(jobId, job.next!, context);
      });
    }
    newJob.previous = findUpstream(plan, jobId);
    newPlan.jobs[jobId] = newJob;
  }

  if (errs.length) {
    const e = new Error('compilation error');
    e.message = errs.map(({ message }) => message).join('\n\n');
    throw e;
  }

  return newPlan as CompiledExecutionPlan;
};