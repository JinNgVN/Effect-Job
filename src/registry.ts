// Handler registry: maps job names to the Effect handlers workers can execute.

import { Context, Data, Effect, Layer, Option } from "effect";

import type { JobDefinition, JobRunInput } from "./job";
import type { JobName } from "./model";

export type JobRun = (
    input: JobRunInput<any, string>,
) => Effect.Effect<any, any, any>;

export interface RegisteredJob {
    readonly job: JobDefinition.Any;
    readonly run: JobRun;
}

export class DuplicateJobHandlerError extends Data.TaggedError(
    "DuplicateJobHandlerError",
)<{
    readonly jobName: JobName;
}> {}

export class JobRegistry extends Context.Service<
    JobRegistry,
    {
        readonly register: (
            job: JobDefinition.Any,
            run: JobRun,
        ) => Effect.Effect<void, DuplicateJobHandlerError>;
        readonly get: (
            name: JobName,
        ) => Effect.Effect<Option.Option<RegisteredJob>>;
        readonly list: Effect.Effect<ReadonlyArray<RegisteredJob>>;
    }
>()("effect-job/JobRegistry") {}

export const JobRegistryMemory = Layer.effect(JobRegistry)(
    Effect.sync(() => {
        const handlers = new Map<JobName, RegisteredJob>();

        return {
            register: (job, run) =>
                Effect.gen(function* () {
                    const name = job.name;

                    if (handlers.has(name)) {
                        return yield* new DuplicateJobHandlerError({
                            jobName: name,
                        });
                    }

                    handlers.set(name, { job, run });
                }),
            get: (name) =>
                Effect.sync(() => Option.fromNullishOr(handlers.get(name))),
            list: Effect.sync(() => Array.from(handlers.values())),
        };
    }),
);
