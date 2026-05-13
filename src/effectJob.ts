// Configured runtime API for Effect Job.
//
// This file owns the public `effectJob(...)` factory and the `Jobs` object it
// returns. Lower-level job definition and storage behavior live in `job.ts` and
// `engine.ts`; this module wires those pieces into one configured runtime.

import { Data, Effect, Layer, Option, Schema } from "effect";

import {
    insertJobCommand,
    Job,
    JobCancelError,
    JobDiscardError,
    type JobCommand,
    type JobDefinition,
    type JobDefinitionOptions,
    type JobHandle,
    JobRuntime,
    JobSnoozeError,
    makeJobDefinition,
    resolveJobCommand,
    type InsertError,
    type ResolvedJobCommand,
} from "./job";
import type { JobId, QueueName } from "./model";
import { JobPlugins, runPluginHooks } from "./plugin";
import { JobRegistry } from "./registry";
import { Worker, type WorkerRunOptions } from "./worker";

export type EffectJobQueueOptions = WorkerRunOptions["queues"][string];
type a = {
    queue: Record<string, { concurrency: number; limit: number }>;
};

type options = a["queue"][string];
export type EffectJobQueues = Readonly<Record<string, EffectJobQueueOptions>>;

export type EffectJobQueueNames<Queues extends EffectJobQueues> = Extract<
    keyof Queues,
    string
>;

export interface EffectJobConfig<Queues extends EffectJobQueues> {
    readonly queues: Queues;
    readonly pollInterval?: WorkerRunOptions["pollInterval"];
    readonly shutdownGracePeriod?: WorkerRunOptions["shutdownGracePeriod"];
}

export interface QueueInfo {
    readonly declared: ReadonlyArray<QueueName>;
    readonly workers: ReadonlyArray<QueueName>;
}

export interface DynamicQueueConfig {
    readonly name: QueueName | { readonly name: QueueName };
    readonly concurrency?: number;
    readonly globalConcurrency?: number;
}

export interface ScheduleUpsertConfig<
    Job extends JobDefinition.Any = JobDefinition.Any,
> {
    readonly id: string;
    readonly cron: string;
    readonly timezone?: string;
    readonly job: Job;
    readonly payload: unknown;
    readonly enabled?: boolean;
}

export interface JobMiddleware {
    readonly name: string;
    readonly around?: (
        job: unknown,
        run: Effect.Effect<unknown, unknown, any>,
    ) => Effect.Effect<unknown, unknown, any>;
}

export interface JobExtension {
    readonly name: string;
    readonly beforeInsert?: (context: {
        readonly command: JobCommand;
    }) => Effect.Effect<JobCommand, unknown, any>;
    readonly classifyError?: (context: {
        readonly error: unknown;
        readonly fallback: (error: unknown) => "retry" | "discard" | "cancel";
    }) => "retry" | "discard" | "cancel";
}

export class JobFeatureNotImplementedError extends Data.TaggedError(
    "JobFeatureNotImplementedError",
)<{
    readonly feature: string;
}> {}

const notImplemented = <A = never>(feature: string) =>
    Effect.fail(
        new JobFeatureNotImplementedError({ feature }),
    ) as Effect.Effect<A, JobFeatureNotImplementedError>;

const uniqueSorted = (values: Iterable<QueueName | undefined>) =>
    Array.from(
        new Set(Array.from(values).filter(Boolean) as Array<QueueName>),
    ).sort();

const configuredWorkerQueues = (
    queues: WorkerRunOptions["queues"] | undefined,
): ReadonlyArray<QueueName> =>
    queues === undefined ? [] : uniqueSorted(Object.keys(queues));

const staticJobQueue = (job: JobDefinition.Any): QueueName | undefined =>
    typeof job.queue === "string" ? job.queue : undefined;

// Insert through the command path, then emit lifecycle hooks for observers such
// as logging, metrics, dashboard events, pruners, and rescuers.
const insertWithHooks = <
    const Name extends string,
    ResultSchema extends Schema.Top,
>(
    command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
) =>
    Effect.gen(function* () {
        const plugins = yield* JobPlugins;
        const handle = yield* insertJobCommand(command);
        const record = yield* Job.find(handle.id);

        if (Option.isSome(record)) {
            yield* runPluginHooks(plugins, (plugin) =>
                plugin.onJobEnqueued?.({ job: record.value }),
            );
        }

        return handle;
    });

// Worker options come from the configured runtime, with per-worker overrides for
// tests or specialized worker processes.
const workerOptions = (
    config: EffectJobConfig<EffectJobQueues>,
    options?: WorkerRunOptions,
): WorkerRunOptions => ({
    queues: options?.queues ?? (config.queues as WorkerRunOptions["queues"]),
    pollInterval: options?.pollInterval ?? config.pollInterval,
    workerId: options?.workerId,
    shutdownGracePeriod:
        options?.shutdownGracePeriod ?? config.shutdownGracePeriod,
});

// Queue info combines queues declared in runtime config with static queues from
// registered job definitions. Dynamic queues will need a storage-backed source.
const makeQueueInfo = (
    config: EffectJobConfig<EffectJobQueues>,
): Effect.Effect<QueueInfo, never, JobRegistry> =>
    Effect.gen(function* () {
        const registry = yield* JobRegistry;
        const registered = yield* registry.list;
        const declared = uniqueSorted([
            ...Object.keys(config.queues ?? {}),
            ...registered.map(({ job }) => staticJobQueue(job)),
        ]);

        return {
            declared,
            workers: configuredWorkerQueues(config.queues),
        };
    });

export interface JobTestRuntime {
    readonly mode: "manual" | "inline";
    readonly expectEnqueued: <Job extends JobDefinition.Any>(
        job: Job,
        expectation?: { readonly payload?: unknown },
    ) => Effect.Effect<void, Error, any>;
    readonly drainQueue: (
        queue: QueueName,
        options?: {
            readonly mode?: "once" | "until-empty";
            readonly maxJobs?: number;
        },
    ) => Effect.Effect<void, JobFeatureNotImplementedError>;
}

export interface EffectJobRuntime<
    Queues extends EffectJobQueues = EffectJobQueues,
> {
    readonly define: <
        const Name extends string,
        PayloadSchema extends Schema.Top,
        ResultSchema extends Schema.Top = typeof Schema.Unknown,
    >(
        options: JobDefinitionOptions<
            Name,
            PayloadSchema,
            ResultSchema,
            EffectJobQueueNames<Queues>
        >,
    ) => JobDefinition<
        Name,
        PayloadSchema,
        ResultSchema,
        EffectJobQueueNames<Queues>
    >;
    readonly insert: <
        const Name extends string,
        ResultSchema extends Schema.Top,
    >(
        command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
    ) => Effect.Effect<JobHandle<Name, ResultSchema>, InsertError, any>;
    readonly insertMany: <
        const Name extends string,
        ResultSchema extends Schema.Top,
    >(
        commands: ReadonlyArray<
            JobCommand<Name, unknown, ResultSchema["Type"], any>
        >,
    ) => Effect.Effect<
        ReadonlyArray<JobHandle<Name, ResultSchema>>,
        InsertError,
        any
    >;
    readonly resolve: <
        const Name extends string,
        ResultSchema extends Schema.Top,
    >(
        command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
    ) => Effect.Effect<
        ResolvedJobCommand<Name, ResultSchema>,
        InsertError,
        any
    >;
    readonly run: Effect.Effect<never, never, JobRegistry | any>;
    readonly worker: (
        options?: WorkerRunOptions,
    ) => Effect.Effect<never, never, JobRegistry | any>;
    readonly queues: {
        readonly info: Effect.Effect<QueueInfo, never, JobRegistry>;
        readonly create: (
            config: DynamicQueueConfig,
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
        readonly pause: (
            queue: QueueName,
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
        readonly resume: (
            queue: QueueName,
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
        readonly scale: (
            queue: QueueName,
            options: { readonly concurrency: number },
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
        readonly scaleGlobal: (
            queue: QueueName,
            options: { readonly concurrency: number },
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
    };
    readonly jobs: {
        readonly retry: (id: JobId) => Effect.Effect<void, any, any>;
        readonly cancel: (
            id: JobId,
            options?: { readonly reason?: unknown },
        ) => Effect.Effect<void, any, any>;
        readonly cancelWhere: (query: {
            readonly name?: string;
            readonly state?: ReadonlyArray<string>;
        }) => Effect.Effect<number, JobFeatureNotImplementedError>;
        readonly snooze: (
            id: JobId,
            options: { readonly until: Date },
        ) => Effect.Effect<void, any, any>;
    };
    readonly schedules: {
        readonly upsert: (
            config: ScheduleUpsertConfig,
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
        readonly delete: (
            id: string,
        ) => Effect.Effect<void, JobFeatureNotImplementedError>;
        readonly list: Effect.Effect<
            ReadonlyArray<ScheduleUpsertConfig>,
            JobFeatureNotImplementedError
        >;
    };
    readonly dashboard: {
        readonly search: (
            query: unknown,
        ) => Effect.Effect<
            ReadonlyArray<unknown>,
            JobFeatureNotImplementedError
        >;
        readonly timeline: (
            id: JobId,
        ) => Effect.Effect<
            ReadonlyArray<unknown>,
            JobFeatureNotImplementedError
        >;
        readonly usage: (
            query?: unknown,
        ) => Effect.Effect<unknown, JobFeatureNotImplementedError>;
        readonly stream: Effect.Effect<never, JobFeatureNotImplementedError>;
    };
    readonly middleware: {
        readonly make: (middleware: JobMiddleware) => JobMiddleware;
        readonly logging: () => JobMiddleware;
        readonly tracing: () => JobMiddleware;
    };
    readonly extension: (extension: JobExtension) => JobExtension;
    readonly use: (
        middleware: ReadonlyArray<JobMiddleware>,
    ) => Effect.Effect<void>;
    readonly test: (options: {
        readonly mode: "manual" | "inline";
    }) => JobTestRuntime;
    readonly cancel: (reason: unknown) => Effect.Effect<never, JobCancelError>;
    readonly discard: (
        reason: unknown,
    ) => Effect.Effect<never, JobDiscardError>;
    readonly snooze: (
        duration: Parameters<typeof Job.snooze>[0],
        reason?: unknown,
    ) => Effect.Effect<never, JobSnoozeError>;
}

// Provides the configured `JobRuntime` service required by `job.enqueue(...)`.
// Keep this explicit in application layers so all runtime services are supplied
// at the program boundary.
export const JobRuntimeLive = <Queues extends EffectJobQueues>(
    jobs: EffectJobRuntime<Queues>,
): Layer.Layer<JobRuntime> =>
    Layer.succeed(JobRuntime, {
        insert: jobs.insert,
        insertMany: jobs.insertMany,
        resolve: jobs.resolve,
    } as any);

// The Queues generic captures queue names from config so `Jobs.define(...)`
// only accepts queues explicitly configured on this runtime.
export const effectJob = <Queues extends EffectJobQueues>(
    config: EffectJobConfig<Queues>,
): EffectJobRuntime<Queues> => {
    return {
        define: (options) => makeJobDefinition(options),
        insert: (command) => insertWithHooks(command),
        insertMany: (commands) =>
            Effect.forEach(commands, (command) => insertWithHooks(command), {
                concurrency: 1,
            }),
        resolve: resolveJobCommand,
        run: Worker.run(workerOptions(config)),
        worker: (options) => Worker.run(workerOptions(config, options)),
        queues: {
            info: makeQueueInfo(config),
            create: () => notImplemented("queues.create"),
            pause: () => notImplemented("queues.pause"),
            resume: () => notImplemented("queues.resume"),
            scale: () => notImplemented("queues.scale"),
            scaleGlobal: () => notImplemented("queues.scaleGlobal"),
        },
        cancel: Job.cancel,
        discard: Job.discard,
        snooze: Job.snooze,
        jobs: {
            retry: (id) => Job.runNow(id),
            cancel: (id, options) => Job.cancelById(id, options?.reason),
            cancelWhere: () => notImplemented("jobs.cancelWhere"),
            snooze: (id, options) => Job.snoozeById(id, options),
        },
        schedules: {
            upsert: () => notImplemented("schedules.upsert"),
            delete: () => notImplemented("schedules.delete"),
            list: notImplemented("schedules.list"),
        },
        dashboard: {
            search: () => notImplemented("dashboard.search"),
            timeline: () => notImplemented("dashboard.timeline"),
            usage: () => notImplemented("dashboard.usage"),
            stream: notImplemented("dashboard.stream"),
        },
        middleware: {
            make: (middleware) => middleware,
            logging: () => ({ name: "logging" }),
            tracing: () => ({ name: "tracing" }),
        },
        extension: (extension) => extension,
        use: () => Effect.void,
        test: (options) => ({
            mode: options.mode,
            expectEnqueued: (job, expectation) =>
                Effect.gen(function* () {
                    const records = yield* Job.list({
                        queue: staticJobQueue(job),
                    });
                    const found = records.find(
                        (record) =>
                            record.name === job.name &&
                            (expectation?.payload === undefined ||
                                JSON.stringify(record.payload) ===
                                    JSON.stringify(expectation.payload)),
                    );

                    if (found === undefined) {
                        return yield* Effect.fail(
                            new Error(`Expected ${job.name} to be enqueued`),
                        );
                    }
                }),
            drainQueue: () => notImplemented("test.drainQueue"),
        }),
    };
};
