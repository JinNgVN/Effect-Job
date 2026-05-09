// High-level configuration API for producer apps and worker processes.

import { Effect, Layer, ManagedRuntime, Option, Schema } from "effect";

import { JobEngine } from "./engine";
import {
    Job,
    type InsertError,
    type InsertManyEffect,
    type InsertJobEffect,
    type JobInsertInput,
    type JobHandle,
} from "./job";
import type {
    JobId,
    JobListOptions,
    JobPruneOptions,
    JobRecord,
    QueueName,
} from "./model";
import { runPluginHooks, type EffectJobPlugin } from "./plugin";
import { JobRegistry, JobRegistryMemory } from "./registry";
import { Worker, type WorkerRunOptions } from "./worker";

export interface EffectJobConfig {
    readonly database: Layer.Layer<JobEngine, any, any>;
    readonly handlers?: ReadonlyArray<Layer.Layer<JobRegistry, any, any>>;
    readonly layers?: ReadonlyArray<Layer.Layer<any, any, any>>;
    readonly plugins?: ReadonlyArray<EffectJobPlugin>;
    readonly queues?: WorkerRunOptions["queues"];
    readonly pollInterval?: WorkerRunOptions["pollInterval"];
    readonly workerId?: WorkerRunOptions["workerId"];
    readonly shutdownGracePeriod?: WorkerRunOptions["shutdownGracePeriod"];
}

export interface QueueInfo {
    readonly declared: ReadonlyArray<QueueName>;
    readonly workers: ReadonlyArray<QueueName>;
}

export interface ConfiguredEffectJob {
    readonly layer: Layer.Layer<JobEngine | JobRegistry, any, any>;
    readonly runtime: ManagedRuntime.ManagedRuntime<any, any>;
    readonly runPromise: <A, E, R>(effect: Effect.Effect<A, E, R>) => Promise<A>;
    readonly insert: <
        const Name extends string,
        ErrorSchema extends Schema.Top,
    >(
        prepared: JobInsertInput<Name, ErrorSchema>,
    ) => Promise<JobHandle<Name, ErrorSchema>>;
    readonly insertMany: <
        const Name extends string,
        ErrorSchema extends Schema.Top,
    >(
        prepared: ReadonlyArray<JobInsertInput<Name, ErrorSchema>>,
    ) => Promise<ReadonlyArray<JobHandle<Name, ErrorSchema>>>;
    readonly find: (
        id: JobId,
    ) => Promise<import("effect").Option.Option<JobRecord>>;
    readonly list: (
        options?: JobListOptions,
    ) => Promise<ReadonlyArray<JobRecord>>;
    readonly queues: () => Promise<QueueInfo>;
    readonly cancelById: (id: JobId, reason: unknown) => Promise<void>;
    readonly runNow: (id: JobId) => Promise<void>;
    readonly prune: (options: JobPruneOptions) => Promise<number>;
    readonly worker: (
        options?: WorkerRunOptions,
    ) => Effect.Effect<never, never, JobEngine | JobRegistry>;
    readonly runWorker: (options?: WorkerRunOptions) => Promise<never>;
    readonly dispose: () => Promise<void>;
}

const mergeLayers = (
    layers: ReadonlyArray<Layer.Layer<any, any, any>>,
): Layer.Layer<any, any, any> | undefined => {
    if (layers.length === 0) {
        return undefined;
    }

    return Layer.mergeAll(
        ...(layers as [
            Layer.Layer<any, any, any>,
            ...Array<Layer.Layer<any, any, any>>,
        ]),
    );
};

const uniqueSorted = (
    values: Iterable<QueueName | undefined>,
): ReadonlyArray<QueueName> =>
    Array.from(
        new Set(
            Array.from(values).filter(
                (value): value is QueueName => value !== undefined,
            ),
        ),
    ).sort();

const configuredWorkerQueues = (
    queues: WorkerRunOptions["queues"] | undefined,
): ReadonlyArray<QueueName> =>
    queues === undefined ? [] : uniqueSorted(Object.keys(queues));

export const effectJob = (config: EffectJobConfig): ConfiguredEffectJob => {
    const extraLayers = [
        ...(config.layers ?? []),
        ...(config.plugins ?? []).flatMap((plugin) =>
            plugin.layer === undefined ? [] : [plugin.layer],
        ),
    ];
    const plugins = config.plugins ?? [];
    const dependencies = mergeLayers([JobRegistryMemory, ...extraLayers]);
    const handlers =
        config.handlers === undefined || config.handlers.length === 0
            ? JobRegistryMemory
            : mergeLayers(config.handlers)?.pipe(
                  Layer.provide(dependencies ?? JobRegistryMemory),
              ) ?? JobRegistryMemory;
    const layer = Layer.mergeAll(
        config.database,
        dependencies ?? JobRegistryMemory,
        handlers,
    ) as Layer.Layer<JobEngine | JobRegistry, any, any>;
    const runtime = ManagedRuntime.make(
        layer as Layer.Layer<JobEngine | JobRegistry, any, never>,
    ) as ManagedRuntime.ManagedRuntime<any, any>;
    const workerOptions = (options?: WorkerRunOptions): WorkerRunOptions => ({
        queues: options?.queues ?? config.queues,
        pollInterval: options?.pollInterval ?? config.pollInterval,
        plugins: options?.plugins ?? plugins,
        workerId: options?.workerId ?? config.workerId,
        shutdownGracePeriod:
            options?.shutdownGracePeriod ?? config.shutdownGracePeriod,
    });
    const runEnqueuedHooks = (id: JobId) =>
        Effect.gen(function* () {
            const record = yield* Job.find(id);

            if (Option.isSome(record)) {
                yield* runPluginHooks(plugins, (plugin) =>
                    plugin.onJobEnqueued?.({ job: record.value }),
                );
            }
        });

    return {
        layer,
        runtime,
        runPromise: (effect) =>
            runtime.runPromise(effect as Effect.Effect<any, any, any>),
        insert: (prepared) => {
            const insertEffect = Job.insert(prepared) as InsertJobEffect<
                string,
                Schema.Top
            > as Effect.Effect<JobHandle<string, Schema.Top>, InsertError, any>;

            return runtime.runPromise(
                Effect.gen(function* () {
                    const handle = yield* insertEffect;

                    yield* runEnqueuedHooks(handle.id);

                    return handle;
                }),
            ) as Promise<any>;
        },
        insertMany: (prepared) => {
            const insertEffect = Job.insertMany(prepared) as InsertManyEffect<
                string,
                Schema.Top
            > as Effect.Effect<
                ReadonlyArray<JobHandle<string, Schema.Top>>,
                InsertError,
                any
            >;

            return runtime.runPromise(
                Effect.gen(function* () {
                    const handles = yield* insertEffect;

                    yield* Effect.forEach(
                        handles,
                        (handle) => runEnqueuedHooks(handle.id),
                        { discard: true },
                    );

                    return handles;
                }),
            ) as Promise<any>;
        },
        find: (id) => runtime.runPromise(Job.find(id)),
        list: (options) => runtime.runPromise(Job.list(options)),
        queues: () =>
            runtime.runPromise(
                Effect.gen(function* () {
                    const registry = yield* JobRegistry;
                    const registered = yield* registry.list;

                    return {
                        declared: uniqueSorted(
                            registered.map((registeredJob) =>
                                registeredJob.job.queue,
                            ),
                        ),
                        workers: configuredWorkerQueues(config.queues),
                    };
                }),
            ),
        cancelById: (id, reason) =>
            runtime.runPromise(Job.cancelById(id, reason)),
        runNow: (id) => runtime.runPromise(Job.runNow(id)),
        prune: (options) => runtime.runPromise(Job.prune(options)),
        worker: (options) => Worker.run(workerOptions(options)),
        runWorker: (options) =>
            runtime.runPromise(Worker.run(workerOptions(options))),
        dispose: () => runtime.dispose(),
    };
};
