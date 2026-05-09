// Plugin contracts and lifecycle events.

import { Effect, type Duration, type Layer } from "effect";

import type { JobRecord } from "./model";

export interface JobEnqueuedEvent {
    readonly job: JobRecord;
}

export interface JobStartedEvent {
    readonly job: JobRecord;
}

export interface JobCompletedEvent {
    readonly job: JobRecord;
}

export interface JobFailedEvent {
    readonly job: JobRecord;
    readonly error: unknown;
    readonly retryAt: Date;
}

export interface JobDiscardedEvent {
    readonly job: JobRecord;
    readonly error: unknown;
}

export interface JobCancelledEvent {
    readonly job: JobRecord;
    readonly reason: unknown;
}

export interface JobSnoozedEvent {
    readonly job: JobRecord;
    readonly runAt: Date;
}

export interface WorkerStartedEvent {
    readonly workerId: string;
    readonly queues: ReadonlyArray<{
        readonly queue?: string;
        readonly concurrency: number;
        readonly pollInterval: Duration.Input;
    }>;
}

export interface EffectJobPlugin {
    readonly name: string;
    readonly layer?: Layer.Layer<any, any, any>;
    readonly onWorkerStarted?: (
        event: WorkerStartedEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobEnqueued?: (
        event: JobEnqueuedEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobStarted?: (
        event: JobStartedEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobCompleted?: (
        event: JobCompletedEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobFailed?: (
        event: JobFailedEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobDiscarded?: (
        event: JobDiscardedEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobCancelled?: (
        event: JobCancelledEvent,
    ) => Effect.Effect<void, unknown, any>;
    readonly onJobSnoozed?: (
        event: JobSnoozedEvent,
    ) => Effect.Effect<void, unknown, any>;
}

export const runPluginHook = (
    plugin: EffectJobPlugin,
    hook: Effect.Effect<void, unknown, any> | undefined,
) =>
    hook === undefined
        ? Effect.void
        : hook.pipe(
              Effect.catchCause((cause) =>
                  Effect.logError(`Plugin ${plugin.name} hook failed`, cause),
              ),
          );

export const runPluginHooks = (
    plugins: ReadonlyArray<EffectJobPlugin>,
    hook: (
        plugin: EffectJobPlugin,
    ) => Effect.Effect<void, unknown, any> | undefined,
) =>
    Effect.forEach(plugins, (plugin) => runPluginHook(plugin, hook(plugin)), {
        concurrency: "unbounded",
        discard: true,
    });
