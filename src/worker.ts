// Worker runtime: polls queues, executes registered handlers, and updates job state.

import { randomUUID } from "node:crypto";

import {
    Cause,
    Data,
    Duration,
    Effect,
    Exit,
    Fiber,
    Option,
    Ref,
    Schema,
} from "effect";

import { JobEngine } from "./engine";
import { JobCancelError, JobSnoozeError } from "./job";
import type { JobName, QueueName, WorkerId } from "./model";
import { JobNotifier } from "./notifier";
import { runPluginHooks, type EffectJobPlugin } from "./plugin";
import { JobRegistry } from "./registry";

export class MissingJobHandlerError extends Data.TaggedError(
    "MissingJobHandlerError",
)<{
    readonly jobName: JobName;
}> { }

export class JobTimeoutError extends Data.TaggedError("JobTimeoutError")<{
    readonly duration: Duration.Input;
}> { }

export interface WorkerQueueOptions {
    readonly concurrency?: number;
    readonly pollInterval?: Duration.Input;
}

export interface WorkerRunOptions {
    readonly queues?: Readonly<Record<QueueName, WorkerQueueOptions | number>>;
    readonly pollInterval?: Duration.Input;
    readonly plugins?: ReadonlyArray<EffectJobPlugin>;
    readonly workerId?: WorkerId;
    readonly shutdownGracePeriod?: Duration.Input;
}

interface NormalizedQueue {
    readonly queue?: QueueName;
    readonly concurrency: number;
    readonly pollInterval: Duration.Input;
}

const normalizeQueues = (
    options?: WorkerRunOptions,
): ReadonlyArray<NormalizedQueue> => {
    const defaultPollInterval = options?.pollInterval ?? "1 second";

    if (options?.queues === undefined) {
        return [
            {
                queue: undefined,
                concurrency: 1,
                pollInterval: defaultPollInterval,
            },
        ];
    }

    return Object.entries(options.queues).map(([queue, queueOptions]) => {
        if (typeof queueOptions === "number") {
            return {
                queue,
                concurrency: queueOptions,
                pollInterval: defaultPollInterval,
            };
        }

        return {
            queue,
            concurrency: queueOptions.concurrency ?? 1,
            pollInterval: queueOptions.pollInterval ?? defaultPollInterval,
        };
    });
};

export const Worker = {
    run: (
        options?: WorkerRunOptions,
    ): Effect.Effect<never, never, JobEngine | JobRegistry> =>
        Effect.scoped(
            Effect.gen(function* () {
                const engine = yield* JobEngine;
                const notifier = yield* JobNotifier;
                const registry = yield* JobRegistry;
                const queues = normalizeQueues(options);
                const plugins = options?.plugins ?? [];
                const workerId = options?.workerId ?? `worker-${randomUUID()}`;
                const shutdownGracePeriod =
                    options?.shutdownGracePeriod ?? "15 seconds";
                const activeJobs = yield* Ref.make(
                    new Set<Fiber.Fiber<void, never>>(),
                );
                const waitForActiveJobs = Effect.gen(function* () {
                    const initial = yield* Ref.get(activeJobs);

                    if (initial.size === 0) {
                        return;
                    }

                    const completed = yield* Fiber.joinAll(initial).pipe(
                        Effect.timeoutOption(shutdownGracePeriod),
                    );

                    if (Option.isNone(completed)) {
                        const stillRunning = yield* Ref.get(activeJobs);

                        yield* Fiber.interruptAll(stillRunning);
                    }
                });

                yield* runPluginHooks(plugins, (plugin) =>
                    plugin.onWorkerStarted?.({ queues, workerId }),
                );

                for (const queue of queues) {
                    for (let index = 0; index < queue.concurrency; index += 1) {
                        yield* Effect.forever(
                            Effect.gen(function* () {
                                // Claim one job from this queue. When none are ready, pause before polling again.
                                const record = yield* engine.claimNext({
                                    queue: queue.queue,
                                    workerId,
                                });

                                if (Option.isNone(record)) {
                                    // Notifications wake the worker quickly; polling remains the correctness fallback.
                                    yield* notifier
                                        .waitForInsert({ queue: queue.queue })
                                        .pipe(
                                            Effect.race(
                                                Effect.sleep(
                                                    queue.pollInterval,
                                                ),
                                            ),
                                            Effect.asVoid,
                                        );
                                    return;
                                }

                                const processJob = Effect.gen(function* () {
                                    yield* runPluginHooks(plugins, (plugin) =>
                                        plugin.onJobStarted?.({
                                            job: record.value,
                                        }),
                                    );

                                    // Find the handler registered by job.toLayer(...).
                                    const registeredJob = yield* registry.get(
                                        record.value.name,
                                    );

                                    if (Option.isNone(registeredJob)) {
                                        const error =
                                            new MissingJobHandlerError({
                                                jobName: record.value.name,
                                            });

                                        yield* engine.fail(
                                            record.value.id,
                                            error,
                                        );
                                        const failed = yield* engine.find(
                                            record.value.id,
                                        );

                                        yield* runPluginHooks(
                                            plugins,
                                            (plugin) =>
                                                Option.isSome(failed)
                                                    ? plugin.onJobFailed?.({
                                                        job: failed.value,
                                                        error,
                                                        retryAt:
                                                            failed.value
                                                                .runAt,
                                                    })
                                                    : undefined,
                                        );
                                        return;
                                    }

                                    // Decode the stored payload, run the handler, then update the job state.
                                    const runJob = Effect.gen(function* () {
                                        const payload =
                                            yield* Schema.decodeUnknownEffect(
                                                registeredJob.value.job
                                                    .payloadSchema,
                                            )(record.value.payload);

                                        yield* Effect.logInfo(
                                            `Working ${record.value.name}`,
                                        );
                                        yield* registeredJob.value.run(payload, {
                                            id: record.value.id,
                                            name: record.value.name,
                                            queue: record.value.queue,
                                            meta: record.value.meta,
                                            tags: record.value.tags,
                                            attempt: record.value.attempt,
                                            maxAttempts:
                                                record.value.maxAttempts,
                                            runAt: record.value.runAt,
                                            insertedAt:
                                                record.value.insertedAt,
                                            attemptedBy:
                                                record.value.attemptedBy,
                                        });
                                    });
                                    const timeout =
                                        registeredJob.value.job.timeout;
                                    const jobEffect =
                                        timeout === undefined
                                            ? runJob
                                            : Effect.timeoutOrElse(runJob, {
                                                duration: timeout,
                                                orElse: () =>
                                                    Effect.fail(
                                                        new JobTimeoutError({
                                                            duration: timeout,
                                                        }),
                                                    ),
                                            });
                                    const exit = yield* jobEffect.pipe(
                                        Effect.exit,
                                    );

                                    if (Exit.isSuccess(exit)) {
                                        yield* engine.complete(record.value.id);
                                        const completed = yield* engine.find(
                                            record.value.id,
                                        );

                                        yield* runPluginHooks(
                                            plugins,
                                            (plugin) =>
                                                Option.isSome(completed)
                                                    ? plugin.onJobCompleted?.({
                                                        job: completed.value,
                                                    })
                                                    : undefined,
                                        );
                                    } else {
                                        const cancel = exit.cause.reasons.find(
                                            (reason) =>
                                                Cause.isFailReason(reason) &&
                                                reason.error instanceof
                                                JobCancelError,
                                        );
                                        const snooze = exit.cause.reasons.find(
                                            (reason) =>
                                                Cause.isFailReason(reason) &&
                                                reason.error instanceof
                                                JobSnoozeError,
                                        );

                                        if (
                                            cancel !== undefined &&
                                            Cause.isFailReason(cancel) &&
                                            cancel.error instanceof JobCancelError
                                        ) {
                                            yield* engine.cancel(
                                                record.value.id,
                                                cancel.error.reason,
                                            );
                                            const cancelled = yield* engine.find(
                                                record.value.id,
                                            );

                                            yield* runPluginHooks(
                                                plugins,
                                                (plugin) =>
                                                    Option.isSome(cancelled)
                                                        ? plugin.onJobCancelled?.({
                                                            job: cancelled.value,
                                                            reason:
                                                                cancel.error
                                                                    .reason,
                                                        })
                                                        : undefined,
                                            );
                                            return;
                                        }

                                        if (
                                            snooze !== undefined &&
                                            Cause.isFailReason(snooze) &&
                                            snooze.error instanceof JobSnoozeError
                                        ) {
                                            const runAt = new Date(
                                                Date.now() +
                                                Duration.toMillis(
                                                    snooze.error.duration,
                                                ),
                                            );

                                            yield* engine.snooze(
                                                record.value.id,
                                                runAt,
                                            );
                                            const snoozed = yield* engine.find(
                                                record.value.id,
                                            );

                                            yield* runPluginHooks(
                                                plugins,
                                                (plugin) =>
                                                    Option.isSome(snoozed)
                                                        ? plugin.onJobSnoozed?.({
                                                            job: snoozed.value,
                                                            runAt,
                                                        })
                                                        : undefined,
                                            );
                                            return;
                                        }

                                        const retryAt = new Date(
                                            Date.now() +
                                            Duration.toMillis(
                                                registeredJob.value.job.backoff(
                                                    {
                                                        attempt:
                                                            record.value
                                                                .attempt,
                                                        maxAttempts:
                                                            record.value
                                                                .maxAttempts,
                                                        error: exit.cause,
                                                        job: record.value,
                                                    },
                                                ),
                                            ),
                                        );

                                        yield* engine.fail(
                                            record.value.id,
                                            exit.cause,
                                            { runAt: retryAt },
                                        );
                                        const failed = yield* engine.find(
                                            record.value.id,
                                        );

                                        yield* runPluginHooks(
                                            plugins,
                                            (plugin) =>
                                                Option.isSome(failed)
                                                    ? plugin.onJobFailed?.({
                                                        job: failed.value,
                                                        error: exit.cause,
                                                        retryAt,
                                                    })
                                                    : undefined,
                                        );
                                        yield* runPluginHooks(
                                            plugins,
                                            (plugin) =>
                                                Option.isSome(failed) &&
                                                    failed.value.status === "discarded"
                                                    ? plugin.onJobDiscarded?.({
                                                        job: failed.value,
                                                        error: exit.cause,
                                                    })
                                                    : undefined,
                                        );
                                        yield* Effect.logError(exit.cause);
                                    }
                                });
                                const jobFiber = yield* Effect.uninterruptible(
                                    Effect.gen(function* () {
                                        const fiber = yield* processJob.pipe(
                                            Effect.catch((error) =>
                                                Effect.logError(error),
                                            ),
                                            Effect.forkDetach,
                                        );

                                        yield* Ref.update(activeJobs, (jobs) =>
                                            new Set(jobs).add(fiber),
                                        );

                                        return fiber;
                                    }),
                                );
                                yield* Fiber.join(jobFiber);
                                yield* Ref.update(activeJobs, (jobs) => {
                                    const next = new Set(jobs);

                                    next.delete(jobFiber);

                                    return next;
                                });
                            }),
                        ).pipe(Effect.forkScoped);
                    }
                }

                return yield* Effect.never.pipe(
                    Effect.onInterrupt(() => waitForActiveJobs),
                );
            }),
        ),
};
