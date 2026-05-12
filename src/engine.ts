// Storage contract for queued jobs, plus the current in-memory implementation.

import { Cause, Context, Data, Effect, Layer, Option } from "effect";

import type {
    JobError,
    JobId,
    JobListOptions,
    JobPruneOptions,
    JobRescueOptions,
    JobRescueResult,
    JobRecord,
    NewJob,
    QueueName,
    WorkerId,
} from "./model";

export class DuplicateJobError extends Data.TaggedError("DuplicateJobError")<{
    readonly existing: JobRecord;
}> {}

export class JobStorageError extends Data.TaggedError("JobStorageError")<{
    readonly operation: string;
    readonly cause: unknown;
}> {}

const toMessage = (error: unknown): string => {
    if (error instanceof Error) {
        return error.message.length > 0 ? error.message : error.name;
    }

    if (typeof error === "string") {
        return error;
    }

    if (typeof error === "object" && error !== null) {
        if (
            "message" in error &&
            typeof error.message === "string" &&
            error.message.length > 0
        ) {
            return error.message;
        }

        if (
            "_tag" in error &&
            typeof error._tag === "string" &&
            error._tag.length > 0
        ) {
            return error._tag;
        }

        const constructorName = error.constructor?.name;

        if (constructorName !== undefined && constructorName !== "Object") {
            return constructorName;
        }
    }

    try {
        const json = JSON.stringify(error);
        const message = json === undefined ? String(error) : json;

        return message === "undefined" ? "Unknown error" : message;
    } catch {
        const message = String(error);

        return message === "undefined" ? "Unknown error" : message;
    }
};

const toJsonSafe = (error: unknown): unknown => {
    if (error instanceof Error) {
        return {
            name: error.name,
            message: error.message,
            stack: error.stack,
        };
    }

    return error;
};

export const normalizeJobErrors = (
    error: unknown,
    attempt: number,
    at: Date,
): ReadonlyArray<JobError> => {
    if (!Cause.isCause(error)) {
        return [
            {
                attempt,
                at,
                kind: "unknown",
                message: toMessage(error),
                error: toJsonSafe(error),
            },
        ];
    }

    return error.reasons.map((reason) => {
        if (Cause.isFailReason(reason)) {
            return {
                attempt,
                at,
                kind: "fail",
                message:
                    reason.error === undefined
                        ? "Fail"
                        : toMessage(reason.error),
                error: toJsonSafe(reason.error),
            };
        }

        if (Cause.isDieReason(reason)) {
            return {
                attempt,
                at,
                kind: "die",
                message: toMessage(reason.defect),
                error: toJsonSafe(reason.defect),
            };
        }

        return {
            attempt,
            at,
            kind: "interrupt",
            message:
                reason.fiberId === undefined
                    ? "Interrupted"
                    : `Interrupted by fiber ${reason.fiberId}`,
            error: { fiberId: reason.fiberId },
        };
    });
};

export class JobEngine extends Context.Service<
    JobEngine,
    {
        readonly enqueue: (
            job: NewJob,
        ) => Effect.Effect<JobRecord, DuplicateJobError | JobStorageError>;
        readonly find: (
            id: JobId,
        ) => Effect.Effect<Option.Option<JobRecord>, JobStorageError>;
        readonly list: (
            options?: JobListOptions,
        ) => Effect.Effect<ReadonlyArray<JobRecord>, JobStorageError>;
        readonly claimNext: (
            options?: {
                readonly queue?: QueueName;
                readonly workerId?: WorkerId;
            },
        ) => Effect.Effect<Option.Option<JobRecord>, JobStorageError>;
        readonly complete: (id: JobId) => Effect.Effect<void, JobStorageError>;
        readonly fail: (
            id: JobId,
            error: unknown,
            options?: { readonly runAt?: Date; readonly discard?: boolean },
        ) => Effect.Effect<void, JobStorageError>;
        readonly cancel: (
            id: JobId,
            reason: unknown,
        ) => Effect.Effect<void, JobStorageError>;
        readonly snooze: (
            id: JobId,
            runAt: Date,
        ) => Effect.Effect<void, JobStorageError>;
        readonly runNow: (id: JobId) => Effect.Effect<void, JobStorageError>;
        readonly prune: (
            options: JobPruneOptions,
        ) => Effect.Effect<number, JobStorageError>;
        readonly rescueExecuting: (
            options: JobRescueOptions,
        ) => Effect.Effect<JobRescueResult, JobStorageError>;
    }
>()("effect-job/JobEngine") {}

export const JobEngineMemory = Layer.effect(JobEngine)(
    Effect.sync(() => {
        const jobs = new Map<JobId, JobRecord>();

        const optionContains = <A>(
            option: A | ReadonlyArray<A> | undefined,
            value: A,
        ): boolean =>
            option === undefined
                ? true
                : Array.isArray(option)
                  ? option.includes(value)
                  : option === value;

        const findActiveDuplicate = (job: NewJob): JobRecord | undefined => {
            if (job.idempotencyKey === undefined) {
                return undefined;
            }

            for (const record of jobs.values()) {
                if (
                    record.name === job.name &&
                    record.queue === job.queue &&
                    record.idempotencyKey === job.idempotencyKey &&
                    record.status !== "completed" &&
                    record.status !== "discarded" &&
                    record.status !== "cancelled"
                ) {
                    return record;
                }
            }

            return undefined;
        };

        return {
            enqueue: (job) =>
                Effect.gen(function* () {
                    const existing = findActiveDuplicate(job);

                    if (existing !== undefined) {
                        if (job.duplicatePolicy === "fail") {
                            return yield* new DuplicateJobError({ existing });
                        }

                        return existing;
                    }

                    const now = new Date();
                    const record: JobRecord = {
                        id: job.id,
                        name: job.name,
                        queue: job.queue,
                        payload: job.payload,
                        meta: job.meta,
                        tags: job.tags,
                        status:
                            job.runAt.getTime() > now.getTime()
                                ? "scheduled"
                                : "available",
                        priority: job.priority,
                        attempt: 0,
                        executions: 0,
                        snoozes: 0,
                        maxAttempts: job.maxAttempts,
                        runAt: job.runAt,
                        idempotencyKey: job.idempotencyKey ?? null,
                        attemptedAt: null,
                        attemptedBy: [],
                        completedAt: null,
                        cancelledAt: null,
                        discardedAt: null,
                        errors: [],
                        insertedAt: now,
                        updatedAt: now,
                    };

                    jobs.set(record.id, record);

                    return record;
                }),
            find: (id) => Effect.sync(() => Option.fromNullishOr(jobs.get(id))),
            list: (options) =>
                Effect.sync(() =>
                    Array.from(jobs.values()).filter(
                        (record) =>
                            optionContains(options?.queue, record.queue) &&
                            optionContains(options?.status, record.status),
                    ).slice(0, options?.limit),
                ),
            claimNext: (options) =>
                Effect.sync(() => {
                    const now = Date.now();
                    const candidates = Array.from(jobs.values())
                        .filter(
                            (record) =>
                                (record.status === "available" ||
                                    record.status === "scheduled" ||
                                    record.status === "retryable") &&
                                record.runAt.getTime() <= now &&
                                (options?.queue === undefined ||
                                    record.queue === options.queue),
                        )
                        .sort((a, b) => {
                            // Oban treats lower priority numbers as more important.
                            const priority = a.priority - b.priority;

                            if (priority !== 0) {
                                return priority;
                            }

                            const runAt = a.runAt.getTime() - b.runAt.getTime();

                            if (runAt !== 0) {
                                return runAt;
                            }

                            return (
                                a.insertedAt.getTime() - b.insertedAt.getTime()
                            );
                        });
                    const record = candidates[0];

                    if (record === undefined) {
                        return Option.none();
                    }

                    const claimed: JobRecord = {
                        ...record,
                        status: "executing",
                        executions: record.executions + 1,
                        attemptedAt: new Date(),
                        attemptedBy:
                            options?.workerId === undefined
                                ? record.attemptedBy
                                : [...record.attemptedBy, options.workerId],
                        updatedAt: new Date(),
                    };

                    jobs.set(claimed.id, claimed);

                    return Option.some(claimed);
                }),
            complete: (id) =>
                Effect.sync(() => {
                    const record = jobs.get(id);

                    if (record === undefined) {
                        return;
                    }

                    jobs.set(id, {
                        ...record,
                        status: "completed",
                        completedAt: new Date(),
                        updatedAt: new Date(),
                    });
                }),
            fail: (id, error, options) =>
                Effect.sync(() => {
                    const record = jobs.get(id);

                    if (record === undefined) {
                        return;
                    }

                    const now = new Date();

                    const nextAttempt = record.attempt + 1;

                    jobs.set(id, {
                        ...record,
                        status:
                            options?.discard === true ||
                            nextAttempt >= record.maxAttempts
                                ? "discarded"
                                : "retryable",
                        attempt: nextAttempt,
                        runAt: options?.runAt ?? record.runAt,
                        discardedAt:
                            options?.discard === true ||
                            nextAttempt >= record.maxAttempts
                                ? now
                                : null,
                        errors: [
                            ...record.errors,
                            ...normalizeJobErrors(error, nextAttempt, now),
                        ],
                        updatedAt: now,
                    });
                }),
            cancel: (id, reason) =>
                Effect.sync(() => {
                    const record = jobs.get(id);

                    if (record === undefined) {
                        return;
                    }

                    const now = new Date();

                    jobs.set(id, {
                        ...record,
                        status: "cancelled",
                        cancelledAt: now,
                        errors: [
                            ...record.errors,
                            ...normalizeJobErrors(reason, record.attempt, now),
                        ],
                        updatedAt: now,
                    });
                }),
            snooze: (id, runAt) =>
                Effect.sync(() => {
                    const record = jobs.get(id);

                    if (record === undefined) {
                        return;
                    }

                    jobs.set(id, {
                        ...record,
                        status: "scheduled",
                        runAt,
                        snoozes: record.snoozes + 1,
                        updatedAt: new Date(),
                    });
                }),
            runNow: (id) =>
                Effect.sync(() => {
                    const record = jobs.get(id);

                    if (record === undefined) {
                        return;
                    }

                    if (
                        record.status !== "scheduled" &&
                        record.status !== "retryable"
                    ) {
                        return;
                    }

                    jobs.set(id, {
                        ...record,
                        status: "available",
                        runAt: new Date(),
                        updatedAt: new Date(),
                    });
                }),
            prune: (options) =>
                Effect.sync(() => {
                    const statuses = options.statuses ?? [
                        "completed",
                        "cancelled",
                        "discarded",
                    ];
                    let deleted = 0;

                    for (const [id, record] of jobs) {
                        if (
                            statuses.includes(record.status) &&
                            record.updatedAt.getTime() <
                                options.before.getTime()
                        ) {
                            jobs.delete(id);
                            deleted += 1;
                        }
                    }

                    return deleted;
                }),
            rescueExecuting: (options) =>
                Effect.sync(() => {
                    const now = new Date();
                    const rescued: Array<JobRecord> = [];
                    const discarded: Array<JobRecord> = [];

                    for (const [id, record] of jobs) {
                        if (
                            record.status !== "executing" ||
                            record.attemptedAt === null ||
                            record.attemptedAt.getTime() >=
                                options.before.getTime()
                        ) {
                            continue;
                        }

                        if (record.attempt >= record.maxAttempts) {
                            const next: JobRecord = {
                                ...record,
                                status: "discarded",
                                discardedAt: now,
                                updatedAt: now,
                            };

                            jobs.set(id, next);
                            discarded.push(next);
                            continue;
                        }

                        const next: JobRecord = {
                            ...record,
                            status: "available",
                            runAt: now,
                            updatedAt: now,
                        };

                        jobs.set(id, next);
                        rescued.push(next);
                    }

                    return { rescued, discarded };
                }),
        };
    }),
);
