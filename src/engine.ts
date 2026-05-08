import { Context, Data, Effect, Layer, Option } from "effect";

import type { JobId, JobName, QueueName } from "./job";

export type JobStatus =
    | "available"
    | "scheduled"
    | "executing"
    | "retryable"
    | "completed"
    | "discarded"
    | "cancelled";

export type DuplicatePolicy = "use-existing" | "fail";

export interface JobRecord {
    readonly id: JobId;
    readonly name: JobName;
    readonly queue: QueueName;
    readonly payload: unknown;
    readonly status: JobStatus;
    readonly priority: number;
    readonly attempt: number;
    readonly maxAttempts: number;
    readonly runAt: Date;
    readonly idempotencyKey: string | null;
    readonly insertedAt: Date;
    readonly updatedAt: Date;
}

export interface NewJob {
    readonly id: JobId;
    readonly name: JobName;
    readonly queue: QueueName;
    readonly payload: unknown;
    readonly maxAttempts: number;
    readonly runAt: Date;
    readonly priority: number;
    readonly idempotencyKey?: string;
    readonly duplicatePolicy: DuplicatePolicy;
}

export class DuplicateJobError extends Data.TaggedError("DuplicateJobError")<{
    readonly existing: JobRecord;
}> {}

export class JobEngine extends Context.Service<
    JobEngine,
    {
        readonly enqueue: (
            job: NewJob,
        ) => Effect.Effect<JobRecord, DuplicateJobError>;
        readonly find: (id: JobId) => Effect.Effect<Option.Option<JobRecord>>;
        readonly list: Effect.Effect<ReadonlyArray<JobRecord>>;
        readonly claimNext: (
            options?: { readonly queue?: QueueName },
        ) => Effect.Effect<Option.Option<JobRecord>>;
        readonly complete: (id: JobId) => Effect.Effect<void>;
    }
>()("effect-job/JobEngine") {}

export const JobEngineMemory = Layer.effect(JobEngine)(
    Effect.sync(() => {
        const jobs = new Map<JobId, JobRecord>();

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
                        status:
                            job.runAt.getTime() > now.getTime()
                                ? "scheduled"
                                : "available",
                        priority: job.priority,
                        attempt: 0,
                        maxAttempts: job.maxAttempts,
                        runAt: job.runAt,
                        idempotencyKey: job.idempotencyKey ?? null,
                        insertedAt: now,
                        updatedAt: now,
                    };

                    jobs.set(record.id, record);

                    return record;
                }),
            find: (id) => Effect.sync(() => Option.fromNullishOr(jobs.get(id))),
            list: Effect.sync(() => Array.from(jobs.values())),
            claimNext: (options) =>
                Effect.sync(() => {
                    const now = Date.now();
                    const candidates = Array.from(jobs.values())
                        .filter(
                            (record) =>
                                record.status === "available" &&
                                record.runAt.getTime() <= now &&
                                (options?.queue === undefined ||
                                    record.queue === options.queue),
                        )
                        .sort((a, b) => {
                            const priority = b.priority - a.priority;

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
                        attempt: record.attempt + 1,
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
                        updatedAt: new Date(),
                    });
                }),
        };
    }),
);
