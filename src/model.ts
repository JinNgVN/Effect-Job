// Shared domain model for job identity, lifecycle state, and stored records.

import type { Duration } from "effect";

export type JobName = string;
export type QueueName = string;
export type JobId = string;
export type WorkerId = string;

export type JobStatus =
    | "suspended"
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
    readonly meta: Record<string, unknown>;
    readonly tags: ReadonlyArray<string>;
    readonly status: JobStatus;
    readonly priority: number;
    readonly attempt: number;
    readonly maxAttempts: number;
    readonly runAt: Date;
    readonly idempotencyKey: string | null;
    readonly attemptedAt: Date | null;
    readonly attemptedBy: ReadonlyArray<WorkerId>;
    readonly completedAt: Date | null;
    readonly cancelledAt: Date | null;
    readonly discardedAt: Date | null;
    readonly errors: ReadonlyArray<JobError>;
    readonly insertedAt: Date;
    readonly updatedAt: Date;
}

export interface JobListOptions {
    readonly queue?: QueueName | ReadonlyArray<QueueName>;
    readonly status?: JobStatus | ReadonlyArray<JobStatus>;
    readonly limit?: number;
}

export interface JobPruneOptions {
    readonly before: Date;
    readonly statuses?: ReadonlyArray<JobStatus>;
}

export interface JobRescueOptions {
    readonly before: Date;
}

export interface JobRescueResult {
    readonly rescued: ReadonlyArray<JobRecord>;
    readonly discarded: ReadonlyArray<JobRecord>;
}

export interface JobError {
    readonly attempt: number;
    readonly at: Date;
    readonly kind: "fail" | "die" | "interrupt" | "unknown";
    readonly message: string;
    readonly error: unknown;
}

export interface JobBackoffContext<Error = unknown> {
    readonly attempt: number;
    readonly maxAttempts: number;
    readonly error: Error;
    readonly job: JobRecord;
}

export type JobBackoff<Error = unknown> = (
    context: JobBackoffContext<Error>,
) => Duration.Input;

export interface NewJob {
    readonly id: JobId;
    readonly name: JobName;
    readonly queue: QueueName;
    readonly payload: unknown;
    readonly meta: Record<string, unknown>;
    readonly tags: ReadonlyArray<string>;
    readonly maxAttempts: number;
    readonly runAt: Date;
    readonly priority: number;
    readonly idempotencyKey?: string;
    readonly duplicatePolicy: DuplicatePolicy;
}
