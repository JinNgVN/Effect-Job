// Public Job API: define jobs, attach handlers as Layers, and insert work.

import { randomUUID } from "node:crypto";

import { Data, Duration, Effect, Layer, Option, Schema } from "effect";

import { DuplicateJobError, JobEngine, type JobStorageError } from "./engine";
import type {
    DuplicatePolicy,
    JobBackoff,
    JobId,
    JobListOptions,
    JobName,
    JobPruneOptions,
    JobRecord,
    NewJob,
    JobRescueOptions,
    JobRescueResult,
    QueueName,
} from "./model";
import { JobNotifier } from "./notifier";
import {
    JobRegistry,
    type DuplicateJobHandlerError,
    type JobRun,
} from "./registry";

export interface JobDefinition<
    Name extends string,
    PayloadSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> {
    readonly name: Name;
    readonly queue: QueueName;
    readonly payloadSchema: PayloadSchema;
    readonly errorSchema: ErrorSchema;
    readonly attempts: number;
    readonly backoff: JobBackoff;
    readonly timeout?: Duration.Input;
    readonly idempotencyKey?: (payload: PayloadSchema["Type"]) => string;
    readonly new: (
        payload: PayloadSchema["Type"],
        options?: InsertOptions,
    ) => BuildJobEffect<Name, PayloadSchema, ErrorSchema>;
    readonly toLayer: <Requirements>(
        run: (
            payload: PayloadSchema["Type"],
            context: JobContext<Name>,
        ) => Effect.Effect<
            unknown,
            ErrorSchema["Type"],
            Requirements
        >,
    ) => Layer.Layer<
        JobRegistry,
        DuplicateJobHandlerError,
        JobRegistry | Requirements
    >;
}

export namespace JobDefinition {
    export type Any = JobDefinition<string, any, any>;
}

export interface JobContext<Name extends string = string> {
    readonly id: JobId;
    readonly name: Name;
    readonly queue: QueueName;
    readonly meta: Record<string, unknown>;
    readonly tags: ReadonlyArray<string>;
    readonly attempt: number;
    readonly maxAttempts: number;
    readonly runAt: Date;
    readonly insertedAt: Date;
    readonly attemptedBy: ReadonlyArray<string>;
}

export interface JobMakeOptions<
    Name extends string,
    PayloadSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> {
    readonly name: Name;
    readonly queue?: QueueName;
    readonly payload: PayloadSchema;
    readonly error?: ErrorSchema;
    readonly attempts?: number;
    readonly backoff?: JobBackoff;
    readonly timeout?: Duration.Input;
    readonly idempotencyKey?: (payload: PayloadSchema["Type"]) => string;
}

export interface JobHandle<
    Name extends string = string,
    ErrorSchema extends Schema.Top = Schema.Top,
> {
    readonly id: string;
    readonly name: Name;
    readonly queue: string;
    readonly errorSchema: ErrorSchema;
}

export interface JobInsert<
    Name extends string = string,
    ErrorSchema extends Schema.Top = Schema.Top,
> {
    readonly job: Pick<
        JobDefinition<Name, Schema.Top, ErrorSchema>,
        "name" | "queue" | "errorSchema"
    >;
    readonly newJob: NewJob;
}

export interface InsertOptions {
    readonly delay?: Duration.Input;
    readonly runAt?: Date;
    readonly priority?: number;
    readonly meta?: Record<string, unknown>;
    readonly tags?: ReadonlyArray<string>;
    readonly idempotencyKey?: string;
    readonly duplicate?: DuplicatePolicy;
}

export type InsertError =
    | Schema.SchemaError
    | DuplicateJobError
    | JobStorageError;

export type BuildJobEffect<
    Name extends string,
    PayloadSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> = Effect.Effect<
    JobInsert<Name, ErrorSchema>,
    Schema.SchemaError,
    PayloadSchema["EncodingServices"]
>;

export type InsertJobEffect<
    Name extends string,
    ErrorSchema extends Schema.Top,
> = Effect.Effect<
    JobHandle<Name, ErrorSchema>,
    InsertError,
    JobEngine
>;

export type JobInsertInput<
    Name extends string,
    ErrorSchema extends Schema.Top,
> =
    | JobInsert<Name, ErrorSchema>
    | Effect.Effect<JobInsert<Name, ErrorSchema>, Schema.SchemaError, any>;

export type InsertRequirements<PayloadSchema extends Schema.Top> =
    | JobEngine
    | PayloadSchema["EncodingServices"];

export type InsertNowEffect<
    Name extends string,
    PayloadSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> = Effect.Effect<
    JobHandle<Name, ErrorSchema>,
    InsertError,
    InsertRequirements<PayloadSchema>
>;

export type InsertManyEffect<
    Name extends string,
    ErrorSchema extends Schema.Top,
> = Effect.Effect<
    ReadonlyArray<JobHandle<Name, ErrorSchema>>,
    InsertError,
    JobEngine
>;

export interface JobModule {
    readonly make: <
        const Name extends string,
        PayloadSchema extends Schema.Top,
        ErrorSchema extends Schema.Top = typeof Schema.Never,
    >(
        options: JobMakeOptions<
            Name,
            PayloadSchema,
            ErrorSchema
        >,
    ) => JobDefinition<Name, PayloadSchema, ErrorSchema>;

    readonly insert: <
        const Name extends string,
        ErrorSchema extends Schema.Top,
    >(
        prepared: JobInsertInput<Name, ErrorSchema>,
    ) => InsertJobEffect<Name, ErrorSchema>;

    readonly insertMany: <
        const Name extends string,
        ErrorSchema extends Schema.Top,
    >(
        prepared: ReadonlyArray<JobInsertInput<Name, ErrorSchema>>,
    ) => InsertManyEffect<Name, ErrorSchema>;

    readonly cancel: (reason: unknown) => Effect.Effect<never, JobCancelError>;
    readonly snooze: (
        duration: Duration.Input,
    ) => Effect.Effect<never, JobSnoozeError>;
    readonly find: (
        id: JobId,
    ) => Effect.Effect<Option.Option<JobRecord>, JobStorageError, JobEngine>;
    readonly list: (
        options?: JobListOptions,
    ) => Effect.Effect<ReadonlyArray<JobRecord>, JobStorageError, JobEngine>;
    readonly cancelById: (
        id: JobId,
        reason: unknown,
    ) => Effect.Effect<void, JobStorageError, JobEngine>;
    readonly runNow: (
        id: JobId,
    ) => Effect.Effect<void, JobStorageError, JobEngine>;
    readonly prune: (
        options: JobPruneOptions,
    ) => Effect.Effect<number, JobStorageError, JobEngine>;
    readonly rescueExecuting: (
        options: JobRescueOptions,
    ) => Effect.Effect<JobRescueResult, JobStorageError, JobEngine>;
}

export class JobCancelError extends Data.TaggedError("JobCancelError")<{
    readonly reason: unknown;
}> {}

export class JobSnoozeError extends Data.TaggedError("JobSnoozeError")<{
    readonly duration: Duration.Input;
}> {}

export const defaultBackoff: JobBackoff = ({ attempt }) =>
    Duration.seconds(
        Math.trunc(Math.pow(attempt, 4) + 15 + Math.random() * 30 * attempt),
    );

const make = <
    const Name extends string,
    PayloadSchema extends Schema.Top,
    ErrorSchema extends Schema.Top = typeof Schema.Never,
>(
    options: JobMakeOptions<Name, PayloadSchema, ErrorSchema>,
): JobDefinition<Name, PayloadSchema, ErrorSchema> => {
    const errorSchema = (options.error ?? Schema.Never) as ErrorSchema;
    const job: JobDefinition<Name, PayloadSchema, ErrorSchema> =
        {
            name: options.name,
            queue: options.queue ?? "default",
            payloadSchema: options.payload,
            errorSchema,
            attempts: options.attempts ?? 20,
            backoff: options.backoff ?? defaultBackoff,
            ...(options.timeout === undefined
                ? {}
                : { timeout: options.timeout }),
            ...(options.idempotencyKey === undefined
                ? {}
                : { idempotencyKey: options.idempotencyKey }),
            new: (payload, insertOptions) =>
                prepare(job, payload, insertOptions),
            toLayer: (run) =>
                Layer.effect(JobRegistry)(
                    Effect.gen(function* () {
                        const registry = yield* JobRegistry;

                        yield* registry.register(job, run as JobRun);

                        return registry;
                    }),
                ),
        };

    return job;
};

const computeRunAt = (options?: InsertOptions): Date => {
    if (options?.runAt !== undefined) {
        return options.runAt;
    }

    if (options?.delay !== undefined) {
        return new Date(Date.now() + Duration.toMillis(options.delay));
    }

    return new Date();
};

const toHandle = <
    const Name extends string,
    ErrorSchema extends Schema.Top,
>(
    job: Pick<
        JobDefinition<Name, Schema.Top, ErrorSchema>,
        "name" | "queue" | "errorSchema"
    >,
    id: string,
): JobHandle<Name, ErrorSchema> => ({
    id,
    name: job.name,
    queue: job.queue,
    errorSchema: job.errorSchema,
});

const isJobInsertEffect = <
    const Name extends string,
    ErrorSchema extends Schema.Top,
>(
    prepared: JobInsertInput<Name, ErrorSchema>,
): prepared is Effect.Effect<
    JobInsert<Name, ErrorSchema>,
    Schema.SchemaError,
    any
> =>
    typeof prepared === "object" &&
    prepared !== null &&
    "pipe" in prepared &&
    !("newJob" in prepared);

const prepare = <
    const Name extends string,
    PayloadSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
>(
    job: JobDefinition<Name, PayloadSchema, ErrorSchema>,
    payload: PayloadSchema["Type"],
    options?: InsertOptions,
): BuildJobEffect<Name, PayloadSchema, ErrorSchema> =>
    Effect.gen(function* () {
        const encodedPayload = yield* Schema.encodeEffect(job.payloadSchema)(
            payload,
        );
        const idempotencyKey =
            options?.idempotencyKey ?? job.idempotencyKey?.(payload);

        return {
            job,
            newJob: {
                id: randomUUID(),
                name: job.name,
                queue: job.queue,
                payload: encodedPayload,
                meta: options?.meta ?? {},
                tags: options?.tags ?? [],
                maxAttempts: job.attempts,
                runAt: computeRunAt(options),
                priority: options?.priority ?? 0,
                ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
                duplicatePolicy: options?.duplicate ?? "use-existing",
            },
        };
    });

const insert = <
    const Name extends string,
    ErrorSchema extends Schema.Top,
>(
    input: JobInsertInput<Name, ErrorSchema>,
): InsertJobEffect<Name, ErrorSchema> =>
    Effect.gen(function* () {
        const prepared = isJobInsertEffect(input) ? yield* input : input;
        const engine = yield* JobEngine;
        const notifier = yield* JobNotifier;
        const record = yield* engine.enqueue(prepared.newJob);

        yield* notifier.notifyInsert({ queue: record.queue });

        return toHandle(prepared.job, record.id);
    });

const insertMany = <
    const Name extends string,
    ErrorSchema extends Schema.Top,
>(
    prepared: ReadonlyArray<JobInsertInput<Name, ErrorSchema>>,
): InsertManyEffect<Name, ErrorSchema> =>
    Effect.forEach(prepared, insert, {
        concurrency: 1,
    });

export const Job: JobModule = {
    make,
    insert,
    insertMany,
    cancel: (reason) => Effect.fail(new JobCancelError({ reason })),
    snooze: (duration) => Effect.fail(new JobSnoozeError({ duration })),
    find: (id) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.find(id);
        }),
    list: (options) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.list(options);
        }),
    cancelById: (id, reason) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            yield* engine.cancel(id, reason);
        }),
    runNow: (id) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            yield* engine.runNow(id);
        }),
    prune: (options) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.prune(options);
        }),
    rescueExecuting: (options) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.rescueExecuting(options);
        }),
};

export type { JobId, JobName, QueueName };
