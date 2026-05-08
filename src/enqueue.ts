import { randomUUID } from "node:crypto";

import { Duration, Effect, Schema } from "effect";

import { DuplicateJobError, JobEngine } from "./engine";
import type { DuplicatePolicy } from "./engine";
import type { JobDefinition } from "./job";

export interface JobHandle<
    Name extends string = string,
    SuccessSchema extends Schema.Top = Schema.Top,
    ErrorSchema extends Schema.Top = Schema.Top,
> {
    readonly id: string;
    readonly name: Name;
    readonly queue: string;
    readonly successSchema: SuccessSchema;
    readonly errorSchema: ErrorSchema;
}

export interface EnqueueOptions {
    readonly delay?: Duration.Input;
    readonly runAt?: Date;
    readonly priority?: number;
    readonly idempotencyKey?: string;
    readonly duplicate?: DuplicatePolicy;
}

export type EnqueueError = Schema.SchemaError | DuplicateJobError;

export type EnqueueRequirements<PayloadSchema extends Schema.Top> =
    | JobEngine
    | PayloadSchema["EncodingServices"];

export type EnqueueEffect<
    Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> = Effect.Effect<
    JobHandle<Name, SuccessSchema, ErrorSchema>,
    EnqueueError,
    EnqueueRequirements<PayloadSchema>
>;

export type EnqueueManyEffect<
    Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> = Effect.Effect<
    ReadonlyArray<JobHandle<Name, SuccessSchema, ErrorSchema>>,
    EnqueueError,
    EnqueueRequirements<PayloadSchema>
>;

const computeRunAt = (options?: EnqueueOptions): Date => {
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
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
>(
    job: Pick<
        JobDefinition<Name, Schema.Top, SuccessSchema, ErrorSchema>,
        "name" | "queue" | "successSchema" | "errorSchema"
    >,
    id: string,
): JobHandle<Name, SuccessSchema, ErrorSchema> => ({
    id,
    name: job.name,
    queue: job.queue,
    successSchema: job.successSchema,
    errorSchema: job.errorSchema,
});

export const enqueue = <
    const Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
>(
    job: JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema>,
    payload: PayloadSchema["Type"],
    options?: EnqueueOptions,
): EnqueueEffect<Name, PayloadSchema, SuccessSchema, ErrorSchema> =>
    Effect.gen(function* () {
        const engine = yield* JobEngine;
        const encodedPayload = yield* Schema.encodeEffect(job.payloadSchema)(
            payload,
        );
        const idempotencyKey =
            options?.idempotencyKey ?? job.idempotencyKey?.(payload);

        const record = yield* engine.enqueue({
            id: randomUUID(),
            name: job.name,
            queue: job.queue,
            payload: encodedPayload,
            maxAttempts: job.attempts,
            runAt: computeRunAt(options),
            priority: options?.priority ?? 0,
            ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
            duplicatePolicy: options?.duplicate ?? "use-existing",
        });

        return toHandle(job, record.id);
    });

export const enqueueMany = <
    const Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
>(
    job: JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema>,
    payloads: ReadonlyArray<PayloadSchema["Type"]>,
    options?: EnqueueOptions,
): EnqueueManyEffect<Name, PayloadSchema, SuccessSchema, ErrorSchema> =>
    Effect.forEach(payloads, (payload) => enqueue(job, payload, options), {
        concurrency: 1,
    });
