import type { Duration, Effect, Layer, Schedule, Schema } from "effect";

import type {
    EnqueueEffect,
    EnqueueManyEffect,
    EnqueueOptions,
} from "./enqueue";
import type { DuplicateJobHandlerError, JobRegistry } from "./registry";

export type JobName = string;
export type QueueName = string;
export type JobId = string;

export interface JobDefinition<
    Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> {
    readonly name: Name;
    readonly queue: QueueName;
    readonly payloadSchema: PayloadSchema;
    readonly successSchema: SuccessSchema;
    readonly errorSchema: ErrorSchema;
    readonly attempts: number;
    readonly retry?: Schedule.Schedule<unknown, ErrorSchema["Type"]>;
    readonly timeout?: Duration.Input;
    readonly idempotencyKey?: (payload: PayloadSchema["Type"]) => string;
    readonly toLayer: <Requirements>(
        run: (
            payload: PayloadSchema["Type"],
        ) => Effect.Effect<
            SuccessSchema["Type"],
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
    export type Any = JobDefinition<string, any, any, any>;
}

export interface JobMakeOptions<
    Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
> {
    readonly name: Name;
    readonly queue?: QueueName;
    readonly payload: PayloadSchema;
    readonly success?: SuccessSchema;
    readonly error?: ErrorSchema;
    readonly attempts?: number;
    readonly retry?: Schedule.Schedule<unknown, ErrorSchema["Type"]>;
    readonly timeout?: Duration.Input;
    readonly idempotencyKey?: (payload: PayloadSchema["Type"]) => string;
}

export interface JobModule {
    readonly make: <
        const Name extends string,
        PayloadSchema extends Schema.Top,
        SuccessSchema extends Schema.Top = typeof Schema.Void,
        ErrorSchema extends Schema.Top = typeof Schema.Never,
    >(
        options: JobMakeOptions<
            Name,
            PayloadSchema,
            SuccessSchema,
            ErrorSchema
        >,
    ) => JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema>;

    readonly enqueue: <
        const Name extends string,
        PayloadSchema extends Schema.Top,
        SuccessSchema extends Schema.Top,
        ErrorSchema extends Schema.Top,
    >(
        job: JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema>,
        payload: PayloadSchema["Type"],
        options?: EnqueueOptions,
    ) => EnqueueEffect<Name, PayloadSchema, SuccessSchema, ErrorSchema>;

    readonly enqueueMany: <
        const Name extends string,
        PayloadSchema extends Schema.Top,
        SuccessSchema extends Schema.Top,
        ErrorSchema extends Schema.Top,
    >(
        job: JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema>,
        payloads: ReadonlyArray<PayloadSchema["Type"]>,
        options?: EnqueueOptions,
    ) => EnqueueManyEffect<Name, PayloadSchema, SuccessSchema, ErrorSchema>;
}
