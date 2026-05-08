import { Effect, Layer, Schema } from "effect";

import { enqueue, enqueueMany } from "./enqueue";
import type {
    JobDefinition,
    JobMakeOptions,
    JobModule,
} from "./job";
import { DuplicateJobHandlerError, JobRegistry } from "./registry";

const registerJobHandler = <
    Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top,
    ErrorSchema extends Schema.Top,
    Requirements,
>(
    job: JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema>,
    run: (
        payload: PayloadSchema["Type"],
    ) => Effect.Effect<
        SuccessSchema["Type"],
        ErrorSchema["Type"],
        Requirements
    >,
): Layer.Layer<
    JobRegistry,
    DuplicateJobHandlerError,
    JobRegistry | Requirements
> => {
    const registration = Effect.gen(function* () {
        const registry = yield* JobRegistry;

        yield* registry.register(job, run);

        return registry;
    });

    return Layer.effect(JobRegistry)(registration);
};

const make = <
    const Name extends string,
    PayloadSchema extends Schema.Top,
    SuccessSchema extends Schema.Top = typeof Schema.Void,
    ErrorSchema extends Schema.Top = typeof Schema.Never,
>(
    options: JobMakeOptions<Name, PayloadSchema, SuccessSchema, ErrorSchema>,
): JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema> => {
    const successSchema = (options.success ?? Schema.Void) as SuccessSchema;
    const errorSchema = (options.error ?? Schema.Never) as ErrorSchema;
    const job: JobDefinition<Name, PayloadSchema, SuccessSchema, ErrorSchema> =
        {
            name: options.name,
            queue: options.queue ?? "default",
            payloadSchema: options.payload,
            successSchema,
            errorSchema,
            attempts: options.attempts ?? 3,
            ...(options.retry === undefined ? {} : { retry: options.retry }),
            ...(options.timeout === undefined
                ? {}
                : { timeout: options.timeout }),
            ...(options.idempotencyKey === undefined
                ? {}
                : { idempotencyKey: options.idempotencyKey }),
            toLayer: (run) => registerJobHandler(job, run),
        };

    return job;
};

export const Job: JobModule = {
    make,
    enqueue,
    enqueueMany,
};
