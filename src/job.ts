import {
    Context,
    Duration,
    Effect,
    Layer,
    Option,
    Schedule,
    Schema,
} from "effect";

export type JobName = string;
export type QueueName = string;
export type JobId = string;

export interface JobContext {
    readonly jobId: JobId;
    readonly attempt: number;
    readonly maxAttempts: number;
    readonly progress: (value: unknown) => Effect.Effect<void>;
    readonly log: (message: string, metadata?: unknown) => Effect.Effect<void>;
}

export interface Job<
    Name extends string,
    Payload extends Schema.Top,
    Success extends Schema.Top,
    Error extends Schema.Top,
> {
    readonly name: Name;
    readonly queue: QueueName;
    readonly payloadSchema: Payload;
    readonly successSchema: Success;
    readonly errorSchema: Error;
    readonly attempts: number;
    readonly retry?: Schedule.Schedule<unknown, Error["Type"]>;
    readonly timeout?: Duration.Input;
    readonly idempotencyKey?: (payload: Payload["Type"]) => string;
    readonly toLayer: <Requirements>(
        run: (
            payload: Payload["Type"],
            context: JobContext,
        ) => Effect.Effect<Success["Type"], Error["Type"], Requirements>,
    ) => Layer.Layer<never, never, JobRegistry | Requirements>;
}

export interface JobHandler<
    Name extends string,
    Payload extends Schema.Top,
    Success extends Schema.Top,
    Error extends Schema.Top,
    Requirements,
> {
    readonly job: Job<Name, Payload, Success, Error>;
    readonly run: (
        payload: Payload["Type"],
        context: JobContext,
    ) => Effect.Effect<Success["Type"], Error["Type"], Requirements>;
}
export namespace JobHandler {
    export type Any = JobHandler<
        string,
        Schema.Top,
        Schema.Top,
        Schema.Top,
        any
    >;
}

export class JobRegistry extends Context.Service<
    JobRegistry,
    {
        readonly register: (handler: JobHandler.Any) => Effect.Effect<void>;
        readonly get: (
            name: JobName,
        ) => Effect.Effect<Option.Option<JobHandler.Any>>;
    }
>()("effect-job/JobRegistry") {}

export interface JobMakeOptions<
    Name extends string,
    Payload extends Schema.Top,
    Success extends Schema.Top,
    Error extends Schema.Top,
> {
    readonly name: Name;
    readonly queue?: QueueName;
    readonly payload: Payload;
    readonly success?: Success;
    readonly error?: Error;
    readonly attempts?: number;
    readonly retry?: Schedule.Schedule<unknown, Error["Type"]>;
    readonly timeout?: Duration.Input;
    readonly idempotencyKey?: (payload: Payload["Type"]) => string;
}

export const Job = {
    make: <
        const Name extends string,
        Payload extends Schema.Top,
        Success extends Schema.Top = typeof Schema.Void,
        Error extends Schema.Top = typeof Schema.Never,
    >(
        options: JobMakeOptions<Name, Payload, Success, Error>,
    ): Job<Name, Payload, Success, Error> => {
        const job: Job<Name, Payload, Success, Error> = {
            name: options.name,
            queue: options.queue ?? "default",
            payloadSchema: options.payload,
            successSchema: (options.success ?? Schema.Void) as Success,
            errorSchema: (options.error ?? Schema.Never) as Error,
            attempts: options.attempts ?? 3,
            ...(options.retry === undefined ? {} : { retry: options.retry }),
            ...(options.timeout === undefined
                ? {}
                : { timeout: options.timeout }),
            ...(options.idempotencyKey === undefined
                ? {}
                : { idempotencyKey: options.idempotencyKey }),
            toLayer: (run) =>
                Layer.effectDiscard(
                    Effect.gen(function* () {
                        const registry = yield* JobRegistry;
                        const handler = {
                            job,
                            run,
                        } as unknown as JobHandler.Any;

                        yield* registry.register(handler);
                    }),
                ),
        };
        return job;
    },
};
