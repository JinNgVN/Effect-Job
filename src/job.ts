// vNext public job primitives: definitions, insert commands, outcomes, and
// Effect services used by configured job systems.

import { randomUUID } from "node:crypto";

import { Context, Data, Duration, Effect, Layer, Option, Schema } from "effect";

import { DuplicateJobError, JobEngine, type JobStorageError } from "./engine";
import type {
    DuplicatePolicy,
    JobBackoffContext,
    JobId,
    JobListOptions,
    JobName,
    JobPruneOptions,
    JobRecord,
    JobRescueOptions,
    JobRescueResult,
    NewJob,
    QueueName,
} from "./model";
import { JobNotifier } from "./notifier";
import type { DynamicQueueName, QueueSelection } from "./queue";
import { Queue } from "./queue";
import {
    JobRegistry,
    type DuplicateJobHandlerError,
    type JobRun,
} from "./registry";

export type MaybeEffect<A, E = never, R = never> = A | Effect.Effect<A, E, R>;

export type Policy<Context, Value, Requirements = never> =
    | Value
    | ((context: Context) => MaybeEffect<Value, never, Requirements>);

export type JobQueuePolicy<Payload, Queues extends string = string> = Policy<
    JobCommandPolicyContext<Payload, Queues>,
    QueueSelection<Queues>,
    any
>;

export interface JobCommandPolicyContext<
    Payload = unknown,
    Queues extends string = string,
> {
    readonly payload: Payload;
    readonly options: JobCommandOptions<Queues>;
}

export interface JobRunContext<Name extends string = string> {
    readonly id: JobId;
    readonly name: Name;
    readonly queue: QueueName;
    readonly meta: Record<string, unknown>;
    readonly tags: ReadonlyArray<string>;
    readonly attempt: number;
    readonly executions: number;
    readonly snoozes: number;
    readonly maxAttempts: number;
    readonly runAt: Date;
    readonly insertedAt: Date;
    readonly attemptedBy: ReadonlyArray<string>;
}

export interface JobRunInput<Payload = unknown, Name extends string = string> {
    readonly payload: Payload;
    readonly job: JobRunContext<Name>;
    readonly context: JobRunContext<Name>;
}

export type JobRunHandler<
    Payload,
    Name extends string = string,
    Result = unknown,
    Error = unknown,
    Requirements = never,
> = (
    input: JobRunInput<Payload, Name>,
) => Effect.Effect<Result, Error, Requirements>;

export interface JobAttemptsOptions<
    Payload = unknown,
    Queues extends string = string,
> {
    readonly max?: Policy<JobCommandPolicyContext<Payload, Queues>, number, any>;
    readonly backoff?: (
        context: JobBackoffContext,
    ) => MaybeEffect<Duration.Input, never, any>;
    readonly classify?: (context: {
        readonly error: unknown;
        readonly job: JobRecord;
    }) => MaybeEffect<"retry" | "discard" | "cancel", never, any>;
}

export type JobAttempts<Payload = unknown, Queues extends string = string> =
    | number
    | JobAttemptsOptions<Payload, Queues>;

export interface UniqueOptions<
    Payload = unknown,
    Queues extends string = string,
> {
    readonly key: Policy<
        JobCommandPolicyContext<Payload, Queues>,
        string | ReadonlyArray<string | number | boolean>,
        any
    >;
    readonly while?: ReadonlyArray<string>;
    readonly for?: Duration.Input | "forever";
}

export interface ConcurrencyOptions<
    Payload = unknown,
    Queues extends string = string,
> {
    readonly key?: Policy<
        JobCommandPolicyContext<Payload, Queues>,
        string | ReadonlyArray<string | number | boolean>,
        any
    >;
    readonly limit: Policy<JobCommandPolicyContext<Payload, Queues>, number, any>;
    readonly scope?: "local" | "global";
}

export interface RateLimitOptions<
    Payload = unknown,
    Queues extends string = string,
> {
    readonly key?: Policy<
        JobCommandPolicyContext<Payload, Queues>,
        string | ReadonlyArray<string | number | boolean>,
        any
    >;
    readonly limit: Policy<JobCommandPolicyContext<Payload, Queues>, number, any>;
    readonly per: Duration.Input;
    readonly weight?: Policy<JobCommandPolicyContext<Payload, Queues>, number, any>;
    readonly scope?: "local" | "global";
}

export interface DashboardOptions<
    Payload = unknown,
    Queues extends string = string,
> {
    readonly title?: Policy<JobCommandPolicyContext<Payload, Queues>, string, any>;
    readonly dimensions?: Readonly<
        Record<string, Policy<JobCommandPolicyContext<Payload, Queues>, string, any>>
    >;
    readonly publicPayload?: Policy<
        JobCommandPolicyContext<Payload, Queues>,
        Record<string, unknown>,
        any
    >;
}

export interface JobHooks<
    Payload = unknown,
    Result = unknown,
    Queues extends string = string,
> {
    readonly beforeEnqueue?: (context: {
        readonly payload: Payload;
        readonly options: JobCommandOptions<Queues>;
    }) => MaybeEffect<
        { readonly payload: Payload; readonly options: JobCommandOptions<Queues> },
        unknown,
        any
    >;
    readonly beforeRun?: (context: {
        readonly job: JobRecord;
    }) => MaybeEffect<void, unknown, any>;
    readonly afterComplete?: (context: {
        readonly job: JobRecord;
        readonly result: Result;
    }) => MaybeEffect<void, unknown, any>;
    readonly afterDiscard?: (context: {
        readonly job: JobRecord;
        readonly reason: unknown;
    }) => MaybeEffect<void, unknown, any>;
}

export interface JobDefinitionOptions<
    Name extends string,
    PayloadSchema extends Schema.Top,
    ResultSchema extends Schema.Top = typeof Schema.Unknown,
    Queues extends string = string,
> {
    readonly name: Name;
    readonly queue: JobQueuePolicy<PayloadSchema["Type"], Queues>;
    readonly payload: PayloadSchema;
    readonly result?: ResultSchema;
    readonly attempts?: JobAttempts<PayloadSchema["Type"], Queues>;
    readonly timeout?: Duration.Input;
    readonly unique?: UniqueOptions<PayloadSchema["Type"], Queues>;
    readonly concurrency?: ConcurrencyOptions<PayloadSchema["Type"], Queues>;
    readonly rateLimit?: RateLimitOptions<PayloadSchema["Type"], Queues>;
    readonly dashboard?: DashboardOptions<PayloadSchema["Type"], Queues>;
    readonly hooks?: JobHooks<PayloadSchema["Type"], ResultSchema["Type"], Queues>;
}

export interface JobDefinition<
    Name extends string = string,
    PayloadSchema extends Schema.Top = Schema.Top,
    ResultSchema extends Schema.Top = Schema.Top,
    Queues extends string = string,
> {
    readonly _tag: "JobDefinition";
    readonly name: Name;
    readonly queue: JobQueuePolicy<PayloadSchema["Type"], Queues>;
    readonly payload: PayloadSchema;
    readonly payloadSchema: PayloadSchema;
    readonly result: ResultSchema;
    readonly resultSchema: ResultSchema;
    readonly attempts: JobAttempts<PayloadSchema["Type"], Queues>;
    readonly defaultMaxAttempts: number;
    readonly backoff: (
        context: JobBackoffContext,
    ) => MaybeEffect<Duration.Input, never, any>;
    readonly timeout?: Duration.Input;
    readonly unique?: UniqueOptions<PayloadSchema["Type"], Queues>;
    readonly concurrency?: ConcurrencyOptions<PayloadSchema["Type"], Queues>;
    readonly rateLimit?: RateLimitOptions<PayloadSchema["Type"], Queues>;
    readonly dashboard?: DashboardOptions<PayloadSchema["Type"], Queues>;
    readonly hooks?: JobHooks<PayloadSchema["Type"], ResultSchema["Type"], Queues>;
    readonly command: (
        payload: PayloadSchema["Type"],
        options?: JobCommandOptions<Queues>,
    ) => JobCommand<Name, PayloadSchema["Type"], ResultSchema["Type"], Queues>;
    readonly enqueue: (
        payload: PayloadSchema["Type"],
        options?: JobCommandOptions<Queues>,
    ) => Effect.Effect<
        JobHandle<Name, ResultSchema>,
        InsertError,
        JobRuntime | PayloadSchema["EncodingServices"]
    >;
    readonly enqueueMany: (
        payloads: ReadonlyArray<PayloadSchema["Type"]>,
        options?: JobEnqueueManyOptions<Queues>,
    ) => Effect.Effect<
        ReadonlyArray<JobHandle<Name, ResultSchema>>,
        InsertError,
        JobRuntime | PayloadSchema["EncodingServices"]
    >;
    readonly enqueueStream: (
        payloads: Iterable<PayloadSchema["Type"]>,
        options?: JobEnqueueManyOptions<Queues>,
    ) => Effect.Effect<
        ReadonlyArray<JobHandle<Name, ResultSchema>>,
        InsertError,
        JobRuntime | PayloadSchema["EncodingServices"]
    >;
    readonly toLayer: <Requirements, Error = unknown>(
        run: JobRunHandler<
            PayloadSchema["Type"],
            Name,
            ResultSchema["Type"],
            Error,
            Requirements
        >,
    ) => Layer.Layer<
        JobRegistry,
        DuplicateJobHandlerError,
        JobRegistry | Requirements
    >;
}

export namespace JobDefinition {
    export type Any = JobDefinition<any, any, any, any>;
}

export interface JobCommandOptions<Queues extends string = string> {
    readonly delay?: Duration.Input;
    readonly runAt?: Date;
    readonly priority?: number;
    readonly meta?: Record<string, unknown>;
    readonly tags?: ReadonlyArray<string>;
    readonly queue?: QueueSelection<Queues>;
    readonly idempotencyKey?: string;
    readonly duplicate?: DuplicatePolicy;
}

export interface JobEnqueueManyOptions<Queues extends string = string>
    extends JobCommandOptions<Queues> {
    readonly chunkSize?: number;
    readonly concurrency?: number;
    readonly onInvalidPayload?: "fail" | "collect-errors";
}

export interface JobCommandIssue {
    readonly path: ReadonlyArray<string>;
    readonly message: string;
}

export interface JobCommand<
    Name extends string = string,
    Payload = unknown,
    Result = unknown,
    Queues extends string = string,
> {
    readonly _tag: "JobCommand";
    readonly job: JobDefinition<Name, any, any, Queues>;
    readonly name: Name;
    readonly payload: Payload;
    readonly options: JobCommandOptions<Queues>;
    readonly changes: JobCommandOptions<Queues>;
    readonly errors: ReadonlyArray<JobCommandIssue>;
    readonly valid: boolean;
    readonly _Result?: Result;
}

export interface ResolvedJobCommand<
    Name extends string = string,
    ResultSchema extends Schema.Top = Schema.Top,
> {
    readonly command: JobCommand<Name, unknown, ResultSchema["Type"], any>;
    readonly job: JobDefinition<Name, Schema.Top, ResultSchema, any>;
    readonly newJob: NewJob;
}

export interface JobHandle<
    Name extends string = string,
    ResultSchema extends Schema.Top = Schema.Top,
> {
    readonly id: JobId;
    readonly name: Name;
    readonly queue: QueueName;
    readonly resultSchema: ResultSchema;
}

export class JobCommandInvalidError extends Data.TaggedError(
    "JobCommandInvalidError",
)<{
    readonly command: JobCommand;
    readonly errors: ReadonlyArray<JobCommandIssue>;
}> {}

export class JobCancelError extends Data.TaggedError("JobCancelError")<{
    readonly reason: unknown;
}> {}

export class JobDiscardError extends Data.TaggedError("JobDiscardError")<{
    readonly reason: unknown;
}> {}

export class JobSnoozeError extends Data.TaggedError("JobSnoozeError")<{
    readonly duration: Duration.Input;
    readonly reason?: unknown;
}> {}

export type InsertError =
    | Schema.SchemaError
    | JobCommandInvalidError
    | DuplicateJobError
    | JobStorageError;

export class JobRuntime extends Context.Service<
    JobRuntime,
    {
        readonly insert: <
            const Name extends string,
            ResultSchema extends Schema.Top,
        >(
            command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
        ) => Effect.Effect<JobHandle<Name, ResultSchema>, InsertError, any>;
        readonly insertMany: <
            const Name extends string,
            ResultSchema extends Schema.Top,
        >(
            commands: ReadonlyArray<
                JobCommand<Name, unknown, ResultSchema["Type"], any>
            >,
        ) => Effect.Effect<
            ReadonlyArray<JobHandle<Name, ResultSchema>>,
            InsertError,
            any
        >;
        readonly resolve: <
            const Name extends string,
            ResultSchema extends Schema.Top,
        >(
            command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
        ) => Effect.Effect<
            ResolvedJobCommand<Name, ResultSchema>,
            InsertError,
            any
        >;
    }
>()("effect-job/JobRuntime") {}

export const defaultBackoff = ({
    attempt,
}: JobBackoffContext): Duration.Input =>
    Duration.seconds(
        Math.trunc(Math.pow(attempt, 4) + 15 + Math.random() * 30 * attempt),
    );

const isEffectLike = <A>(
    value: unknown,
): value is Effect.Effect<A, never, any> => Effect.isEffect(value);

export const resolvePolicy = <Context, Value>(
    policy: Policy<Context, Value, any>,
    context: Context,
): Effect.Effect<Value, never, any> => {
    const value =
        typeof policy === "function"
            ? (policy as (context: Context) => MaybeEffect<Value, never, any>)(
                  context,
              )
            : policy;

    return isEffectLike<Value>(value) ? value : Effect.succeed(value);
};

const computeRunAt = (options: JobCommandOptions): Date => {
    if (options.runAt !== undefined) {
        return options.runAt;
    }

    if (options.delay !== undefined) {
        return new Date(Date.now() + Duration.toMillis(options.delay));
    }

    return new Date();
};

const normalizeKey = (
    key: string | ReadonlyArray<string | number | boolean>,
): string => (typeof key === "string" ? key : key.map(String).join(":"));

const queueName = (queue: QueueSelection): QueueName =>
    Queue.isDynamic(queue) ? queue.name : queue;

const maxAttemptsFrom = <Payload, Queues extends string>(
    attempts: JobAttempts<Payload, Queues>,
): number => {
    if (typeof attempts === "number") {
        return attempts;
    }

    if (typeof attempts.max === "number") {
        return attempts.max;
    }

    return 20;
};

const backoffFrom = <Payload, Queues extends string>(
    attempts: JobAttempts<Payload, Queues>,
): ((context: JobBackoffContext) => MaybeEffect<Duration.Input, never, any>) =>
    typeof attempts === "number" || attempts.backoff === undefined
        ? defaultBackoff
        : attempts.backoff;

const makeCommand = <
    const Name extends string,
    Payload,
    Result,
    Queues extends string,
>(
    job: JobDefinition<Name, any, any, Queues>,
    payload: Payload,
    options: JobCommandOptions<Queues> = {},
    errors: ReadonlyArray<JobCommandIssue> = [],
): JobCommand<Name, Payload, Result, Queues> => {
    const command = {
        _tag: "JobCommand" as const,
        job,
        name: job.name,
        payload,
        options,
        changes: options,
        errors,
        valid: errors.length === 0,
        _Result: undefined as Result | undefined,
    };

    return command;
};

const toHandle = <const Name extends string, ResultSchema extends Schema.Top>(
    job: JobDefinition<Name, Schema.Top, ResultSchema, any>,
    record: JobRecord,
): JobHandle<Name, ResultSchema> => ({
    id: record.id,
    name: job.name,
    queue: record.queue,
    resultSchema: job.resultSchema,
});

export const resolveJobCommand = <
    const Name extends string,
    ResultSchema extends Schema.Top,
>(
    command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
): Effect.Effect<ResolvedJobCommand<Name, ResultSchema>, InsertError, any> =>
    Effect.gen(function* () {
        if (!command.valid) {
            return yield* new JobCommandInvalidError({
                command: command as JobCommand<any, any, any, any>,
                errors: command.errors,
            });
        }

        const job = command.job as JobDefinition<
            Name,
            Schema.Top,
            ResultSchema,
            string
        >;
        const context: JobCommandPolicyContext<unknown, string> = {
            payload: command.payload,
            options: command.options,
        };
        const encodedPayload = yield* Schema.encodeEffect(job.payloadSchema)(
            command.payload,
        );
        const selectedQueue =
            command.options.queue ?? (yield* resolvePolicy(job.queue, context));
        const maxAttempts = yield* resolvePolicy(
            typeof job.attempts === "number"
                ? job.attempts
                : (job.attempts.max ?? 20),
            context,
        );
        const uniqueKey =
            command.options.idempotencyKey ??
            (job.unique === undefined
                ? undefined
                : normalizeKey(yield* resolvePolicy(job.unique.key, context)));

        return {
            command,
            job,
            newJob: {
                id: randomUUID(),
                name: job.name,
                queue: queueName(selectedQueue),
                payload: encodedPayload,
                meta: command.options.meta ?? {},
                tags: command.options.tags ?? [],
                maxAttempts,
                runAt: computeRunAt(command.options),
                priority: command.options.priority ?? 0,
                ...(uniqueKey === undefined
                    ? {}
                    : { idempotencyKey: uniqueKey }),
                duplicatePolicy: command.options.duplicate ?? "use-existing",
            },
        };
    });

export const insertJobCommand = <
    const Name extends string,
    ResultSchema extends Schema.Top,
>(
    command: JobCommand<Name, unknown, ResultSchema["Type"], any>,
): Effect.Effect<JobHandle<Name, ResultSchema>, InsertError, any> =>
    Effect.gen(function* () {
        const resolved = yield* resolveJobCommand(command);
        const engine = yield* JobEngine;
        const record = yield* engine.enqueue(resolved.newJob);
        const notifier = yield* Effect.serviceOption(JobNotifier);

        if (Option.isSome(notifier)) {
            yield* notifier.value.notifyInsert({ queue: record.queue });
        }

        return toHandle(resolved.job, record);
    });

export const insertJobCommands = <
    const Name extends string,
    ResultSchema extends Schema.Top,
>(
    commands: ReadonlyArray<JobCommand<Name, unknown, ResultSchema["Type"], any>>,
): Effect.Effect<
    ReadonlyArray<JobHandle<Name, ResultSchema>>,
    InsertError,
    any
> =>
    Effect.forEach(commands, insertJobCommand, {
        concurrency: 1,
    });

export const makeJobDefinition = <
    const Name extends string,
    PayloadSchema extends Schema.Top,
    ResultSchema extends Schema.Top = typeof Schema.Unknown,
    Queues extends string = string,
>(
    options: JobDefinitionOptions<Name, PayloadSchema, ResultSchema, Queues>,
): JobDefinition<Name, PayloadSchema, ResultSchema, Queues> => {
    const result = (options.result ?? Schema.Unknown) as ResultSchema;
    const attempts = options.attempts ?? 20;
    const job = {
        _tag: "JobDefinition" as const,
        name: options.name,
        queue: options.queue,
        payload: options.payload,
        payloadSchema: options.payload,
        result,
        resultSchema: result,
        attempts,
        defaultMaxAttempts: maxAttemptsFrom(attempts),
        backoff: backoffFrom(attempts),
        ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
        ...(options.unique === undefined ? {} : { unique: options.unique }),
        ...(options.concurrency === undefined
            ? {}
            : { concurrency: options.concurrency }),
        ...(options.rateLimit === undefined
            ? {}
            : { rateLimit: options.rateLimit }),
        ...(options.dashboard === undefined
            ? {}
            : { dashboard: options.dashboard }),
        ...(options.hooks === undefined ? {} : { hooks: options.hooks }),
        command: (
            payload: PayloadSchema["Type"],
            commandOptions?: JobCommandOptions<Queues>,
        ) =>
            makeCommand<Name, PayloadSchema["Type"], ResultSchema["Type"], Queues>(
                job as JobDefinition<Name, any, any, Queues>,
                payload,
                commandOptions,
            ),
        enqueue: (
            payload: PayloadSchema["Type"],
            commandOptions?: JobCommandOptions<Queues>,
        ) =>
            Effect.gen(function* () {
                const runtime = yield* JobRuntime;
                return yield* runtime.insert(
                    job.command(payload, commandOptions),
                );
            }),
        enqueueMany: (
            payloads: ReadonlyArray<PayloadSchema["Type"]>,
            commandOptions?: JobEnqueueManyOptions<Queues>,
        ) =>
            Effect.gen(function* () {
                const runtime = yield* JobRuntime;
                return yield* runtime.insertMany(
                    payloads.map((payload) =>
                        job.command(payload, commandOptions),
                    ),
                );
            }),
        enqueueStream: (
            payloads: Iterable<PayloadSchema["Type"]>,
            commandOptions?: JobEnqueueManyOptions<Queues>,
        ) =>
            Effect.gen(function* () {
                const runtime = yield* JobRuntime;
                return yield* runtime.insertMany(
                    Array.from(payloads, (payload) =>
                        job.command(payload, commandOptions),
                    ),
                );
            }),
        toLayer: <Requirements, Error = unknown>(
            run: JobRunHandler<
                PayloadSchema["Type"],
                Name,
                ResultSchema["Type"],
                Error,
                Requirements
            >,
        ) =>
            Layer.effect(JobRegistry)(
                Effect.gen(function* () {
                    const registry = yield* JobRegistry;
                    yield* registry.register(
                        job as unknown as JobDefinition.Any,
                        run as JobRun,
                    );

                    return registry;
                }),
            ),
    };

    return job;
};

export const Job = {
    cancel: (reason: unknown) => Effect.fail(new JobCancelError({ reason })),
    discard: (reason: unknown) => Effect.fail(new JobDiscardError({ reason })),
    snooze: (duration: Duration.Input, reason?: unknown) =>
        Effect.fail(new JobSnoozeError({ duration, reason })),
    find: (id: JobId) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.find(id);
        }),
    list: (options?: JobListOptions) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.list(options);
        }),
    cancelById: (id: JobId, reason: unknown) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            yield* engine.cancel(id, reason);
        }),
    runNow: (id: JobId) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            yield* engine.runNow(id);
        }),
    snoozeById: (id: JobId, options: { readonly until: Date }) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            yield* engine.snooze(id, options.until);
        }),
    prune: (options: JobPruneOptions) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.prune(options);
        }),
    rescueExecuting: (options: JobRescueOptions) =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            return yield* engine.rescueExecuting(options);
        }),
};

export type { DynamicQueueName, JobId, JobName, QueueName };
